using StackExchange.Redis;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace System.Threading.RateLimiting.StackExchangeRedis.TokenBucket;

public sealed class RedisTokenBucketRateLimiter : RateLimiter
{
    private static readonly RateLimitLease SuccessfulLease = new Lease(isAcquired: true);
    private static readonly RateLimitLease FailedLease = new Lease(isAcquired: false);
    private readonly LuaScript _acquireScript;
    private readonly RedisTokenBucketRateLimiterOptions _options;
    private readonly SemaphoreSlim _connectionLock = new(initialCount: 1, maxCount: 1);
    private IConnectionMultiplexer? _connection;
    private IDatabase? _store;
    private bool _disposed;
    private volatile int _estimatedRemainingPermits;

    /// <inheritdoc />
    public override TimeSpan? IdleDuration => null;

    public RedisTokenBucketRateLimiter(RedisTokenBucketRateLimiterOptions options)
    {
        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (options.TokenLimit <= 0 || options.TokensPerPeriod <= 0)
        {
            throw new ArgumentException($"Both {nameof(options.TokenLimit)} and {nameof(options.TokensPerPeriod)} must be set to values greater than 0.", nameof(options));
        }

        if (options.ReplenishmentPeriod < TimeSpan.Zero)
        {
            throw new ArgumentException($"{nameof(options.ReplenishmentPeriod)} must be set to a value greater than or equal to TimeSpan.Zero.", nameof(options));
        }

        if (options.Configuration is not { Length: > 0 } && options.ConfigurationOptions is null && options.ConnectionMultiplexerFactory is null)
        {
            throw new ArgumentException($"One of {nameof(RedisTokenBucketRateLimiterOptions.Configuration)}, {nameof(RedisTokenBucketRateLimiterOptions.ConfigurationOptions)}, or {nameof(RedisTokenBucketRateLimiterOptions.ConnectionMultiplexerFactory)} must be specified.", nameof(options));
        }

        _options = options;
        _acquireScript = LuaScript.Prepare(GetAcquireLuaScript(_options.TokenLimit, _options.FillRatePerSecond));
    }

    public override int GetAvailablePermits()
    {
        return _estimatedRemainingPermits;
    }

    protected override RateLimitLease AcquireCore(int permitCount)
    {
        return FailedLease;
    }

    protected override async ValueTask<RateLimitLease> WaitAsyncCore(int permitCount, CancellationToken cancellationToken)
    {
        await ConnectAsync(cancellationToken).ConfigureAwait(false);
        Debug.Assert(_store is not null);

        var rawResult = await _store.ScriptEvaluateAsync(_acquireScript, new { BucketId = _options.InstanceName, PermitCount = permitCount }).ConfigureAwait(false);
        var result = (int[])rawResult!;
        if (result is null or { Length: 0 })
        {
            _estimatedRemainingPermits = 0;
            return FailedLease;
        }

        if (result is { Length: >= 2 })
        {
            _estimatedRemainingPermits = result[1];
        }

        if (result[0] != 1)
        {
            return FailedLease;
        }

        return SuccessfulLease;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _connection?.Close();
    }

    /// <inheritdoc />
    protected override async ValueTask DisposeAsyncCore()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_connection is { } connection)
        {
            await connection.CloseAsync();
        }
    }

    private async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (_store is not null)
        {
            Debug.Assert(_connection is not null);
            return;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_store is null)
            {
                if (_options.ConnectionMultiplexerFactory is null)
                {
                    if (_options.ConfigurationOptions is not null)
                    {
                        _connection = await ConnectionMultiplexer.ConnectAsync(_options.ConfigurationOptions).ConfigureAwait(false);
                    }
                    else
                    {
                        _connection = await ConnectionMultiplexer.ConnectAsync(_options.Configuration!).ConfigureAwait(false);
                    }
                }
                else
                {
                    _connection = await _options.ConnectionMultiplexerFactory().ConfigureAwait(false);
                }

                PrepareConnection();
                _store = _connection.GetDatabase();
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private void PrepareConnection()
    {
        TryRegisterProfiler();
    }

    private void CheckDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(GetType().FullName);
        }
    }

    private void TryRegisterProfiler()
    {
        _ = _connection ?? throw new InvalidOperationException($"{nameof(_connection)} cannot be null.");

        if (_options.ProfilingSession != null)
        {
            _connection.RegisterProfiler(_options.ProfilingSession);
        }
    }

    private static string GetAcquireLuaScript(int capacity, double fillRate) =>
        // The bulk of the rate limiting logic is implemented in the database via this Lua script.
        // The primary reason for this is to allow the database act as a single source of truth as far as time is concerned.
        // In the case that server failover occurs and the new server's clock is behind, the script will adopt the new server's clock.
        // If the new server's clock is ahead of the previous server, that may cause a one-time anomalous jump in the number of avaialable tokens (up to the bucket's capacity)
        $$"""
        local bucket = @BucketId
        local permit_count = tonumber(@PermitCount) -- estimated number of remaining permits
        local capacity = {{capacity}} -- max token count
        local fill_rate = {{fillRate}} -- tokens added per second

        -- Converts a Redis hash result array into a table where the values are numbers
        local array_to_table_of_numbers = function (array)
            local result = {}
            local key
            for index, value in ipairs(array) do
                if index % 2 == 1 then
                    key = value
                else
                    result[key] = tonumber(value)
                end
            end
            return result
        end

        -- Compute the current time as milliseconds since epoch
        local now = redis.call('TIME')
        local new_t = now[1] + (now[2] / 1000000); -- TIME returns a seconds and a microseconds component. Combine them here.

        local success
        local prev
        local new_v

        -- Read values from store, if present, falling back to initial values if they are not
        local existing_values = redis.call('HGETALL', bucket)
        if #existing_values > 0 then
          prev = array_to_table_of_numbers(existing_values)
        else
          prev = { v = capacity, t = new_t }
        end

        -- Account for clock synchronization issues when failing over to a different replica
        local delta_t = math.max(0, new_t - prev.t)

        -- Account for newly added tokens, clamping the value to the [0, capacity] range
        new_v = math.max(0, math.min(capacity, prev.v + (delta_t * fill_rate)))

        -- Attempt to service the request
        success = new_v >= permit_count
        if success then
          -- Compute the post-servicing value
          new_v = new_v - permit_count;

          -- Update the store
          redis.call('HSET', bucket, 'v', new_v, 't', new_t)

          -- Expire the key after the full refill duration if no other operations occur.
          -- The value is clamped between 1 second and 1 year to cover the overflow/underflow case.
          local expiration_seconds = math.ceil(math.min(math.max(capacity / fill_rate, 1), 31536000))
          redis.call('EXPIRE', bucket, expiration_seconds)
        end

        return { success, new_v }
        """;

    private sealed class Lease : RateLimitLease
    {
        public override bool IsAcquired { get; }

        public override IEnumerable<string> MetadataNames => Array.Empty<string>();

        public Lease(bool isAcquired)
        {
            IsAcquired = isAcquired;
        }

        public override bool TryGetMetadata(string metadataName, [NotNullWhen(true)] out object? metadata)
        {
            metadata = default;
            return false;
        }

        public override string? ToString() => IsAcquired switch
        {
            true => $"{nameof(Lease)} (IsAcquired: true)",
            _ => $"{nameof(Lease)} (IsAcquired: false)"
        };
    }
}
