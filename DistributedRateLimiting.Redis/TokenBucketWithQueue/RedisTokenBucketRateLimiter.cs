using StackExchange.Redis;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;

/*
namespace System.Threading.RateLimiting.StackExchangeRedis.TokenBucketWithQueue;

public sealed partial class RedisQueueingTokenBucketRateLimiter : RateLimiter
{
    private static readonly RateLimitLease SuccessfulLease = new Lease(isAcquired: true, retryAfter: null);
    private static readonly RateLimitLease FailedLease = new Lease(isAcquired: false, retryAfter: null);
    private readonly LuaScript _acquireScript;
    private readonly RedisQueueingTokenBucketRateLimiterOptions _options;
    private readonly SemaphoreSlim _connectionLock = new(initialCount: 1, maxCount: 1);
    private readonly Deque<RequestRegistration> _queue = new();
    private int _queueCount;
    private long _lastSyncTime;
    private IConnectionMultiplexer? _connection;
    private IDatabase? _store;
    private bool _disposed;
    private Task? _lastRenewTask;
    private volatile int _estimatedRemainingPermits;

    // Use the queue as the lock field so we don't need to allocate another object for a lock and have another field in the object
    private object Lock => _queue;

    /// <inheritdoc />
    public override TimeSpan? IdleDuration => null;

    public RedisQueueingTokenBucketRateLimiter(RedisQueueingTokenBucketRateLimiterOptions options)
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
            throw new ArgumentException($"One of {nameof(RedisQueueingTokenBucketRateLimiterOptions.Configuration)}, {nameof(RedisQueueingTokenBucketRateLimiterOptions.ConfigurationOptions)}, or {nameof(RedisQueueingTokenBucketRateLimiterOptions.ConnectionMultiplexerFactory)} must be specified.", nameof(options));
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

    protected override ValueTask<RateLimitLease> WaitAsyncCore(int permitCount, CancellationToken cancellationToken)
    {
        // These amounts of resources can never be acquired
        if (permitCount > _options.TokenLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(permitCount), permitCount, $"{permitCount} token(s) exceeds the token limit of {_options.TokenLimit}");
        }

        ThrowIfDisposed();

        // Return SuccessfulAcquisition if requestedCount is 0 and resources are available
        if (permitCount == 0 && _tokenCount > 0)
        {
            return new ValueTask<RateLimitLease>(SuccessfulLease);
        }

        lock (Lock)
        {
            if (TryLeaseUnsynchronized(permitCount, out RateLimitLease? lease))
            {
                return new ValueTask<RateLimitLease>(lease);
            }

            // Avoid integer overflow by using subtraction instead of addition
            Debug.Assert(_options.QueueLimit >= _queueCount);
            if (_options.QueueLimit - _queueCount < permitCount)
            {
                if (_options.QueueProcessingOrder == QueueProcessingOrder.NewestFirst && permitCount <= _options.QueueLimit)
                {
                    // remove oldest items from queue until there is space for the newest acquisition request
                    do
                    {
                        RequestRegistration oldestRequest = _queue.DequeueHead();
                        _queueCount -= oldestRequest.Count;
                        Debug.Assert(_queueCount >= 0);
                        if (!oldestRequest.Tcs.TrySetResult(FailedLease))
                        {
                            // Updating queue count is handled by the cancellation code
                            _queueCount += oldestRequest.Count;
                        }
                    }
                    while (_options.QueueLimit - _queueCount < permitCount);
                }
                else
                {
                    // Don't queue if queue limit reached and QueueProcessingOrder is OldestFirst
                    return new ValueTask<RateLimitLease>(CreateFailedTokenLease(permitCount));
                }
            }

            CancelQueueState tcs = new CancelQueueState(permitCount, this, cancellationToken);
            CancellationTokenRegistration ctr = default;
            if (cancellationToken.CanBeCanceled)
            {
                ctr = cancellationToken.Register(static obj =>
                {
                    ((CancelQueueState)obj!).TrySetCanceled();
                }, tcs);
            }

            RequestRegistration registration = new RequestRegistration(permitCount, tcs, ctr);
            _queue.EnqueueTail(registration);
            _queueCount += permitCount;
            Debug.Assert(_queueCount <= _options.QueueLimit);

            return new ValueTask<RateLimitLease>(registration.Tcs.Task);
        }
    }

    private bool TryLeaseUnsynchronized(int tokenCount, [NotNullWhen(true)] out RateLimitLease? lease)
    {
        ThrowIfDisposed();

        // If permitCount is 0 we want to queue it if there are no available permits
        var availableTokens = AvailableTokens;
        if (availableTokens >= tokenCount && availableTokens != 0)
        {
            if (tokenCount == 0)
            {
                // Edge case where the check before the lock showed 0 available permits but when we got the lock some permits were now available
                lease = SuccessfulLease;
                return true;
            }

            // a. if there are no items queued we can lease
            // b. if there are items queued but the processing order is newest first, then we can lease the incoming request since it is the newest
            if (_queueCount == 0 || (_queueCount > 0 && _options.QueueProcessingOrder == QueueProcessingOrder.NewestFirst))
            {
                _idleSince = null;
                _localThrottleScore += tokenCount;
                Debug.Assert(_localThrottleScore >= 0);
                lease = SuccessfulLease;
                return true;
            }
        }

        lease = null;
        return false;
    }

    private static void Refresh(object? state)
    {
        RedisQueueingTokenBucketRateLimiter limiter = (state as RedisQueueingTokenBucketRateLimiter)!;
        Debug.Assert(limiter is not null);

        // Start a refresh only if the previous refresh has completed.
        if (limiter._lastRenewTask is null or { IsCompleted: true })
        {
            lock (limiter.Lock)
            {
                limiter._lastRenewTask = limiter!.RefreshAsync();
            }
        }
    }

    private async Task RefreshAsync()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            await ConnectAsync();
            Debug.Assert(_store is not null);
        }
        catch (Exception exception)
        {
            Log.CouldNotConnectToRedis(_logger, exception);
            return;
        }

        int localTokens;
        lock (Lock)
        {
            localTokens = _localThrottleScore;
            _localThrottleScore = 0;
        }

        try
        {
            var result = await _store.ScriptEvaluateAsync(_syncScript, new { BucketId = _options.InstanceName, LocalCount = localTokens }).ConfigureAwait(false);
            var resultArray = (RedisValue[])result!;
            _globalThrottleScore = (int)resultArray[0];
            var period = (double)resultArray[1];
            _instanceCountEstimate = Math.Max(1, Math.Round(_options.ReplenishmentPeriod.TotalSeconds / period));
        }
        catch (Exception exception)
        {
            Log.ErrorEvaluatingRedisScript(_logger, exception);
            return;
        }

        // method is re-entrant (from Timer), lock to avoid multiple simultaneous replenishes
        var nowTicks = Stopwatch.GetTimestamp();
        lock (Lock)
        {
            if (_disposed)
            {
                return;
            }

            _lastSyncTime = nowTicks;

            // Process queued requests
            Deque<RequestRegistration> queue = _queue;

            Debug.Assert(AvailableTokens <= _options.TokenLimit);
            var options = _options;
            while (queue.Count > 0)
            {
                RequestRegistration nextPendingRequest =
                      options.QueueProcessingOrder == QueueProcessingOrder.OldestFirst
                      ? queue.PeekHead()
                      : queue.PeekTail();

                if (AvailableTokens >= nextPendingRequest.Count)
                {
                    // Request can be fulfilled
                    nextPendingRequest =
                        options.QueueProcessingOrder == QueueProcessingOrder.OldestFirst
                        ? queue.DequeueHead()
                        : queue.DequeueTail();

                    _queueCount -= nextPendingRequest.Count;
                    _localThrottleScore += nextPendingRequest.Count;
                    Debug.Assert(_localThrottleScore >= 0);

                    if (!nextPendingRequest.Tcs.TrySetResult(SuccessfulLease))
                    {
                        // Queued item was canceled so add count back
                        _localThrottleScore += nextPendingRequest.Count;
                        // Updating queue count is handled by the cancellation code
                        _queueCount += nextPendingRequest.Count;
                    }
                    nextPendingRequest.CancellationTokenRegistration.Dispose();
                    Debug.Assert(_queueCount >= 0);
                }
                else
                {
                    // Request cannot be fulfilled
                    break;
                }
            }

            if (ConsumedTokens == 0)
            {
                _idleSince = Stopwatch.GetTimestamp();
            }
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        lock (Lock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _connection?.Close();
            _renewTimer?.Dispose();
            while (_queue.Count > 0)
            {
                RequestRegistration next = _options.QueueProcessingOrder == QueueProcessingOrder.OldestFirst
                    ? _queue.DequeueHead()
                    : _queue.DequeueTail();
                next.CancellationTokenRegistration.Dispose();
                next.Tcs.TrySetResult(FailedLease);
            }
        }
    }

    /// <inheritdoc />
    protected override async ValueTask DisposeAsyncCore()
    {
        Dispose(true);

        if (_connection is { } connection)
        {
            await connection.CloseAsync();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(TokenBucketRateLimiter));
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

    private Lease CreateFailedTokenLease(int permitCount)
    {
        // This will likely be inaccurate, since there is an expectation of multiple clients simultaneously adding to the global throttle score.
        var deficit = Math.Max(0, ConsumedTokens + permitCount + _queueCount - _options.TokenLimit);
        return new Lease(false, TimeSpan.FromSeconds(deficit * _options.FillRatePerSecond));
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

        -- Read values from store, if present, falling back to initial values if they are not
        local existing_values = redis.call('HGETALL', bucket)
        local prev
        if #existing_values > 0 then
          prev = array_to_table_of_numbers(existing_values)
          if prev.v == nil then prev.v = capacity end
          if prev.t == nil then prev.t = new_t end
        else
          prev = { v = capacity, t = new_t }
        end

        -- Account for clock synchronization issues when failing over to a different replica
        local delta_t = math.max(0, new_t - prev.t)

        -- Account for newly added tokens, clamping the value to the [0, capacity] range
        local new_v = math.max(0, math.min(capacity, prev.v + (delta_t * fill_rate)))

        -- Attempt to service the request
        local success = new_v >= permit_count
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

    private readonly struct RequestRegistration
    {
        public RequestRegistration(int tokenCount, TaskCompletionSource<RateLimitLease> tcs, CancellationTokenRegistration cancellationTokenRegistration)
        {
            Count = tokenCount;
            Tcs = tcs;
            CancellationTokenRegistration = cancellationTokenRegistration;
        }

        public int Count { get; }

        public TaskCompletionSource<RateLimitLease> Tcs { get; }

        public CancellationTokenRegistration CancellationTokenRegistration { get; }
    }

    private sealed class CancelQueueState : TaskCompletionSource<RateLimitLease>
    {
        private readonly int _tokenCount;
        private readonly RedisQueueingTokenBucketRateLimiter _limiter;
        private readonly CancellationToken _cancellationToken;

        public CancelQueueState(int tokenCount, RedisQueueingTokenBucketRateLimiter limiter, CancellationToken cancellationToken)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            _tokenCount = tokenCount;
            _limiter = limiter;
            _cancellationToken = cancellationToken;
        }

        public new bool TrySetCanceled()
        {
            if (TrySetCanceled(_cancellationToken))
            {
                lock (_limiter.Lock)
                {
                    _limiter._queueCount -= _tokenCount;
                }
                return true;
            }
            return false;
        }
    }

    private sealed class Lease : RateLimitLease
    {
        private static readonly string[] s_allMetadataNames = new[] { MetadataName.RetryAfter.Name };

        private readonly TimeSpan? _retryAfter;

        public Lease(bool isAcquired, TimeSpan? retryAfter)
        {
            IsAcquired = isAcquired;
            _retryAfter = retryAfter;
        }

        public override bool IsAcquired { get; }

        public override IEnumerable<string> MetadataNames => s_allMetadataNames;

        public override bool TryGetMetadata(string metadataName, out object? metadata)
        {
            if (metadataName == MetadataName.RetryAfter.Name && _retryAfter.HasValue)
            {
                metadata = _retryAfter.Value;
                return true;
            }

            metadata = default;
            return false;
        }

        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{nameof(Lease)} {nameof(IsAcquired)}: {IsAcquired}");
            if (_retryAfter is not null)
            {
                result.Append($" {MetadataName.RetryAfter.Name}: {_retryAfter}");
            }

            return result.ToString();
        }
    }
}
*/
