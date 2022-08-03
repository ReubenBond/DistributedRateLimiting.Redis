using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Diagnostics;
using System.Threading.RateLimiting;

namespace DistributedRateLimiting.Redis;

public sealed class RedisTokenBucketRateLimiter : RateLimiter
{
    private static readonly RateLimitLease SuccessfulLease = new RedisTokenBucketRateLimiterLease(isAcquired: true);
    private static readonly RateLimitLease FailedLease = new RedisTokenBucketRateLimiterLease(isAcquired: false);
    private readonly IDatabase _db;
    private readonly LuaScript _acquireScript;
    private readonly RedisTokenBucketRateLimiterOptions _options;
    private readonly IConnectionMultiplexer _redis;
    private readonly Stopwatch _idleTimer = Stopwatch.StartNew();
    private volatile int _estimatedRemainingPermits;

    public override TimeSpan? IdleDuration => _idleTimer.Elapsed;

    public RedisTokenBucketRateLimiter(IOptions<RedisTokenBucketRateLimiterOptions> options)
    {
        _options = options.Value;
        _redis = _options.CreateConnectionMultiplexer();
        _db = _redis.GetDatabase(_options.DatabaseId);
        _acquireScript = LuaScript.Prepare(GetAcquireLuaScript(_options.Capacity, _options.FillRate));
    }

    public override int GetAvailablePermits()
    {
        _idleTimer.Restart();
        return _estimatedRemainingPermits;
    }

    protected override RateLimitLease AcquireCore(int permitCount)
    {
        _idleTimer.Restart();
        return FailedLease;
    }

    protected override async ValueTask<RateLimitLease> WaitAsyncCore(int permitCount, CancellationToken cancellationToken)
    {
        _idleTimer.Restart();
        var bucket = "test";
        var rawResult = await _db.ScriptEvaluateAsync(_acquireScript, new { BucketId = bucket, PermitCount = permitCount });
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
          redis.call('HMSET', bucket, 'v', new_v, 't', new_t)

          -- Expire the key after the full refill duration if no other operations occur.
          -- The value is clamped between 1 second and 1 year to cover the overflow/underflow case.
          local expiration_seconds = math.ceil(math.min(math.max(capacity / fill_rate, 1), 31536000))
          redis.call('EXPIRE', bucket, expiration_seconds)
        end

        return { success, new_v }
        """;
}
