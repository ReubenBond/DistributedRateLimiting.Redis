using StackExchange.Redis;

namespace DistributedRateLimiting.Redis.TokenBucket;

public class RedisTokenBucketRateLimiterOptions
{
    /// <summary>
    /// Gets or sets the Redis database id.
    /// </summary>
    public int DatabaseId { get; set; } = -1;

    /// <summary>
    /// The maximum number of tokens in a bucket.
    /// </summary>
    public int Capacity { get; set; }

    /// <summary>
    /// The number of tokens to insert into each bucket per second.
    /// </summary>
    public double FillRate { get; set; }

    /// <summary>
    /// The key in the database to use to store rate limiting information.
    /// </summary>
    public string DatabaseKey { get; set; } = "rate_limiter_bucket:default";
}
