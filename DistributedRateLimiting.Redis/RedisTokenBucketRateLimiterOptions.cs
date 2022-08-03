using StackExchange.Redis;

namespace DistributedRateLimiting.Redis;

public class RedisTokenBucketRateLimiterOptions
{
    /// <summary>
    /// Gets or sets the Redis database id.
    /// </summary>
    public int DatabaseId { get; set; } = -1;

    public Func<IConnectionMultiplexer> CreateConnectionMultiplexer { get; set; } = () => ConnectionMultiplexer.Connect("localhost");

    /// <summary>
    /// The maximum number of tokens in a bucket.
    /// </summary>
    public int Capacity { get; set; }

    /// <summary>
    /// The number of tokens to insert into each bucket per second.
    /// </summary>
    public double FillRate { get; set; }
}
