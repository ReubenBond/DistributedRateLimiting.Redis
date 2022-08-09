using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StackExchange.Redis.Profiling;

namespace DistributedRateLimiting.Redis.TokenBucket;

public class RedisTokenBucketRateLimiterOptions : IOptions<RedisTokenBucketRateLimiterOptions>
{
    /// <summary>
    /// The maximum number of tokens in a bucket.
    /// </summary>
    public int Capacity { get; set; }

    /// <summary>
    /// The number of tokens to insert into each bucket per second.
    /// </summary>
    public double FillRate { get; set; }

    /// <summary>
    /// The configuration used to connect to Redis.
    /// </summary>
    public string? Configuration { get; set; }

    /// <summary>
    /// The configuration used to connect to Redis.
    /// If specified, this takes precedence over <see cref="Configuration"/>.
    /// </summary>
    public ConfigurationOptions? ConfigurationOptions { get; set; }

    /// <summary>
    /// Gets or sets a delegate to create the ConnectionMultiplexer instance.
    /// If specified, this takes precedence over <see cref="Configuration"/> and <see cref="ConfigurationOptions"/>.
    /// </summary>
    public Func<Task<IConnectionMultiplexer>>? ConnectionMultiplexerFactory { get; set; }

    /// <summary>
    /// The Redis instance name.
    /// </summary>
    public string? InstanceName { get; set; }

    /// <summary>
    /// The Redis profiling session
    /// </summary>
    public Func<ProfilingSession>? ProfilingSession { get; set; }

    RedisTokenBucketRateLimiterOptions IOptions<RedisTokenBucketRateLimiterOptions>.Value
    {
        get { return this; }
    }
}
