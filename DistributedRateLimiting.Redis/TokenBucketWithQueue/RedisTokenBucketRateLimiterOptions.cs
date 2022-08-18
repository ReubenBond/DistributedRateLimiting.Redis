using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StackExchange.Redis.Profiling;

namespace System.Threading.RateLimiting.StackExchangeRedis.TokenBucketWithQueue;

public class RedisQueueingTokenBucketRateLimiterOptions : IOptions<RedisQueueingTokenBucketRateLimiterOptions>
{
    private TimeSpan _replenishmentPeriod = TimeSpan.FromSeconds(1);
    private int _tokensPerPeriod;

    /// <summary>
    /// Specifies the minimum period between replenishments.
    /// Must be set to a value >= <see cref="TimeSpan.Zero" /> by the time these options are passed to the constructor of <see cref="RedisQueueingTokenBucketRateLimiterOptions"/>.
    /// </summary>
    public TimeSpan ReplenishmentPeriod
    {
        get => _replenishmentPeriod;
        set
        {
            _replenishmentPeriod = value;
            UpdateFillRate();
        }
    }

    /// <summary>
    /// Specifies the maximum number of tokens to restore each replenishment.
    /// Must be set to a value > 0 by the time these options are passed to the constructor of <see cref="RedisQueueingTokenBucketRateLimiterOptions"/>.
    /// </summary>
    public int TokensPerPeriod 
    {
        get => _tokensPerPeriod;
        set
        {
            _tokensPerPeriod = value;
            UpdateFillRate();
        }
    }

    /// <summary>
    /// The maximum number of tokens in a bucket.
    /// </summary>
    public int TokenLimit { get; set; }

    /// <summary>
    /// Determines the behaviour of <see cref="RateLimiter.AcquireAsync"/> when not enough resources can be leased.
    /// </summary>
    /// <value>
    /// <see cref="QueueProcessingOrder.OldestFirst"/> by default.
    /// </value>
    public QueueProcessingOrder QueueProcessingOrder { get; set; } = QueueProcessingOrder.OldestFirst;

    /// <summary>
    /// Maximum cumulative token count of queued acquisition requests on each instance.
    /// Must be set to a value >= 0 by the time these options are passed to the constructor of <see cref="RedisQueueingTokenBucketRateLimiterOptions"/>.
    /// </summary>
    public int QueueLimit { get; set; }

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

    RedisQueueingTokenBucketRateLimiterOptions IOptions<RedisQueueingTokenBucketRateLimiterOptions>.Value
    {
        get { return this; }
    }

    /// <summary>
    /// The values of <see cref="TokensPerPeriod"/> and <see cref="ReplenishmentPeriod"/> expressed as a rate per second.
    /// </summary>
    internal double FillRatePerSecond { get; private set; }

    private void UpdateFillRate()
    {
        FillRatePerSecond = _tokensPerPeriod / _replenishmentPeriod.TotalSeconds;
    }
}
