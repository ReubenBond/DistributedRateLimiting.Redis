using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Threading.RateLimiting.StackExchangeRedis.ApproximateTokenBucket;
using System.Threading.RateLimiting.StackExchangeRedis.TokenBucket;

namespace System.Threading.RateLimiting.StackExchangeRedis;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRedisTokenBucketRateLimiter(this IServiceCollection services, Action<RedisTokenBucketRateLimiterOptions> configureOptions)
    {
        services
            .AddOptions<RedisTokenBucketRateLimiterOptions>()
            .Configure(configureOptions);
        services.AddSingleton<RateLimiter, RedisTokenBucketRateLimiter>();
        return services;
    }

    public static IServiceCollection AddRedisApproximateTokenBucketRateLimiter(this IServiceCollection services, Action<RedisApproximateTokenBucketRateLimiterOptions> configureOptions)
    {
        services
            .AddOptions<RedisApproximateTokenBucketRateLimiterOptions>()
            .Configure(configureOptions);
        services.AddSingleton<RateLimiter, RedisApproximateTokenBucketRateLimiter>();
        return services;
    }
}
