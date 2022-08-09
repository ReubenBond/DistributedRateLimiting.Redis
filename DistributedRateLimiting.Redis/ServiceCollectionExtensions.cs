using DistributedRateLimiting.Redis.TokenBucket;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Threading.RateLimiting;

namespace DistributedRateLimiting.Redis;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRedisTokenBucketRateLimiter(this IServiceCollection services, Action<RedisTokenBucketRateLimiterOptions> configureOptions)
    {
        services
            .AddOptions<RedisTokenBucketRateLimiterOptions>()
            .Configure(configureOptions);
        services.AddSingleton<RateLimiter, RedisTokenBucketRateLimiter>();
        services.AddTransient<IValidateOptions<RedisTokenBucketRateLimiterOptions>, RedisTokenBucketRateLimiterOptionsValidator>();
        return services;
    }
}
