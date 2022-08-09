using Microsoft.Extensions.Options;

namespace DistributedRateLimiting.Redis.TokenBucket;

internal sealed class RedisTokenBucketRateLimiterOptionsValidator : IValidateOptions<RedisTokenBucketRateLimiterOptions>
{
    public ValidateOptionsResult Validate(string? name, RedisTokenBucketRateLimiterOptions options)
    {
        if (options.FillRate < 0)
        {
            return ValidateOptionsResult.Fail($"{nameof(RedisTokenBucketRateLimiterOptions.FillRate)} must not be negative");
        }

        if (options.Configuration is not { Length: > 0 } && options.ConfigurationOptions is null && options.ConnectionMultiplexerFactory is null)
        {
            return ValidateOptionsResult.Fail($"One of {nameof(RedisTokenBucketRateLimiterOptions.Configuration)}, {nameof(RedisTokenBucketRateLimiterOptions.ConfigurationOptions)}, or {nameof(RedisTokenBucketRateLimiterOptions.ConnectionMultiplexerFactory)} must be specified");
        }

        return ValidateOptionsResult.Success;
    }
}