using Microsoft.Extensions.Options;

namespace DistributedRateLimiting.Redis;

internal sealed class RedisTokenBucketRateLimiterOptionsValidator : IValidateOptions<RedisTokenBucketRateLimiterOptions>
{
    public ValidateOptionsResult Validate(string? name, RedisTokenBucketRateLimiterOptions options)
    {
        if (options.FillRate < 0)
        {
            return ValidateOptionsResult.Fail($"{nameof(RedisTokenBucketRateLimiterOptions.FillRate)} must not be negative");
        }

        if (options.CreateConnectionMultiplexer is null)
        {
            return ValidateOptionsResult.Fail($"{nameof(RedisTokenBucketRateLimiterOptions.CreateConnectionMultiplexer)} must not be null");
        }

        return ValidateOptionsResult.Success;
    }
}