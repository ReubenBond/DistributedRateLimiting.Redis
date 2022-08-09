using System.Diagnostics.CodeAnalysis;
using System.Threading.RateLimiting;

namespace DistributedRateLimiting.Redis.TokenBucket;

internal sealed class RedisTokenBucketRateLimiterLease : RateLimitLease
{
    public override bool IsAcquired { get; }

    public override IEnumerable<string> MetadataNames => Array.Empty<string>();

    public RedisTokenBucketRateLimiterLease(bool isAcquired)
    {
        IsAcquired = isAcquired;
    }

    public override bool TryGetMetadata(string metadataName, [NotNullWhen(true)] out object? metadata)
    {
        metadata = default;
        return false;
    }

    public override string? ToString() => IsAcquired switch
    {
        true => $"{nameof(RedisTokenBucketRateLimiterLease)} (IsAcquired: true)",
        _ => $"{nameof(RedisTokenBucketRateLimiterLease)} (IsAcquired: false)"
    };
}
