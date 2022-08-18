using Microsoft.Extensions.Logging;

namespace System.Threading.RateLimiting.StackExchangeRedis.ApproximateTokenBucket;

public sealed partial class RedisApproximateTokenBucketRateLimiter
{
    private static partial class Log
    {
        [LoggerMessage(1, LogLevel.Error, "Could not connect to Redis.", EventName = "CouldNotConnectToRedis")]
        public static partial void CouldNotConnectToRedis(ILogger logger, Exception exception);

        [LoggerMessage(2, LogLevel.Error, "Error evaluating Redis script.", EventName = "ErrorEvaluatingRedisScript")]
        public static partial void ErrorEvaluatingRedisScript(ILogger logger, Exception exception);
    }
}
