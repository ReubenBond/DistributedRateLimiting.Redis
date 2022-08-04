// See https://aka.ms/new-console-template for more information

using DistributedRateLimiting.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using StackExchange.Redis;
using System.Net;
using System.Threading.RateLimiting;

var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync("localhost");
Func<string, RateLimiter> createRateLimiter = key =>
{
    return new RedisTokenBucketRateLimiter(connectionMultiplexer, new RedisTokenBucketRateLimiterOptions() { Capacity = 100, FillRate = 10, DatabaseKey = $"rate_limiter_bucket:{key}" });
};

var rl = PartitionedRateLimiter.Create<string, string>(resource =>
{
    return RateLimitPartition.Create(resource, createRateLimiter);
});


var options = new RedisTokenBucketRateLimiterOptions() { Capacity = 100, FillRate = 10 };
var limiter = new RedisTokenBucketRateLimiter(connectionMultiplexer, options);

while (true)
{
    Console.WriteLine($"fish: {await rl.WaitAsync("fish")} (remaining: {rl.GetAvailablePermits("fish")})");
    //var result = await limiter.WaitAsync(1);
    //var permits = limiter.GetAvailablePermits();
    //Console.WriteLine($"{result} (remaining: {permits})");
    await Task.Delay(100);
}
/*
var hostBuilder = Host.CreateDefaultBuilder(args);
hostBuilder
    .UseOrleans(silo =>
    {
        int instanceId = 0;
        if (args is { Length: > 0 })
        {
            instanceId = int.Parse(args[0]);
        }

        silo.UseLocalhostClustering(
            siloPort: 11111 + instanceId,
            gatewayPort: 30000 + instanceId,
            primarySiloEndpoint: new IPEndPoint(IPAddress.Loopback, 11111));
    })
    .ConfigureServices(services =>
    {
        services.AddDistributedRateLimiter(options =>
        {
            options.QueueLimit = 200;
            options.GlobalPermitCount = 20;
            options.TargetPermitsPerClient = 2;
        });
    })
    .UseConsoleLifetime();
var host = await hostBuilder.StartAsync();

var rateLimiter = host.Services.GetRequiredService<RateLimiter>();
long numLeaseHolders = 0;
var cancellationToken = new CancellationTokenSource();
var tasks = new List<Task>();
for (var i = 0; i < 5; i++)
{
    var name = $"worker-{i}";
    tasks.Add(Task.Run(() => RunWorker(rateLimiter, name, cancellationToken.Token)));
}

Console.CancelKeyPress += (_, __) => cancellationToken.Cancel();
await Task.WhenAll(tasks);
await host.StopAsync();

host.Dispose();

async Task RunWorker(RateLimiter rateLimiter, string name, CancellationToken cancellationToken)
{
    const int PermitCount = 1;
    while (!cancellationToken.IsCancellationRequested)
    {
        RateLimitLease lease;
        do
        {
            lease = rateLimiter.Acquire(PermitCount);
            if (lease.IsAcquired) break;
            Console.WriteLine($"{name}: Waiting for lease (holders: {numLeaseHolders})");
            lease = await rateLimiter.WaitAsync(PermitCount);
        } while (!lease.IsAcquired);

        var holders = Interlocked.Increment(ref numLeaseHolders);
        Console.WriteLine($"{name}: Acquired lease (holders: {holders})");
        await Task.Delay(500);
        lease.Dispose();
        holders = Interlocked.Decrement(ref numLeaseHolders);
        Console.WriteLine($"{name}: Disposed lease (holders: {holders})");
        await Task.Delay(500);
    }
}
*/
