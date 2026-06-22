using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using BenchmarkDotNet.Attributes;
using NaschStorage.InMemory;

namespace NaschStorage.Benchmarks;

[MemoryDiagnoser]
public class InMemoryBlobStoreBenchmarks
{
    private ActorSystem _system = null!;
    private IMaterializer _materializer = null!;
    private InMemoryBlobStore _store = null!;
    private byte[] _data = null!;

    [GlobalSetup]
    public void Setup()
    {
        _system = ActorSystem.Create("bench");
        _materializer = _system.Materializer();
        _store = new InMemoryBlobStore();
        _data = new byte[8192];
        Random.Shared.NextBytes(_data);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _system.Terminate();
    }

    [Benchmark]
    public async Task<BlobWriteResult> WriteSmallBlob()
    {
        return await Source.Single(new ReadOnlyMemory<byte>(_data))
            .RunWith(_store.Write("/bench.dat"), _materializer);
    }

    [Benchmark]
    public async Task WriteAndReadRoundTrip()
    {
        await Source.Single(new ReadOnlyMemory<byte>(_data))
            .RunWith(_store.Write("/roundtrip.dat"), _materializer);

        await _store.Read("/roundtrip.dat")
            .RunWith(Sink.Ignore<ReadOnlyMemory<byte>>(), _materializer);
    }
}
