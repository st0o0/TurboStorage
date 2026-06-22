using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using NaschStorage.InMemory;
using NaschStorage.Virtual;

namespace NaschStorage.UnitTests.Virtual;

public sealed class VirtualBlobStoreTests : IAsyncLifetime
{
    private ActorSystem _system = null!;
    private IMaterializer _materializer = null!;
    private InMemoryBlobStore _localStore = null!;
    private InMemoryBlobStore _cacheStore = null!;
    private VirtualBlobStore _store = null!;

    public ValueTask InitializeAsync()
    {
        _system = ActorSystem.Create("test");
        _materializer = _system.Materializer();
        _localStore = new InMemoryBlobStore();
        _cacheStore = new InMemoryBlobStore();
        _store = new VirtualStorageBuilder()
            .Mount("/local", _localStore)
            .Mount("/cache", _cacheStore)
            .Build();
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _system.Terminate();

    [Fact]
    public async Task Write_Routes_To_Correct_Store()
    {
        var data = "hello local"u8.ToArray();
        var writeResult = await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("/local/test.txt"), _materializer);

        Assert.Equal("/local/test.txt", writeResult.Path);

        // _localStore should have "test.txt" stored
        var localExists = await _localStore.ExistsAsync(["test.txt"], TestContext.Current.CancellationToken);
        Assert.True(localExists.First());

        // _cacheStore should not have it
        var cacheExists = await _cacheStore.ExistsAsync(["test.txt"], TestContext.Current.CancellationToken);
        Assert.False(cacheExists.First());
    }

    [Fact]
    public async Task Read_Routes_To_Correct_Store()
    {
        var data = "cached data"u8.ToArray();
        // Write directly to underlying store
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_cacheStore.Write("data.bin"), _materializer);

        // Read via virtual path
        var readSource = _store.Read("/cache/data.bin");
        var (readResultTask, chunksTask) = readSource
            .ToMaterialized(Sink.Seq<ReadOnlyMemory<byte>>(), Keep.Both)
            .Run(_materializer);

        var chunks = await chunksTask;
        var resolvedReadResult = await readResultTask;
        var readData = chunks.SelectMany(c => c.ToArray()).ToArray();

        Assert.Equal(data, readData);
        Assert.Equal("/cache/data.bin", resolvedReadResult.Path);
    }

    [Fact]
    public async Task List_WithoutPrefix_AggregatesAllMounts()
    {
        var data = "content"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_localStore.Write("a.txt"), _materializer);
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_cacheStore.Write("b.txt"), _materializer);

        var items = await _store.List()
            .RunWith(Sink.Seq<BlobItem>(), _materializer);

        var paths = items.Select(i => i.Path).ToList();
        Assert.Contains("/local/a.txt", paths);
        Assert.Contains("/cache/b.txt", paths);
    }

    [Fact]
    public async Task List_WithPrefix_DelegatesToMount()
    {
        var data = "content"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_localStore.Write("a.txt"), _materializer);
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_cacheStore.Write("b.txt"), _materializer);

        var items = await _store.List(new ListOptions { Prefix = "/local" })
            .RunWith(Sink.Seq<BlobItem>(), _materializer);

        var paths = items.Select(i => i.Path).ToList();
        Assert.All(paths, p => Assert.StartsWith("/local", p));
        Assert.DoesNotContain(paths, p => p.StartsWith("/cache", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Delete_Routes_To_Correct_Store()
    {
        var data = "delete me"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_localStore.Write("to-delete.txt"), _materializer);

        // Confirm it exists first
        var before = await _localStore.ExistsAsync(["to-delete.txt"], TestContext.Current.CancellationToken);
        Assert.True(before.First());

        // Delete via virtual path
        await _store.DeleteAsync(["/local/to-delete.txt"], TestContext.Current.CancellationToken);

        // Should be gone
        var after = await _localStore.ExistsAsync(["to-delete.txt"], TestContext.Current.CancellationToken);
        Assert.False(after.First());
    }

    [Fact]
    public async Task ExistsAsync_Routes_To_Correct_Store()
    {
        var data = "exists check"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_cacheStore.Write("exists.bin"), _materializer);

        var results = await _store.ExistsAsync(
            ["/cache/exists.bin", "/local/exists.bin"],
            TestContext.Current.CancellationToken);

        var list = results.ToList();
        Assert.True(list[0]);   // /cache/exists.bin exists
        Assert.False(list[1]);  // /local/exists.bin does not
    }

    [Fact]
    public async Task UnknownMount_Throws()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            var source = _store.Read("/unknown/file.txt");
            await source.RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);
        });
    }

    [Fact]
    public void Build_WithNoMounts_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
            new VirtualStorageBuilder().Build());
    }

    [Fact]
    public void Build_WithDuplicateMount_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new VirtualStorageBuilder()
                .Mount("/local", _localStore)
                .Mount("/local", _cacheStore));
    }
}
