using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using NaschStorage.InMemory;

namespace NaschStorage.UnitTests.InMemory;

public sealed class InMemoryBlobStoreTests : IAsyncLifetime
{
    private ActorSystem _system = null!;
    private IMaterializer _materializer = null!;
    private InMemoryBlobStore _store = null!;

    public ValueTask InitializeAsync()
    {
        _system = ActorSystem.Create("test");
        _materializer = _system.Materializer();
        _store = new InMemoryBlobStore();
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _system.Terminate();

    [Fact]
    public async Task Write_And_Read_RoundTrips()
    {
        var data = "Hello, NaschStorage!"u8.ToArray();
        var writeSource = Source.Single(new ReadOnlyMemory<byte>(data));
        var writeSink = _store.Write("test/file.txt");

        var writeResult = await writeSource.RunWith(writeSink, _materializer);

        Assert.Equal("test/file.txt", writeResult.Path);
        Assert.Equal(data.Length, writeResult.BytesWritten);

        var readSource = _store.Read("test/file.txt");
        var (readResultTask, chunksTask) = readSource.ToMaterialized(Sink.Seq<ReadOnlyMemory<byte>>(), Keep.Both).Run(_materializer);
        var chunks = await chunksTask;
        var resolvedReadResult = await readResultTask;
        var readData = chunks.SelectMany(c => c.ToArray()).ToArray();

        Assert.Equal(data, readData);
        Assert.Equal("test/file.txt", resolvedReadResult.Path);
        Assert.Equal(data.Length, resolvedReadResult.Size);
    }

    [Fact]
    public async Task ExistsAsync_Returns_True_For_Existing_Blob()
    {
        var data = "exists check"u8.ToArray();
        var writeSource = Source.Single(new ReadOnlyMemory<byte>(data));
        await writeSource.RunWith(_store.Write("exists/file.txt"), _materializer);

        var results = await _store.ExistsAsync(["exists/file.txt", "nonexistent/file.txt"], TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        var list = results.ToList();
        Assert.True(list[0]);
        Assert.False(list[1]);
    }

    [Fact]
    public async Task DeleteAsync_Removes_Blob()
    {
        var data = "delete me"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("delete/file.txt"), _materializer);

        var beforeDelete = await _store.ExistsAsync(["delete/file.txt"], TestContext.Current.CancellationToken);
        Assert.True(beforeDelete.First());

        await _store.DeleteAsync(["delete/file.txt"], TestContext.Current.CancellationToken);

        var afterDelete = await _store.ExistsAsync(["delete/file.txt"], TestContext.Current.CancellationToken);
        Assert.False(afterDelete.First());
    }

    [Fact]
    public async Task List_Returns_All_Blobs()
    {
        var data = "list test"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("list/file1.txt"), _materializer);
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("list/file2.txt"), _materializer);

        var items = await _store.List()
            .RunWith(Sink.Seq<BlobItem>(), _materializer);

        var paths = items.Select(i => i.Path).ToList();
        Assert.Contains("list/file1.txt", paths);
        Assert.Contains("list/file2.txt", paths);
        Assert.True(items.Count >= 2);
    }

    [Fact]
    public async Task List_WithPrefix_Filters()
    {
        var data = "prefix test"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("alpha/file.txt"), _materializer);
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("beta/file.txt"), _materializer);

        var alphaItems = await _store.List(new ListOptions { Prefix = "alpha/" })
            .RunWith(Sink.Seq<BlobItem>(), _materializer);

        Assert.All(alphaItems, item => Assert.StartsWith("alpha/", item.Path));
        Assert.DoesNotContain(alphaItems, item => item.Path.StartsWith("beta/", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Write_Append_AppendsData()
    {
        var part1 = "Hello, "u8.ToArray();
        var part2 = "World!"u8.ToArray();

        await Source.Single(new ReadOnlyMemory<byte>(part1))
            .RunWith(_store.Write("append/file.txt"), _materializer);
        await Source.Single(new ReadOnlyMemory<byte>(part2))
            .RunWith(_store.Write("append/file.txt", append: true), _materializer);

        var readSource = _store.Read("append/file.txt");
        var chunks = await readSource.RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);
        var allData = chunks.SelectMany(c => c.ToArray()).ToArray();

        var expected = part1.Concat(part2).ToArray();
        Assert.Equal(expected, allData);
    }

    [Fact]
    public async Task GetBlobsAsync_Returns_Metadata()
    {
        var data = new byte[42];
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("meta/file.txt"), _materializer);

        var blobs = await _store.GetBlobsAsync(["meta/file.txt"], TestContext.Current.CancellationToken);

        Assert.Single(blobs);
        var blob = blobs.First();
        Assert.Equal("meta/file.txt", blob.Path);
        Assert.Equal(42L, blob.Size);
        Assert.Equal(BlobKind.File, blob.Kind);
    }

    [Fact]
    public async Task SetBlobsAsync_Updates_Properties()
    {
        var data = "set props"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("props/file.txt"), _materializer);

        var updatedBlob = new BlobItem
        {
            Path = "props/file.txt",
            ContentType = "text/plain",
            Properties = new Dictionary<string, string> { ["author"] = "test" },
        };
        await _store.SetBlobsAsync([updatedBlob], TestContext.Current.CancellationToken);

        var blobs = await _store.GetBlobsAsync(["props/file.txt"], TestContext.Current.CancellationToken);
        var blob = blobs.First();
        Assert.Equal("text/plain", blob.ContentType);
        Assert.Equal("test", blob.Properties!["author"]);
    }
}
