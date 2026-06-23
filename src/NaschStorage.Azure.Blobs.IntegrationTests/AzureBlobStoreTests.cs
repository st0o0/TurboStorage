using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using NaschStorage.Azure.Blobs;

namespace NaschStorage.Azure.Blobs.IntegrationTests;

public sealed class AzureBlobStoreTests : IClassFixture<AzuriteContainerFixture>, IAsyncLifetime
{
    private readonly AzuriteContainerFixture _fixture;
    private ActorSystem _system = null!;
    private IMaterializer _materializer = null!;
    private AzureBlobStore _store = null!;

    public AzureBlobStoreTests(AzuriteContainerFixture fixture)
    {
        _fixture = fixture;
    }

    public ValueTask InitializeAsync()
    {
        _system = ActorSystem.Create("test");
        _materializer = _system.Materializer();
        _store = new AzureBlobStore(new AzureBlobStoreOptions
        {
            ConnectionString = _fixture.ConnectionString,
            ContainerName = $"test-{Guid.NewGuid():N}",
        });
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _system.Terminate();
    }

    [Fact]
    public async Task Write_And_Read_RoundTrips()
    {
        var data = "Hello, NaschStorage Azure!"u8.ToArray();
        var writeSink = _store.Write("test/file.txt");
        var writeResult = await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(writeSink, _materializer);

        Assert.Equal("test/file.txt", writeResult.Path);
        Assert.Equal(data.Length, writeResult.BytesWritten);
        Assert.NotNull(writeResult.ETag);

        var readSource = _store.Read("test/file.txt");
        var (readResultTask, chunksTask) = readSource
            .ToMaterialized(Sink.Seq<ReadOnlyMemory<byte>>(), Keep.Both)
            .Run(_materializer);
        var chunks = await chunksTask;
        var resolvedReadResult = await readResultTask;
        var readData = chunks.SelectMany(c => c.ToArray()).ToArray();

        Assert.Equal(data, readData);
        Assert.Equal("test/file.txt", resolvedReadResult.Path);
        Assert.Equal(data.Length, resolvedReadResult.Size);
        Assert.NotNull(resolvedReadResult.ETag);
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
    public async Task Write_VirtualDirectories_Work()
    {
        var data = "nested data"u8.ToArray();
        var writeResult = await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("sub/dir/deep/file.txt"), _materializer);

        Assert.Equal("sub/dir/deep/file.txt", writeResult.Path);

        var chunks = await _store.Read("sub/dir/deep/file.txt")
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);
        var readData = chunks.SelectMany(c => c.ToArray()).ToArray();
        Assert.Equal(data, readData);
    }

    [Fact]
    public async Task Read_NonExistent_Throws_FileNotFoundException()
    {
        var readSource = _store.Read("nonexistent/file.txt");
        var (readResultTask, _) = readSource
            .ToMaterialized(Sink.Seq<ReadOnlyMemory<byte>>(), Keep.Both)
            .Run(_materializer);

        await Assert.ThrowsAsync<FileNotFoundException>(() => readResultTask);
    }

    [Fact]
    public async Task Read_Returns_Metadata()
    {
        var data = "metadata test"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("meta/read.txt"), _materializer);

        var readSource = _store.Read("meta/read.txt");
        var (readResultTask, _) = readSource
            .ToMaterialized(Sink.Seq<ReadOnlyMemory<byte>>(), Keep.Both)
            .Run(_materializer);
        var result = await readResultTask;

        Assert.Equal("meta/read.txt", result.Path);
        Assert.Equal(data.Length, result.Size);
        Assert.NotNull(result.ETag);
        Assert.NotNull(result.ModifiedOn);
    }

    [Fact]
    public async Task List_Returns_Files()
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
    public async Task DeleteAsync_Removes_File()
    {
        var data = "delete me"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("delete/file.txt"), _materializer);

        var existsBefore = await _store.ExistsAsync(["delete/file.txt"], TestContext.Current.CancellationToken);
        Assert.True(existsBefore.First());

        await _store.DeleteAsync(["delete/file.txt"], TestContext.Current.CancellationToken);

        var existsAfter = await _store.ExistsAsync(["delete/file.txt"], TestContext.Current.CancellationToken);
        Assert.False(existsAfter.First());
    }

    [Fact]
    public async Task ExistsAsync_Returns_Correct_Results()
    {
        var data = "exists check"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("exists/file.txt"), _materializer);

        var results = await _store.ExistsAsync(
            ["exists/file.txt", "nonexistent/file.txt"],
            TestContext.Current.CancellationToken);

        var list = results.ToList();
        Assert.True(list[0]);
        Assert.False(list[1]);
    }

    [Fact]
    public async Task GetBlobsAsync_Returns_FileInfo()
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
        Assert.NotNull(blob.ETag);
        Assert.NotNull(blob.ModifiedOn);
        Assert.NotNull(blob.CreatedOn);
    }

    [Fact]
    public async Task SetBlobsAsync_Sets_ContentType_And_Metadata()
    {
        var data = "metadata test"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("setmeta/file.txt"), _materializer);

        await _store.SetBlobsAsync([
            new BlobItem
            {
                Path = "setmeta/file.txt",
                ContentType = "text/plain",
                Properties = new Dictionary<string, string>
                {
                    ["author"] = "test-user",
                    ["version"] = "1",
                },
            }
        ], TestContext.Current.CancellationToken);

        var blobs = await _store.GetBlobsAsync(["setmeta/file.txt"], TestContext.Current.CancellationToken);
        var blob = blobs.First();

        Assert.Equal("text/plain", blob.ContentType);
        Assert.NotNull(blob.Properties);
        Assert.Equal("test-user", blob.Properties["author"]);
        Assert.Equal("1", blob.Properties["version"]);
    }
}
