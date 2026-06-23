using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.TestKit.Xunit;
using NaschStorage.Sftp;

namespace NaschStorage.Sftp.IntegrationTests;

public sealed class SftpBlobStoreTests : TestKit, IClassFixture<SftpContainerFixture>
{
    private readonly IMaterializer _materializer;
    private readonly SftpBlobStore _store;

    public SftpBlobStoreTests(SftpContainerFixture fixture)
    {
        _materializer = Sys.Materializer();
        _store = new SftpBlobStore(new SftpBlobStoreOptions
        {
            Host = fixture.Host,
            Port = fixture.Port,
            Username = fixture.Username,
            Password = fixture.Password,
            BasePath = "/upload",
        });
    }

    protected override void AfterAll()
    {
        _store.Dispose();
        base.AfterAll();
    }

    [Fact]
    public async Task Write_And_Read_RoundTrips()
    {
        var data = "Hello, NaschStorage SFTP!"u8.ToArray();
        var writeSink = _store.Write("test/file.txt");
        var writeResult = await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(writeSink, _materializer);

        Assert.Equal("test/file.txt", writeResult.Path);
        Assert.Equal(data.Length, writeResult.BytesWritten);

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
    public async Task Write_CreatesSubdirectories()
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
    public async Task List_Recursive_IncludesSubdirectories()
    {
        var data = "recursive test"u8.ToArray();
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("root/file.txt"), _materializer);
        await Source.Single(new ReadOnlyMemory<byte>(data))
            .RunWith(_store.Write("root/sub/nested.txt"), _materializer);

        var recursiveItems = await _store.List(new ListOptions { Prefix = "root/", Recursive = true })
            .RunWith(Sink.Seq<BlobItem>(), _materializer);

        var paths = recursiveItems.Select(i => i.Path).ToList();
        Assert.Contains("root/file.txt", paths);
        Assert.Contains("root/sub/nested.txt", paths);
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
    }
}
