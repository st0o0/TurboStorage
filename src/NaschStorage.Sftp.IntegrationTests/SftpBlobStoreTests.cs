using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using NaschStorage.Sftp;

namespace NaschStorage.Sftp.IntegrationTests;

public sealed class SftpBlobStoreTests : IClassFixture<SftpContainerFixture>, IAsyncLifetime
{
    private readonly SftpContainerFixture _fixture;
    private ActorSystem _system = null!;
    private IMaterializer _materializer = null!;
    private SftpBlobStore _store = null!;

    public SftpBlobStoreTests(SftpContainerFixture fixture)
    {
        _fixture = fixture;
    }

    public ValueTask InitializeAsync()
    {
        _system = ActorSystem.Create("test");
        _materializer = _system.Materializer();
        _store = new SftpBlobStore(new SftpBlobStoreOptions
        {
            Host = _fixture.Host,
            Port = _fixture.Port,
            Username = _fixture.Username,
            Password = _fixture.Password,
            BasePath = "/upload",
        });
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _store.Dispose();
        await _system.Terminate();
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
}
