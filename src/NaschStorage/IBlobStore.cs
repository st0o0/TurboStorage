using Akka;
using Akka.Streams.Dsl;

namespace NaschStorage;

public interface IBlobStore
{
    Source<ReadOnlyMemory<byte>, Task<BlobReadResult>> Read(string path);
    Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>> Write(string path, bool append = false);
    Source<BlobItem, NotUsed> List(ListOptions? options = null);
    Task DeleteAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default);
    Task<IReadOnlyCollection<bool>> ExistsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default);
    Task<IReadOnlyCollection<BlobItem>> GetBlobsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default);
    Task SetBlobsAsync(IReadOnlyCollection<BlobItem> blobs, CancellationToken ct = default);
}
