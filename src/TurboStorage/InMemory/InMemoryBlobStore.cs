using System.Collections.Concurrent;
using Akka;
using Akka.Streams.Dsl;

namespace TurboStorage.InMemory;

public sealed class InMemoryBlobStore : IBlobStore
{
    private readonly ConcurrentDictionary<string, byte[]> _data = new();
    private readonly ConcurrentDictionary<string, BlobItem> _metadata = new();

    public Source<ReadOnlyMemory<byte>, Task<BlobReadResult>> Read(string path)
    {
        if (!_data.TryGetValue(path, out var bytes))
        {
            return Source.Failed<ReadOnlyMemory<byte>>(new FileNotFoundException($"Blob not found: {path}"))
                .MapMaterializedValue(_ => Task.FromException<BlobReadResult>(new FileNotFoundException($"Blob not found: {path}")));
        }

        _metadata.TryGetValue(path, out var meta);

        var result = new BlobReadResult
        {
            Path = path,
            Size = bytes.Length,
            ContentType = meta?.ContentType,
            ETag = meta?.ETag,
            ModifiedOn = meta?.ModifiedOn,
            Properties = meta?.Properties,
        };

        return Source.Single(new ReadOnlyMemory<byte>(bytes))
            .MapMaterializedValue(_ => Task.FromResult(result));
    }

    public Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>> Write(string path, bool append = false)
    {
        return Flow.Create<ReadOnlyMemory<byte>>()
            .Aggregate(new List<byte>(), (acc, chunk) =>
            {
                acc.AddRange(chunk.ToArray());
                return acc;
            })
            .Select(allBytes =>
            {
                byte[] finalData;
                if (append && _data.TryGetValue(path, out var existing))
                {
                    finalData = existing.Concat(allBytes).ToArray();
                }
                else
                {
                    finalData = allBytes.ToArray();
                }

                _data[path] = finalData;

                _metadata.AddOrUpdate(path,
                    _ => new BlobItem
                    {
                        Path = path,
                        Kind = BlobKind.File,
                        Size = finalData.Length,
                        ModifiedOn = DateTimeOffset.UtcNow,
                    },
                    (_, existing) => existing with
                    {
                        Size = finalData.Length,
                        ModifiedOn = DateTimeOffset.UtcNow,
                    });

                return new BlobWriteResult
                {
                    Path = path,
                    BytesWritten = finalData.Length,
                };
            })
            .ToMaterialized(Sink.First<BlobWriteResult>(), Keep.Right);
    }

    public Source<BlobItem, NotUsed> List(ListOptions? options = null)
    {
        var items = _metadata.Values.AsEnumerable();

        if (options?.Prefix is { } prefix)
        {
            items = items.Where(i => i.Path.StartsWith(prefix, StringComparison.Ordinal));
        }

        if (options?.MaxResults is { } max)
        {
            items = items.Take(max);
        }

        return Source.From(items.ToList());
    }

    public Task DeleteAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        foreach (var path in paths)
        {
            _data.TryRemove(path, out _);
            _metadata.TryRemove(path, out _);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyCollection<bool>> ExistsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        var results = paths.Select(p => _data.ContainsKey(p)).ToList();
        return Task.FromResult<IReadOnlyCollection<bool>>(results);
    }

    public Task<IReadOnlyCollection<BlobItem>> GetBlobsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        var results = paths
            .Where(p => _metadata.ContainsKey(p))
            .Select(p => _metadata[p])
            .ToList();
        return Task.FromResult<IReadOnlyCollection<BlobItem>>(results);
    }

    public Task SetBlobsAsync(IReadOnlyCollection<BlobItem> blobs, CancellationToken ct = default)
    {
        foreach (var blob in blobs)
        {
            _metadata.AddOrUpdate(blob.Path,
                _ => blob,
                (_, existing) => existing with
                {
                    ContentType = blob.ContentType ?? existing.ContentType,
                    ETag = blob.ETag ?? existing.ETag,
                    Properties = blob.Properties ?? existing.Properties,
                    ModifiedOn = blob.ModifiedOn ?? existing.ModifiedOn,
                });
        }

        return Task.CompletedTask;
    }
}
