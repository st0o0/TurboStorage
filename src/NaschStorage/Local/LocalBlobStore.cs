using Akka;
using Akka.IO;
using Akka.Streams.Dsl;

namespace NaschStorage.Local;

public sealed class LocalBlobStore : IBlobStore
{
    private readonly string _rootPath;

    public LocalBlobStore(string rootPath)
    {
        _rootPath = Path.GetFullPath(rootPath);
    }

    private string ToFullPath(string blobPath) =>
        Path.GetFullPath(Path.Combine(_rootPath, blobPath.TrimStart('/')));

    private string ToBlobPath(string fullPath) =>
        Path.GetRelativePath(_rootPath, fullPath).Replace('\\', '/');

    public Source<ReadOnlyMemory<byte>, Task<BlobReadResult>> Read(string path)
    {
        var fullPath = ToFullPath(path);

        if (!File.Exists(fullPath))
        {
            var ex = new FileNotFoundException($"Blob not found: {path}", fullPath);
            return Source.Failed<ReadOnlyMemory<byte>>(ex)
                .MapMaterializedValue(_ => Task.FromException<BlobReadResult>(ex));
        }

        var fileInfo = new FileInfo(fullPath);
        var size = fileInfo.Length;
        var modifiedOn = fileInfo.LastWriteTimeUtc;

        return FileIO.FromFile(fileInfo)
            .Select(bs => (ReadOnlyMemory<byte>)bs.ToArray())
            .MapMaterializedValue(ioTask => ioTask.ContinueWith<BlobReadResult>(t =>
            {
                if (t.IsFaulted)
                {
                    ExceptionDispatchHelper.Rethrow(t.Exception!);
                }

                var ioResult = t.Result;
                if (!ioResult.WasSuccessful)
                {
                    throw ioResult.Error;
                }

                return new BlobReadResult
                {
                    Path = path,
                    Size = size,
                    ModifiedOn = modifiedOn,
                };
            }));
    }

    public Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>> Write(string path, bool append = false)
    {
        var fullPath = ToFullPath(path);
        var dir = Path.GetDirectoryName(fullPath)!;
        Directory.CreateDirectory(dir);

        var fileMode = append ? FileMode.Append : FileMode.Create;

        return Flow.Create<ReadOnlyMemory<byte>>()
            .Select(mem => ByteString.FromBytes(mem.ToArray()))
            .ToMaterialized(FileIO.ToFile(new FileInfo(fullPath), fileMode), Keep.Right)
            .MapMaterializedValue(ioTask => ioTask.ContinueWith<BlobWriteResult>(t =>
            {
                if (t.IsFaulted)
                {
                    ExceptionDispatchHelper.Rethrow(t.Exception!);
                }

                var ioResult = t.Result;
                if (!ioResult.WasSuccessful)
                {
                    throw ioResult.Error;
                }

                var fi = new FileInfo(fullPath);
                return new BlobWriteResult
                {
                    Path = path,
                    BytesWritten = fi.Exists ? fi.Length : 0,
                };
            }));
    }

    public Source<BlobItem, NotUsed> List(ListOptions? options = null)
    {
        var searchRoot = _rootPath;
        var originalPrefix = options?.Prefix;
        var recursive = options?.Recursive ?? false;

        // If prefix provided, narrow the search directory if possible
        if (originalPrefix is not null)
        {
            var prefixDir = Path.GetFullPath(Path.Combine(_rootPath, originalPrefix.TrimStart('/')));
            // If the prefix itself is a directory, use it as the search root
            if (Directory.Exists(prefixDir))
            {
                searchRoot = prefixDir;
            }
            // Otherwise, use the parent directory of the prefix
            else
            {
                var parentDir = Path.GetDirectoryName(prefixDir);
                if (parentDir is not null && Directory.Exists(parentDir))
                {
                    searchRoot = parentDir;
                }
            }
        }

        // Always enumerate all subdirectories; prefix filtering handles scope
        const SearchOption searchOption = SearchOption.AllDirectories;

        IEnumerable<string> files;
        try
        {
            files = Directory.EnumerateFiles(searchRoot, "*", searchOption);
        }
        catch (DirectoryNotFoundException)
        {
            files = [];
        }

        // Re-apply prefix filter in blob-path space
        var items = files
            .Select(ToBlobPath)
            .Where(p => originalPrefix is null || p.StartsWith(originalPrefix, StringComparison.Ordinal));

        if (options?.MaxResults is { } max)
        {
            items = items.Take(max);
        }

        var blobItems = items.Select(blobPath =>
        {
            var fi = new FileInfo(ToFullPath(blobPath));
            return new BlobItem
            {
                Path = blobPath,
                Kind = BlobKind.File,
                Size = fi.Exists ? fi.Length : null,
                ModifiedOn = fi.Exists ? fi.LastWriteTimeUtc : null,
            };
        }).ToList();

        return Source.From(blobItems);
    }

    public Task DeleteAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        foreach (var path in paths)
        {
            var fullPath = ToFullPath(path);
            if (File.Exists(fullPath))
            {
                File.Delete(fullPath);
            }
            else if (Directory.Exists(fullPath))
            {
                Directory.Delete(fullPath, recursive: true);
            }
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyCollection<bool>> ExistsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        var results = paths
            .Select(p =>
            {
                var fullPath = ToFullPath(p);
                return File.Exists(fullPath) || Directory.Exists(fullPath);
            })
            .ToList();

        return Task.FromResult<IReadOnlyCollection<bool>>(results);
    }

    public Task<IReadOnlyCollection<BlobItem>> GetBlobsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        var results = paths.Select(path =>
        {
            var fullPath = ToFullPath(path);
            if (File.Exists(fullPath))
            {
                var fi = new FileInfo(fullPath);
                return new BlobItem
                {
                    Path = path,
                    Kind = BlobKind.File,
                    Size = fi.Length,
                    ModifiedOn = fi.LastWriteTimeUtc,
                    CreatedOn = fi.CreationTimeUtc,
                };
            }

            if (Directory.Exists(fullPath))
            {
                return new BlobItem
                {
                    Path = path,
                    Kind = BlobKind.Folder,
                };
            }

            return null;
        })
        .OfType<BlobItem>()
        .ToList();

        return Task.FromResult<IReadOnlyCollection<BlobItem>>(results);
    }

    public Task SetBlobsAsync(IReadOnlyCollection<BlobItem> blobs, CancellationToken ct = default)
    {
        // No-op: local filesystem has no standard way to persist custom metadata
        return Task.CompletedTask;
    }
}

file static class ExceptionDispatchHelper
{
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    public static void Rethrow(AggregateException ex) =>
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.GetBaseException()).Throw();
}
