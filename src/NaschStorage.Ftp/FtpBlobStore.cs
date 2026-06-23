using Akka;
using Akka.IO;
using Akka.Streams.Dsl;
using FluentFTP;
using FluentFTP.Exceptions;

namespace NaschStorage.Ftp;

public sealed class FtpBlobStore : IBlobStore, IDisposable
{
    private readonly FtpBlobStoreOptions _options;
    private readonly FtpClient _client;

    public FtpBlobStore(FtpBlobStoreOptions options)
    {
        _options = options;
        _client = new FtpClient(options.Host, options.Username, options.Password, options.Port);
        _client.Config.EncryptionMode = options.EncryptionMode;
    }

    private void EnsureConnected()
    {
        if (!_client.IsConnected)
        {
            _client.AutoConnect();
        }
    }

    private string ToFullPath(string blobPath)
    {
        var normalized = blobPath.TrimStart('/');
        if (_options.BasePath is { } basePath)
        {
            return $"{basePath.TrimEnd('/')}/{normalized}";
        }

        return $"/{normalized}";
    }

    private string ToBlobPath(string fullPath)
    {
        if (_options.BasePath is not { } basePath)
        {
            return fullPath.TrimStart('/');
        }

        var prefix = basePath.TrimEnd('/') + "/";
        return fullPath.StartsWith(prefix, StringComparison.Ordinal)
            ? fullPath[prefix.Length..]
            : fullPath.TrimStart('/');
    }

    private static string? GetParentDirectory(string path)
    {
        var lastSlash = path.LastIndexOf('/');
        return lastSlash > 0 ? path[..lastSlash] : null;
    }

    public Source<ReadOnlyMemory<byte>, Task<BlobReadResult>> Read(string path)
    {
        EnsureConnected();
        var fullPath = ToFullPath(path);

        if (!_client.FileExists(fullPath))
        {
            var ex = new FileNotFoundException($"Blob not found: {path}", fullPath);
            return Source.Failed<ReadOnlyMemory<byte>>(ex)
                .MapMaterializedValue(_ => Task.FromException<BlobReadResult>(ex));
        }

        var info = _client.GetObjectInfo(fullPath);

        return StreamConverters.FromInputStream(() => _client.OpenRead(fullPath))
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
                    Size = info?.Size,
                    ModifiedOn = info?.Modified is { } mod && mod != DateTime.MinValue
                        ? new DateTimeOffset(mod, TimeSpan.Zero)
                        : null,
                };
            }));
    }

    public Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>> Write(string path, bool append = false)
    {
        EnsureConnected();
        var fullPath = ToFullPath(path);

        var parentDir = GetParentDirectory(fullPath);
        if (parentDir is not null)
        {
            _client.CreateDirectory(parentDir, true);
        }

        return Flow.Create<ReadOnlyMemory<byte>>()
            .Select(mem => ByteString.FromBytes(mem.ToArray()))
            .ToMaterialized(
                StreamConverters.FromOutputStream(() =>
                    append ? _client.OpenAppend(fullPath) : _client.OpenWrite(fullPath)),
                Keep.Right)
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

                EnsureConnected();
                var fileInfo = _client.GetObjectInfo(fullPath);
                return new BlobWriteResult
                {
                    Path = path,
                    BytesWritten = fileInfo?.Size ?? ioResult.Count,
                };
            }));
    }

    public Source<BlobItem, NotUsed> List(ListOptions? options = null)
    {
        EnsureConnected();

        var basePath = _options.BasePath?.TrimEnd('/') ?? "";
        var prefix = options?.Prefix;
        var recursive = options?.Recursive ?? false;

        var searchPath = string.IsNullOrEmpty(basePath) ? "/" : basePath;
        if (prefix is not null)
        {
            var fullPrefix = string.IsNullOrEmpty(basePath)
                ? $"/{prefix.TrimStart('/')}"
                : $"{basePath}/{prefix.TrimStart('/')}";

            if (_client.DirectoryExists(fullPrefix))
            {
                searchPath = fullPrefix;
            }
            else
            {
                var parent = GetParentDirectory(fullPrefix);
                if (parent is not null && _client.DirectoryExists(parent))
                {
                    searchPath = parent;
                }
            }
        }

        FtpListItem[] listing;
        try
        {
            listing = _client.GetListing(searchPath, FtpListOption.Recursive);
        }
        catch (FtpException)
        {
            listing = [];
        }

        var items = listing
            .Where(item => item.Type == FtpObjectType.File || (recursive && item.Type == FtpObjectType.Directory))
            .Select(item => new BlobItem
            {
                Path = ToBlobPath(item.FullName),
                Kind = item.Type == FtpObjectType.Directory ? BlobKind.Folder : BlobKind.File,
                Size = item.Size >= 0 ? item.Size : null,
                ModifiedOn = item.Modified != DateTime.MinValue
                    ? new DateTimeOffset(item.Modified, TimeSpan.Zero)
                    : null,
            });

        if (prefix is not null)
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
        EnsureConnected();
        foreach (var path in paths)
        {
            var fullPath = ToFullPath(path);
            if (_client.FileExists(fullPath))
            {
                _client.DeleteFile(fullPath);
            }
            else if (_client.DirectoryExists(fullPath))
            {
                _client.DeleteDirectory(fullPath);
            }
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyCollection<bool>> ExistsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        EnsureConnected();
        var results = paths
            .Select(p =>
            {
                var fullPath = ToFullPath(p);
                return _client.FileExists(fullPath) || _client.DirectoryExists(fullPath);
            })
            .ToList();

        return Task.FromResult<IReadOnlyCollection<bool>>(results);
    }

    public Task<IReadOnlyCollection<BlobItem>> GetBlobsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        EnsureConnected();
        var results = paths.Select(path =>
        {
            var fullPath = ToFullPath(path);
            var info = _client.GetObjectInfo(fullPath);
            if (info is null)
            {
                return null;
            }

            return new BlobItem
            {
                Path = path,
                Kind = info.Type == FtpObjectType.Directory ? BlobKind.Folder : BlobKind.File,
                Size = info.Size >= 0 ? info.Size : null,
                ModifiedOn = info.Modified != DateTime.MinValue
                    ? new DateTimeOffset(info.Modified, TimeSpan.Zero)
                    : null,
            };
        })
        .OfType<BlobItem>()
        .ToList();

        return Task.FromResult<IReadOnlyCollection<BlobItem>>(results);
    }

    public Task SetBlobsAsync(IReadOnlyCollection<BlobItem> blobs, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }

    public void Dispose() => _client.Dispose();
}

file static class ExceptionDispatchHelper
{
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    public static void Rethrow(AggregateException ex) =>
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.GetBaseException()).Throw();
}
