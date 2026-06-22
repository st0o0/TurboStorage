using Akka;
using Akka.IO;
using Akka.Streams.Dsl;
using Renci.SshNet;

namespace NaschStorage.Sftp;

public sealed class SftpBlobStore : IBlobStore, IDisposable
{
    private readonly SftpBlobStoreOptions _options;
    private readonly SftpClient _client;

    public SftpBlobStore(SftpBlobStoreOptions options)
    {
        _options = options;

        if (options.PrivateKeyPath is { } keyPath)
        {
            var keyFile = options.PrivateKeyPassphrase is { } passphrase
                ? new PrivateKeyFile(keyPath, passphrase)
                : new PrivateKeyFile(keyPath);

            var authMethods = new List<AuthenticationMethod>
            {
                new PrivateKeyAuthenticationMethod(options.Username, keyFile),
            };

            if (options.Password is { } password)
                authMethods.Add(new PasswordAuthenticationMethod(options.Username, password));

            var connInfo = new ConnectionInfo(options.Host, options.Port, options.Username, authMethods.ToArray());
            _client = new SftpClient(connInfo);
        }
        else
        {
            _client = new SftpClient(options.Host, options.Port, options.Username, options.Password ?? string.Empty);
        }
    }

    private void EnsureConnected()
    {
        if (!_client.IsConnected)
            _client.Connect();
    }

    private string ToFullPath(string blobPath)
    {
        var normalized = blobPath.TrimStart('/');
        if (_options.BasePath is { } basePath)
            return $"{basePath.TrimEnd('/')}/{normalized}";
        return $"/{normalized}";
    }

    private string ToBlobPath(string fullPath)
    {
        if (_options.BasePath is not { } basePath)
            return fullPath.TrimStart('/');

        var prefix = basePath.TrimEnd('/') + "/";
        return fullPath.StartsWith(prefix, StringComparison.Ordinal)
            ? fullPath[prefix.Length..]
            : fullPath.TrimStart('/');
    }

    private void EnsureDirectoryExists(string path)
    {
        if (_client.Exists(path)) return;

        var parent = GetParentDirectory(path);
        if (parent is not null)
            EnsureDirectoryExists(parent);

        _client.CreateDirectory(path);
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

        if (!_client.Exists(fullPath))
        {
            var ex = new FileNotFoundException($"Blob not found: {path}", fullPath);
            return Source.Failed<ReadOnlyMemory<byte>>(ex)
                .MapMaterializedValue(_ => Task.FromException<BlobReadResult>(ex));
        }

        var attrs = _client.GetAttributes(fullPath);

        return StreamConverters.FromInputStream(() => _client.OpenRead(fullPath))
            .Select(bs => (ReadOnlyMemory<byte>)bs.ToArray())
            .MapMaterializedValue(ioTask => ioTask.ContinueWith<BlobReadResult>(t =>
            {
                if (t.IsFaulted)
                    ExceptionDispatchHelper.Rethrow(t.Exception!);

                var ioResult = t.Result;
                if (!ioResult.WasSuccessful)
                    throw ioResult.Error;

                return new BlobReadResult
                {
                    Path = path,
                    Size = attrs.Size,
                    ModifiedOn = new DateTimeOffset(attrs.LastWriteTime.ToUniversalTime(), TimeSpan.Zero),
                };
            }));
    }

    public Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>> Write(string path, bool append = false)
    {
        EnsureConnected();
        var fullPath = ToFullPath(path);

        var parentDir = GetParentDirectory(fullPath);
        if (parentDir is not null)
            EnsureDirectoryExists(parentDir);

        return Flow.Create<ReadOnlyMemory<byte>>()
            .Select(mem => ByteString.FromBytes(mem.ToArray()))
            .ToMaterialized(
                StreamConverters.FromOutputStream(() => append
                    ? _client.Open(fullPath, FileMode.Append, FileAccess.Write)
                    : _client.Open(fullPath, FileMode.Create)),
                Keep.Right)
            .MapMaterializedValue(ioTask => ioTask.ContinueWith<BlobWriteResult>(t =>
            {
                if (t.IsFaulted)
                    ExceptionDispatchHelper.Rethrow(t.Exception!);

                var ioResult = t.Result;
                if (!ioResult.WasSuccessful)
                    throw ioResult.Error;

                EnsureConnected();
                var attrs = _client.GetAttributes(fullPath);
                return new BlobWriteResult
                {
                    Path = path,
                    BytesWritten = attrs.Size,
                };
            }));
    }

    public Source<BlobItem, NotUsed> List(ListOptions? options = null)
    {
        throw new NotImplementedException();
    }

    public Task DeleteAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public Task<IReadOnlyCollection<bool>> ExistsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public Task<IReadOnlyCollection<BlobItem>> GetBlobsAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        throw new NotImplementedException();
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
