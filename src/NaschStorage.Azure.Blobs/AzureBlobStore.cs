using Akka;
using Akka.IO;
using Akka.Streams.Dsl;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace NaschStorage.Azure.Blobs;

public sealed class AzureBlobStore : IBlobStore
{
    private readonly BlobContainerClient _containerClient;
    private readonly bool _createIfNotExists;
    private bool _containerEnsured;

    public AzureBlobStore(AzureBlobStoreOptions options)
    {
        _containerClient = new BlobContainerClient(options.ConnectionString, options.ContainerName);
        _createIfNotExists = options.CreateContainerIfNotExists;
    }

    private void EnsureContainer()
    {
        if (_containerEnsured || !_createIfNotExists) return;
        _containerClient.CreateIfNotExists();
        _containerEnsured = true;
    }

    public Source<ReadOnlyMemory<byte>, Task<BlobReadResult>> Read(string path)
    {
        EnsureContainer();
        var client = _containerClient.GetBlobClient(path);

        BlobProperties properties;
        try
        {
            properties = client.GetProperties().Value;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            var notFound = new FileNotFoundException($"Blob not found: {path}", path);
            return Source.Failed<ReadOnlyMemory<byte>>(notFound)
                .MapMaterializedValue(_ => Task.FromException<BlobReadResult>(notFound));
        }

        return StreamConverters.FromInputStream(() => client.OpenRead())
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
                    Size = properties.ContentLength,
                    ContentType = properties.ContentType,
                    ETag = properties.ETag.ToString(),
                    ModifiedOn = properties.LastModified,
                    Properties = properties.Metadata.Count > 0
                        ? properties.Metadata.ToDictionary(kv => kv.Key, kv => kv.Value)
                        : null,
                };
            }));
    }

    public Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>> Write(string path, bool append = false)
    {
        EnsureContainer();

        return Flow.Create<ReadOnlyMemory<byte>>()
            .Select(mem => ByteString.FromBytes(mem.ToArray()))
            .ToMaterialized(
                StreamConverters.FromOutputStream(() =>
                {
                    if (append)
                    {
                        var appendClient = _containerClient.GetAppendBlobClient(path);
                        var blobClient = _containerClient.GetBlobClient(path);

                        // Check if blob exists and what type it is
                        bool existsAsBlockBlob = false;
                        try
                        {
                            var props = blobClient.GetProperties();
                            // If we get here, blob exists; check type
                            existsAsBlockBlob = props.Value.BlobType != BlobType.Append;
                        }
                        catch (RequestFailedException ex) when (ex.Status == 404)
                        {
                            // Blob does not exist; will be created as append blob
                        }

                        if (existsAsBlockBlob)
                        {
                            // Convert block blob to append blob: read existing content, delete, recreate
                            byte[] existing;
                            using (var ms = new MemoryStream())
                            {
                                blobClient.DownloadTo(ms);
                                existing = ms.ToArray();
                            }
                            blobClient.Delete();
                            appendClient.Create();
                            var writeStream = appendClient.OpenWrite(overwrite: false);
                            if (existing.Length > 0)
                            {
                                writeStream.Write(existing, 0, existing.Length);
                                writeStream.Flush();
                            }
                            return writeStream;
                        }

                        appendClient.CreateIfNotExists();
                        return appendClient.OpenWrite(overwrite: false);
                    }
                    return _containerClient.GetBlobClient(path).OpenWrite(overwrite: true);
                }),
                Keep.Right)
            .MapMaterializedValue(ioTask => ioTask.ContinueWith<BlobWriteResult>(t =>
            {
                if (t.IsFaulted)
                    ExceptionDispatchHelper.Rethrow(t.Exception!);

                var ioResult = t.Result;
                if (!ioResult.WasSuccessful)
                    throw ioResult.Error;

                var props = _containerClient.GetBlobClient(path).GetProperties().Value;
                return new BlobWriteResult
                {
                    Path = path,
                    BytesWritten = props.ContentLength,
                    ETag = props.ETag.ToString(),
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
        throw new NotImplementedException();
    }
}

file static class ExceptionDispatchHelper
{
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    public static void Rethrow(AggregateException ex) =>
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.GetBaseException()).Throw();
}
