using Akka;
using Akka.Streams.Dsl;

namespace NaschStorage.Virtual;

public sealed class VirtualBlobStore : IBlobStore
{
    private readonly IReadOnlyDictionary<string, IBlobStore> _mounts;

    internal VirtualBlobStore(IReadOnlyDictionary<string, IBlobStore> mounts)
    {
        _mounts = mounts;
    }

    /// <summary>
    /// Resolves a virtual path to (store, relativePath).
    /// The path must start with a known mount prefix (e.g. "/local/foo.txt" → store for "/local", "foo.txt").
    /// Throws ArgumentException if the mount prefix is unknown.
    /// </summary>
    private (IBlobStore store, string relativePath, string mountPrefix) Resolve(string path)
    {
        var normalized = path.StartsWith('/') ? path : $"/{path}";

        foreach (var kvp in _mounts)
        {
            var mountPrefix = kvp.Key; // e.g. "/local"
            if (normalized.StartsWith(mountPrefix, StringComparison.Ordinal))
            {
                // Make sure it's an exact prefix boundary: either ends here or followed by /
                var suffix = normalized[mountPrefix.Length..];
                if (suffix.Length == 0 || suffix[0] == '/')
                {
                    var relativePath = suffix.TrimStart('/');
                    return (kvp.Value, relativePath, mountPrefix);
                }
            }
        }

        throw new ArgumentException($"No mount found for path: {path}", nameof(path));
    }

    public Source<ReadOnlyMemory<byte>, Task<BlobReadResult>> Read(string path)
    {
        (IBlobStore store, string relativePath, string mountPrefix) resolution;
        try
        {
            resolution = Resolve(path);
        }
        catch (ArgumentException ex)
        {
            return Source.Failed<ReadOnlyMemory<byte>>(ex)
                .MapMaterializedValue(_ => Task.FromException<BlobReadResult>(ex));
        }

        var virtualPath = $"{resolution.mountPrefix}/{resolution.relativePath}";
        return resolution.store.Read(resolution.relativePath)
            .MapMaterializedValue(task => task.ContinueWith(
                t => t.IsCompletedSuccessfully
                    ? t.Result with { Path = virtualPath }
                    : throw t.Exception!.InnerException ?? t.Exception!,
                TaskContinuationOptions.ExecuteSynchronously));
    }

    public Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>> Write(string path, bool append = false)
    {
        var (store, relativePath, mountPrefix) = Resolve(path);
        var virtualPath = $"{mountPrefix}/{relativePath}";

        return store.Write(relativePath, append)
            .MapMaterializedValue(task => task.ContinueWith(
                t => t.IsCompletedSuccessfully
                    ? t.Result with { Path = virtualPath }
                    : throw t.Exception!.InnerException ?? t.Exception!,
                TaskContinuationOptions.ExecuteSynchronously));
    }

    public Source<BlobItem, NotUsed> List(ListOptions? options = null)
    {
        // If a prefix is provided, route to the matching mount
        if (options?.Prefix is { } prefix)
        {
            var normalizedPrefix = prefix.StartsWith('/') ? prefix : $"/{prefix}";

            // Find the matching mount
            foreach (var (mountPrefix, value) in _mounts)
            {
                if (normalizedPrefix.StartsWith(mountPrefix, StringComparison.Ordinal))
                {
                    var relativePrefix = normalizedPrefix[mountPrefix.Length..].TrimStart('/');
                    var delegatedOptions = options with
                    {
                        Prefix = string.IsNullOrEmpty(relativePrefix) ? null : relativePrefix
                    };

                    return value.List(delegatedOptions)
                        .Select(item => item with { Path = $"{mountPrefix}/{item.Path}" });
                }
            }

            // No mount matches — return empty
            return Source.Empty<BlobItem>();
        }

        // No prefix — aggregate all mounts
        var mountList = _mounts.ToList();
        if (mountList.Count == 1)
        {
            var kvp = mountList[0];
            return kvp.Value.List(options)
                .Select(item => item with { Path = $"{kvp.Key}/{item.Path}" });
        }

        var first = mountList[0];
        var second = mountList[1];
        var rest = mountList.Skip(2).ToArray();

        var firstSource = first.Value.List(options)
            .Select(item => item with { Path = $"{first.Key}/{item.Path}" });
        var secondSource = second.Value.List(options)
            .Select(item => item with { Path = $"{second.Key}/{item.Path}" });
        var restSources = rest
            .Select(kvp => kvp.Value.List(options)
                .Select(item => item with { Path = $"{kvp.Key}/{item.Path}" }))
            .ToArray();

        return Source.Combine(firstSource, secondSource, i => new Merge<BlobItem>(i), restSources);
    }

    public async Task DeleteAsync(IReadOnlyCollection<string> paths, CancellationToken ct = default)
    {
        // Group paths by mount, preserving order for reassembly
        var groups = paths
            .GroupBy(p => Resolve(p).mountPrefix)
            .ToDictionary(
                g => g.Key,
                g => g.Select(p => Resolve(p).relativePath).ToList());

        var tasks = groups.Select(kvp => _mounts[kvp.Key].DeleteAsync(kvp.Value, ct));
        await Task.WhenAll(tasks);
    }

    public async Task<IReadOnlyCollection<bool>> ExistsAsync(IReadOnlyCollection<string> paths,
        CancellationToken ct = default)
    {
        // We need to preserve input order in the result
        var pathList = paths.ToList();
        var results = new bool[pathList.Count];

        // Map each index to its (mountPrefix, relativePath)
        var grouped = pathList
            .Select((p, i) =>
            {
                var (_, relativePath, mountPrefix) = Resolve(p);
                return (index: i, mountPrefix, relativePath);
            })
            .GroupBy(x => x.mountPrefix)
            .ToList();

        await Task.WhenAll(grouped.Select(async group =>
        {
            var indices = group.Select(x => x.index).ToList();
            var relativePaths = group.Select(x => x.relativePath).ToList();
            var store = _mounts[group.Key];
            var storeResults = await store.ExistsAsync(relativePaths, ct);
            var storeList = storeResults.ToList();
            for (var i = 0; i < indices.Count; i++)
            {
                results[indices[i]] = storeList[i];
            }
        }));

        return results;
    }

    public async Task<IReadOnlyCollection<BlobItem>> GetBlobsAsync(IReadOnlyCollection<string> paths,
        CancellationToken ct = default)
    {
        var pathList = paths.ToList();

        // Group by mount preserving per-path info
        var grouped = pathList
            .Select((p, i) =>
            {
                var (_, relativePath, mountPrefix) = Resolve(p);
                return (index: i, mountPrefix, relativePath, virtualPath: p);
            })
            .GroupBy(x => x.mountPrefix)
            .ToList();

        var allResults = new List<BlobItem>();

        await Task.WhenAll(grouped.Select(async group =>
        {
            var relativePaths = group.Select(x => x.relativePath).ToList();
            var store = _mounts[group.Key];
            var storeResults = await store.GetBlobsAsync(relativePaths, ct);

            var prefixed = storeResults.Select(item => item with
            {
                Path = $"{group.Key}/{item.Path}",
            });

            lock (allResults)
            {
                allResults.AddRange(prefixed);
            }
        }));

        return allResults;
    }

    public async Task SetBlobsAsync(IReadOnlyCollection<BlobItem> blobs, CancellationToken ct = default)
    {
        // Group blobs by mount, rewriting paths to relative
        var grouped = blobs
            .GroupBy(b =>
            {
                var (_, _, mountPrefix) = Resolve(b.Path);
                return mountPrefix;
            })
            .ToList();

        await Task.WhenAll(grouped.Select(group =>
        {
            var store = _mounts[group.Key];
            var relativeBlobs = group.Select(b =>
            {
                var (_, relativePath, _) = Resolve(b.Path);
                return b with { Path = relativePath };
            }).ToList();
            return store.SetBlobsAsync(relativeBlobs, ct);
        }));
    }
}