namespace NaschStorage.Virtual;

public sealed class VirtualStorageBuilder
{
    private readonly Dictionary<string, IBlobStore> _mounts = new(StringComparer.Ordinal);

    public VirtualStorageBuilder Mount(string prefix, IBlobStore store)
    {
        var normalized = $"/{prefix.Trim('/')}";
        if (!_mounts.TryAdd(normalized, store))
        {
            throw new ArgumentException($"Mount already exists: {normalized}", nameof(prefix));
        }

        return this;
    }

    public VirtualBlobStore Build()
    {
        if (_mounts.Count == 0)
        {
            throw new InvalidOperationException("At least one mount is required.");
        }

        return new VirtualBlobStore(new Dictionary<string, IBlobStore>(_mounts, StringComparer.Ordinal));
    }
}
