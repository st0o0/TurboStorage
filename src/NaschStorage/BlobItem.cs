namespace NaschStorage;

public sealed record BlobItem
{
    public required string Path { get; init; }
    public BlobKind Kind { get; init; }
    public long? Size { get; init; }
    public DateTimeOffset? CreatedOn { get; init; }
    public DateTimeOffset? ModifiedOn { get; init; }
    public string? ContentType { get; init; }
    public string? ETag { get; init; }
    public IReadOnlyDictionary<string, string>? Properties { get; init; }
}
