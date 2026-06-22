namespace NaschStorage;

public sealed record BlobReadResult
{
    public required string Path { get; init; }
    public long? Size { get; init; }
    public string? ContentType { get; init; }
    public string? ETag { get; init; }
    public DateTimeOffset? ModifiedOn { get; init; }
    public IReadOnlyDictionary<string, string>? Properties { get; init; }
}
