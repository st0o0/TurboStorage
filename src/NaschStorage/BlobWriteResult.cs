namespace NaschStorage;

public sealed record BlobWriteResult
{
    public required string Path { get; init; }
    public long BytesWritten { get; init; }
    public string? ETag { get; init; }
}
