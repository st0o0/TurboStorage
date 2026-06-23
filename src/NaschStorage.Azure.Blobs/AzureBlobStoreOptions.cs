namespace NaschStorage.Azure.Blobs;

public sealed record AzureBlobStoreOptions
{
    public required string ConnectionString { get; init; }
    public required string ContainerName { get; init; }
    public bool CreateContainerIfNotExists { get; init; } = true;
}
