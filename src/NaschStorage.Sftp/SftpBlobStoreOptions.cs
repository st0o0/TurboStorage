namespace NaschStorage.Sftp;

public sealed record SftpBlobStoreOptions
{
    public required string Host { get; init; }
    public int Port { get; init; } = 22;
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? PrivateKeyPath { get; init; }
    public string? PrivateKeyPassphrase { get; init; }
    public string? BasePath { get; init; }
}
