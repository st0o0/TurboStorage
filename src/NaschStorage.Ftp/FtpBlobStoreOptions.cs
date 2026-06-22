using FluentFTP;

namespace NaschStorage.Ftp;

public sealed record FtpBlobStoreOptions
{
    public required string Host { get; init; }
    public int Port { get; init; } = 21;
    public string? Username { get; init; }
    public string? Password { get; init; }
    public string? BasePath { get; init; }
    public FtpEncryptionMode EncryptionMode { get; init; } = FtpEncryptionMode.Auto;
}
