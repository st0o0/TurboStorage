using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace NaschStorage.Sftp.IntegrationTests;

public sealed class SftpContainerFixture : IAsyncLifetime
{
    private const int SshPort = 22;
    private const string SftpUser = "testuser";
    private const string SftpPass = "testpass";

    private readonly IContainer _container;

    public string Host => "127.0.0.1";
    public int Port => _container.GetMappedPublicPort(SshPort);
    public string Username => SftpUser;
    public string Password => SftpPass;

    public SftpContainerFixture()
    {
        _container = new ContainerBuilder()
            .WithImage("atmoz/sftp")
            .WithCommand($"{SftpUser}:{SftpPass}:::upload")
            .WithPortBinding(SshPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(SshPort))
            .Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
