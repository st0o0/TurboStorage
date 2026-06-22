using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace NaschStorage.Ftp.IntegrationTests;

public sealed class FtpContainerFixture : IAsyncLifetime
{
    private const int ControlPort = 21;
    private const int PassivePortStart = 30000;
    private const int PassivePortEnd = 30009;
    private const string FtpUser = "testuser";
    private const string FtpPass = "testpass";

    private readonly IContainer _container;

    public string Host => "127.0.0.1";
    public int Port => _container.GetMappedPublicPort(ControlPort);
    public string Username => FtpUser;
    public string Password => FtpPass;

    public FtpContainerFixture()
    {
        var builder = new ContainerBuilder()
            .WithImage("fauria/vsftpd")
            .WithEnvironment("FTP_USER", FtpUser)
            .WithEnvironment("FTP_PASS", FtpPass)
            .WithEnvironment("PASV_ADDRESS", "127.0.0.1")
            .WithEnvironment("PASV_MIN_PORT", PassivePortStart.ToString())
            .WithEnvironment("PASV_MAX_PORT", PassivePortEnd.ToString())
            .WithPortBinding(ControlPort, true);

        for (var port = PassivePortStart; port <= PassivePortEnd; port++)
        {
            builder = builder.WithPortBinding(port, port);
        }

        _container = builder
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(ControlPort))
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
