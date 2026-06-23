using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace NaschStorage.Azure.Blobs.IntegrationTests;

public sealed class AzuriteContainerFixture : IAsyncLifetime
{
    private const int BlobPort = 10000;

    private readonly IContainer _container;

    public int Port => _container.GetMappedPublicPort(BlobPort);

    public string ConnectionString =>
        "DefaultEndpointsProtocol=http;"
        + "AccountName=devstoreaccount1;"
        + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        + $"BlobEndpoint=http://127.0.0.1:{Port}/devstoreaccount1";

    public AzuriteContainerFixture()
    {
        _container = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite")
            .WithCommand("azurite-blob", "--blobHost", "0.0.0.0")
            .WithPortBinding(BlobPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(BlobPort))
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
