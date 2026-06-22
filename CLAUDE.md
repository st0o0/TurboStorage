# NaschStorage

High-performance blob storage abstraction for .NET built on Akka.Streams.

## Build & Test

```bash
cd src
dotnet restore NaschStorage.slnx
dotnet build NaschStorage.slnx --configuration Release
dotnet test --configuration Release --no-build --solution *.slnx
```

## Run Benchmarks

```bash
cd src
dotnet run --project NaschStorage.Benchmarks/NaschStorage.Benchmarks.csproj --configuration Release
```

## Architecture

- `IBlobStore` — core interface. Read/Write use Akka.Streams `Source`/`Sink` with `ReadOnlyMemory<byte>`. List returns `Source<BlobItem>`. Delete/Exists/GetBlobs/SetBlobs are Task-based.
- `LocalBlobStore` — filesystem-backed provider. Constructor takes root directory path.
- `InMemoryBlobStore` — in-memory provider for testing/caching. Uses `ConcurrentDictionary`.
- `VirtualBlobStore` — composite provider that routes by path prefix to mounted sub-stores. Built via `VirtualStorageBuilder`.

## Key Design Decisions

- Akka.Streams public API (not hidden behind Stream/IAsyncEnumerable)
- `ReadOnlyMemory<byte>` instead of Akka's `ByteString` for idiomatic .NET
- Immutable sealed records for all data types
- No `IDisposable` on `IBlobStore` — lifecycle managed by ActorSystem
- No transactions on core interface — provider-specific if supported

## CI/CD

- Release Please for versioning (conventional commits)
- Trusted NuGet Publishing (OIDC)
- Slopwatch code quality gate
- xunit.v3 with Microsoft Testing Platform

## Conventions

- Conventional commits: feat/fix/perf/docs/chore/refactor/test/ci/build/deps
- Package lock files with locked-mode restore
- Tests: UnitTests (NSubstitute), IntegrationTests (real providers), Benchmarks (BenchmarkDotNet)
