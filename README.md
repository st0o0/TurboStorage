# NaschStorage

<div align="center">

<img alt="NaschStorage logo" height="128" src="https://raw.githubusercontent.com/Leberkas-org/NaschStorage/refs/heads/main/docs/logo/logo.svg" width="128"/>

**A high-performance blob storage abstraction for .NET built on [Akka.Streams](https://getakka.net/)**

[![License](https://img.shields.io/github/license/st0o0/NaschStorage?style=flat-square)](LICENSE)
[![Dotnet](https://img.shields.io/badge/dotnet-10.0-5027d5?style=flat-square)](https://dotnet.microsoft.com)

</div>

---

## Table of Contents

- [Features](#-features)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Core Concepts](#-core-concepts)
- [Providers](#-providers)
- [Advanced Usage](#-advanced-usage)
- [API Reference](#-api-reference)
- [Inspiration](#-inspiration)
- [Contributing](#-contributing)
- [License](#-license)

## Features

- **Akka.Streams-First API** — `Source<ReadOnlyMemory<byte>>` / `Sink<ReadOnlyMemory<byte>>` with full backpressure
- **Immutable Data Model** — sealed records for all types (`BlobItem`, `BlobReadResult`, `BlobWriteResult`)
- **Multiple Providers** — Local filesystem, in-memory, and virtual mount-based routing
- **Composable Pipelines** — plug Akka.Streams operators (throttle, buffer, map) directly into storage pipelines
- **Read Metadata** — blob metadata returned as materialized value from `Read`, no extra call needed
- **Virtual Storage** — mount multiple providers under path prefixes, route transparently

## Installation

```bash
dotnet add package NaschStorage
```

**Requirements:** .NET 10.0 or higher

## Quick Start

### Write and Read a File

```csharp
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using NaschStorage.Local;

using var system = ActorSystem.Create("my-app");
var materializer = system.Materializer();

var store = new LocalBlobStore("/path/to/storage");

// Write
var data = "hello world"u8.ToArray();
var writeResult = await Source.Single(new ReadOnlyMemory<byte>(data))
    .RunWith(store.Write("greeting.txt"), materializer);

Console.WriteLine($"Wrote {writeResult.BytesWritten} bytes");

// Read
var (readResult, chunks) = await store.Read("greeting.txt")
    .ToMaterialized(Sink.Seq<ReadOnlyMemory<byte>>(), Keep.Both)
    .Run(materializer);

var metadata = await readResult;
Console.WriteLine($"Read {metadata.Size} bytes from {metadata.Path}");
```

### In-Memory Store (Testing/Caching)

```csharp
using NaschStorage.InMemory;

var store = new InMemoryBlobStore();

await Source.Single(new ReadOnlyMemory<byte>("cached data"u8.ToArray()))
    .RunWith(store.Write("cache/key.bin"), materializer);
```

### Virtual Storage (Multi-Provider)

```csharp
using NaschStorage.Virtual;

var store = new VirtualStorageBuilder()
    .Mount("/local", new LocalBlobStore("/data"))
    .Mount("/cache", new InMemoryBlobStore())
    .Build();

// Routes to LocalBlobStore
await Source.Single(new ReadOnlyMemory<byte>(data))
    .RunWith(store.Write("/local/documents/report.pdf"), materializer);

// Routes to InMemoryBlobStore
await Source.Single(new ReadOnlyMemory<byte>(data))
    .RunWith(store.Write("/cache/temp.bin"), materializer);

// List across all mounts
var allBlobs = await store.List()
    .RunWith(Sink.Seq<BlobItem>(), materializer);
```

## Core Concepts

### Streaming with Backpressure

NaschStorage uses Akka.Streams for all data transfer, providing automatic backpressure:

```csharp
// Read returns a Source — compose with any Akka.Streams operator
var totalBytes = await store.Read("large-file.bin")
    .Select(chunk => chunk.Length)
    .RunWith(Sink.Aggregate<int, long>(0L, (sum, len) => sum + len), materializer);
```

### Blob Operations

```csharp
// Check existence
var exists = await store.ExistsAsync(["file1.txt", "file2.txt"]);

// List with filtering
var items = await store.List(new ListOptions
    {
        Prefix = "documents/",
        Recursive = true,
        MaxResults = 100,
    })
    .RunWith(Sink.Seq<BlobItem>(), materializer);

// Delete
await store.DeleteAsync(["old-file.txt", "temp/"]);

// Get metadata without reading content
var blobs = await store.GetBlobsAsync(["report.pdf"]);
Console.WriteLine($"Size: {blobs.First().Size}, Modified: {blobs.First().ModifiedOn}");

// Update metadata
await store.SetBlobsAsync([new BlobItem
{
    Path = "report.pdf",
    ContentType = "application/pdf",
    Properties = new Dictionary<string, string> { ["author"] = "Jane" },
}]);
```

### Append Mode

```csharp
// Write initial data
await Source.Single(new ReadOnlyMemory<byte>("line 1\n"u8.ToArray()))
    .RunWith(store.Write("log.txt"), materializer);

// Append more data
await Source.Single(new ReadOnlyMemory<byte>("line 2\n"u8.ToArray()))
    .RunWith(store.Write("log.txt", append: true), materializer);
```

## Providers

| Package | Providers | Description |
|---|---|---|
| `NaschStorage` | `LocalBlobStore` | Filesystem-backed, maps paths to files relative to a root directory |
| `NaschStorage` | `InMemoryBlobStore` | In-memory store using `ConcurrentDictionary`, ideal for tests and caching |
| `NaschStorage` | `VirtualBlobStore` | Composite store routing by path prefix to mounted sub-stores |

### Future Providers

| Package | Provider |
|---|---|
| `NaschStorage.AWS` | Amazon S3 (+ MinIO, Wasabi, DigitalOcean Spaces) |
| `NaschStorage.Azure.Blobs` | Azure Blob Storage |
| `NaschStorage.Azure.Files` | Azure File Shares |
| `NaschStorage.GCP` | Google Cloud Storage |
| `NaschStorage.FTP` | FTP/FTPS |
| `NaschStorage.SFTP` | SFTP |

## Advanced Usage

### Chunked File Processing

```csharp
// Process a file in chunks without loading it all into memory
await store.Read("huge-file.bin")
    .Select(chunk =>
    {
        // Process each chunk (e.g., compute hash, transform, compress)
        return chunk;
    })
    .RunWith(anotherStore.Write("processed.bin"), materializer);
```

### Copy Between Providers

```csharp
var local = new LocalBlobStore("/data");
var cache = new InMemoryBlobStore();

// Stream directly from local to in-memory — no intermediate buffering
await local.Read("source.dat")
    .RunWith(cache.Write("cached-copy.dat"), materializer);
```

### Throttled Uploads

```csharp
// Throttle write speed using Akka.Streams operators
await Source.From(largeChunks)
    .Throttle(10, TimeSpan.FromSeconds(1), 1, ThrottleMode.Shaping)
    .RunWith(store.Write("rate-limited.bin"), materializer);
```

## API Reference

### IBlobStore

| Method | Return Type | Description |
|---|---|---|
| `Read(path)` | `Source<ReadOnlyMemory<byte>, Task<BlobReadResult>>` | Stream blob content with metadata |
| `Write(path, append)` | `Sink<ReadOnlyMemory<byte>, Task<BlobWriteResult>>` | Stream data into a blob |
| `List(options)` | `Source<BlobItem, NotUsed>` | Stream blob listing with backpressure |
| `DeleteAsync(paths)` | `Task` | Delete blobs by path |
| `ExistsAsync(paths)` | `Task<IReadOnlyCollection<bool>>` | Check blob existence |
| `GetBlobsAsync(paths)` | `Task<IReadOnlyCollection<BlobItem>>` | Get blob metadata |
| `SetBlobsAsync(blobs)` | `Task` | Update blob metadata |

### Data Types

| Type | Description |
|---|---|
| `BlobItem` | Blob metadata — path, kind, size, timestamps, content type, ETag, custom properties |
| `BlobReadResult` | Read metadata — path, size, content type, ETag, modified date, properties |
| `BlobWriteResult` | Write result — path, bytes written, ETag |
| `ListOptions` | List filter — prefix, recursive, max results |
| `BlobKind` | Enum: `File`, `Folder` |

## Inspiration

NaschStorage is inspired by [FluentStorage](https://github.com/robinrodricks/FluentStorage), a polycloud storage abstraction for .NET.

### What's Different?

While FluentStorage provides a solid foundation, NaschStorage takes a different approach:

- **Akka.Streams** instead of .NET `Stream` — full backpressure and composable pipelines
- **`ReadOnlyMemory<byte>`** instead of `byte[]` — zero-copy where possible
- **Immutable records** instead of mutable classes — thread-safe by design
- **Virtual storage** — built-in mount-based routing across providers
- **Metadata on read** — `BlobReadResult` returned as materialized value, no extra API call
- **Honest capabilities** — no `NotSupportedException` at runtime; capabilities are provider-specific interfaces

## Contributing

Contributions are welcome! This library grows with the community's needs.

### How to Contribute

1. **Fork** the repository
2. **Create** a feature branch: `git checkout -b feature/amazing-feature`
3. **Write** tests for your changes
4. **Ensure** all tests pass: `dotnet test`
5. **Submit** a Pull Request

### Guidelines

- Follow existing code style and conventions
- Include unit tests for new features
- Update documentation for API changes
- Keep PRs focused and atomic
- Write meaningful commit messages (conventional commits)

### Development Setup

```bash
git clone https://github.com/st0o0/NaschStorage.git
cd NaschStorage/src
dotnet restore NaschStorage.slnx
dotnet build NaschStorage.slnx
dotnet test --solution *.slnx
```

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built with Akka.Streams for high-performance .NET storage**

[Report Bug](https://github.com/st0o0/NaschStorage/issues) · [Request Feature](https://github.com/st0o0/NaschStorage/issues)

</div>
