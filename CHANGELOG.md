# Changelog

## [0.1.0](https://github.com/Leberkas-org/NaschStorage/compare/v0.1.0...v0.1.0) (2026-06-23)


* set initial version to 0.1.0 ([06a7818](https://github.com/Leberkas-org/NaschStorage/commit/06a7818505f4fec940f1824bf288460756a4eec5))


### Features

* add AzureBlobStore with Read/Write/Append support ([6862770](https://github.com/Leberkas-org/NaschStorage/commit/686277027fb5536bd94dfda334cd0b864c60fd98))
* add benchmarks scaffold and slopwatch baseline ([29a0eea](https://github.com/Leberkas-org/NaschStorage/commit/29a0eea049ddd3bb2b320d81f273cf53dfa989b7))
* add core data model and IBlobStore interface ([012a368](https://github.com/Leberkas-org/NaschStorage/commit/012a368d1c6ae00b568e00ed68cf583b21bb6b7d))
* add FtpBlobStore with Read/Write/Append support ([ff94c11](https://github.com/Leberkas-org/NaschStorage/commit/ff94c1192b936617a14fe889bbd0736d4bfc73a4))
* add InMemoryBlobStore implementation ([0e391f6](https://github.com/Leberkas-org/NaschStorage/commit/0e391f699029b1b6e800fa22ad6f1339d43998c3))
* add List, Delete, Exists, GetBlobs to FtpBlobStore ([0057f18](https://github.com/Leberkas-org/NaschStorage/commit/0057f18c00d15c46f5dfaaae05e718db478a416c))
* add List, Delete, Exists, GetBlobs to SftpBlobStore ([cb72cca](https://github.com/Leberkas-org/NaschStorage/commit/cb72cca03da6eca5e4f6bf99f06cfa4d972264f8))
* add List, Delete, Exists, GetBlobs, SetBlobs to AzureBlobStore ([fb9989c](https://github.com/Leberkas-org/NaschStorage/commit/fb9989ca8a73a9dbc5bc1a09ad540bbdc5869cb3))
* add LocalBlobStore implementation ([a7d6c1a](https://github.com/Leberkas-org/NaschStorage/commit/a7d6c1a6cd69014d396bdeae1c99100bb9281d7d))
* add SftpBlobStore with Read/Write/Append support ([a021054](https://github.com/Leberkas-org/NaschStorage/commit/a0210546fca82eb0950d0d623c11a4580201efcd))
* add VirtualBlobStore with mount-based routing ([70c329e](https://github.com/Leberkas-org/NaschStorage/commit/70c329e86f8098097a56d1cbac084b7e4dcdaf11))
* enhance README with project details and assets ([64ee3d4](https://github.com/Leberkas-org/NaschStorage/commit/64ee3d40907f42db6c4dadd72ace9df044c8df6f))
* Organize solution folders ([cb0a329](https://github.com/Leberkas-org/NaschStorage/commit/cb0a329d4f96a42f16cd12fc6c3a53a9244066a3))
* rename TurboStorage to NaschStorage ([e535561](https://github.com/Leberkas-org/NaschStorage/commit/e535561d84e385a509faf30b3d5a9a528e0179c3))
* replace template scaffolding with TurboStorage project structure ([9e2c05c](https://github.com/Leberkas-org/NaschStorage/commit/9e2c05c59afcb177049711f110c7ebc1f59167f7))
* **tests:** Use Akka TestKit for tests ([8e3b622](https://github.com/Leberkas-org/NaschStorage/commit/8e3b62235012844dde8024e4a6a2bd082037ca87))


### Bug Fixes

* add missing braces to single-line if statements ([c511791](https://github.com/Leberkas-org/NaschStorage/commit/c511791232f169d48a3a82aee89b76a1d252cd37))
* resolve Slopwatch SW003 empty catch block in AzureBlobStore ([55bd5df](https://github.com/Leberkas-org/NaschStorage/commit/55bd5df6a1b5b7f81da07a11c5131b8d4e01c224))


### Documentation

* add README and CLAUDE.md ([29a4136](https://github.com/Leberkas-org/NaschStorage/commit/29a413659f8b975ef915c2003956b88fd98e8cea))


### Refactoring

* add Central Package Management via Directory.Packages.props ([85f3040](https://github.com/Leberkas-org/NaschStorage/commit/85f30409df4d500d2f568e91f58ba6c9682372bc))
* add Directory.Build.props and clean up csproj files ([9a67632](https://github.com/Leberkas-org/NaschStorage/commit/9a67632fedd7ebb0aca461fce0ae8423c8c0b3ad))
* Improve exception handling and directory cleanup ([85db5f7](https://github.com/Leberkas-org/NaschStorage/commit/85db5f776610be5f00b3331147dd7b335087d084))
* simplify InMemoryBlobStore.Read metadata lookup ([fceee7e](https://github.com/Leberkas-org/NaschStorage/commit/fceee7ede77ea8ba275e4ef1716714fcf5c4f530))

## Changelog
