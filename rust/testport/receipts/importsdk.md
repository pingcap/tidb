# `pkg/importsdk` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains 18 tracked artifacts and 3,879 lines. Every production,
test, Bazel, and generated mock artifact was read line by line before this
receipt was written. There are no additional platform/build-tag variants,
fixtures, benchmarks, fuzz targets, or generated inputs. `mock/sdk_mock.go` is
the only generated output and is included in the atomic inventory.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 67 | `ec05790001cca61d70aa272fac3b2857c5a8e1ef` | `98fd44cc4f8643c08477cc9208068f49f0cbe7229848aea975c656634e69b7d9` | public SDK library and test target |
| `config.go` | 150 | `9d68336c4f8ce7699e7196cdfeaeeac978eca4a2` | `08392ee85e2ba3a2c8c0a6a8746b758391448e7a9c22a30c1ca7fb3b7559a74a` | SDK options, defaults, routes, charset, CSV, and scan controls |
| `config_test.go` | 94 | `928b169f452dd4c3063c2b406edcb3837eb4278b` | `fc06c2c45156b23248ca0f2d568ea939162cd05c87307089958b50b41e7fcf9e` | configuration default and option tests |
| `error.go` | 46 | `eb88c1bd907115d699f94732f0982e8df48e6715` | `fa848226ef9d532c663d8adebfab6bd906cf070a0e4edf50663d94bec1d6695a` | exported SDK error sentinels |
| `file_scanner.go` | 492 | `e7827b3650ddb01f39d7bac0249b0ae8e04a038f` | `e09d1450e8c5cbbdf79b46d712e6d75edd93b6f84f1fe3d5c9b29b3bbd66d013` | external-storage discovery, schema/table scanning, metadata, size estimates |
| `file_scanner_test.go` | 495 | `103bf822a5cbf8f17b48de88e255f0256fa3675a` | `8f5a79ef20827662cacfa26eeab33bb1b8a91e651548e46424578dee3645f1c6` | local scanner, metadata, redaction, wildcard, and estimate tests |
| `job_manager.go` | 258 | `c2ab3a0c3b498ff915b47d989d1316714e691bf7` | `3c3ff0fc8250a271fd150b128d80f200ce2c00b2759ae96df4a89a29c3268a8c` | IMPORT job submit/status/group/cancel SQL wrapper |
| `job_manager_test.go` | 242 | `317a182f069db45c8c859f752e8d83bbed298bcf` | `24bedc5eb2a4c6b62791400a568b9e2ce8c602d7982c6e8fff1d3debfb2764c5` | job-manager SQL and scan/error tests |
| `mock/BUILD.bazel` | 12 | `0e9df33acba1517f16d642414641ba62928c458b` | `f8703f08ff415a55c6f506189596e504dbdbfdfb83dc06f7d8b6efc71c63eddb` | generated GoMock target |
| `mock/sdk_mock.go` | 494 | `34508c0fd8b00af6b4ae24bcbb8af991eeffb197` | `9de7adcf13aa528c226af25895dd99abb35e960c902c9fd1894b52872ed628c6` | MockGen output for SDK interfaces |
| `model.go` | 137 | `94e01166a9256721e7934b70a4a6425b79cb1e97` | `9537e8c17cc0e5b7f462c379d4bce8fedb54661e58c04ebbcf8f095702e12168` | table/file metadata, import options, and job status models |
| `model_test.go` | 82 | `9759060c8932f13fa5607ad5303b45f393be18be` | `7ac011155ced01e6144223908d6d46c790f9af2e2f94b7bcb473c0faf7382eb1` | terminal job-state tests |
| `pattern.go` | 218 | `befae5e8b97216cf27599d6b842e716c2b28b9d7` | `bedcefebee326423c57ce62db89b1155c6e96ae0f4af94bc72e8aa0a6e0f5924` | Mydumper and prefix/suffix wildcard generation |
| `pattern_test.go` | 262 | `997c82d0a83b9471ee6df1ef1d03a67035962508` | `d82f9fbe9f00270319c097b621f986601ef6c59b67cbbb6468f885a142d3c442` | wildcard specificity and path-pattern tests |
| `sdk.go` | 62 | `b68a7ad584876b79102371eba573ee6b81688efe` | `90fddc2560f4924ab75166a3863e85422d4f72094754e3b0af00026f97d05001` | SDK composition and lifecycle |
| `sdk_test.go` | 434 | `b7bfef106ab69c01215b24789b7faf222d9a2022` | `70a73ce69c6e3a3cd499d4320d10022b7148def97c5341bbcadda41f651d3b89` | fake-GCS end-to-end scanner/SDK tests |
| `sql_generator.go` | 161 | `1b27223ac1799863b48fe3064e166f2674a669de` | `fbbbb0a4d3afc63c1d7d198f537118a90c2862a67f5a41113a530a5ee43b16b6` | IMPORT INTO SQL and CSV option escaping |
| `sql_generator_test.go` | 173 | `5c4a193b555fdbf7f72eb19b3655de055d3e3348` | `5fd3d7db159e8793785e9bbe7fa6423e0b4d7a11410aac65c312550960a86347` | SQL generation, options, and escaping tests |

The production files cover cloud/local external-storage construction and
credential redaction, schema extraction, wildcard routing, data-size
estimation, IMPORT SQL generation, and the `SHOW/CANCEL IMPORT` job API. The
tests exercise those behaviors plus the generated mock interfaces; no source
delta exists between the pinned implementation and current Go master for this
package.

## Rust ownership and explicit boundary

Rust has no dependency-closed equivalent of this Go SDK. `tidb-parser` and
`tidb-ast` own the `IMPORT INTO` grammar/AST and their source-derived tests,
while session/model crates carry import-related statement metadata. No Rust
crate owns the Go SDK's external-storage scanner, Mydumper/file-router and
schema importer, size estimator, SQL generator API, or import-job SQL manager.
The generated GoMock output is test support and has no Rust production
counterpart.

No Rust-only behavior was found to remove, and adding a Rust SDK or a
placeholder compatibility layer would invent external-storage and job-service
behavior absent from the current Rust ownership graph. This package is
therefore recorded as an explicit boundary; future Rust import execution must
first establish a real owner and then consume this complete inventory.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, Bazel metadata, or module files changed, so `make bazel_prepare` and
the Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/importsdk -run '^(TestDefaultSDKConfig|TestSDKOptions|TestCreateDataFileMeta|TestProcessDataFiles|TestFileScanner|TestFileScannerWith|TestSubmitJob|TestGetJobStatus|TestCancelJob|TestGetGroupSummary|TestGetJobsByGroup|TestJobStatus|TestLongestCommonPrefix|TestLongestCommonSuffix|TestGeneratePrefixSuffixPattern|TestGenerateMydumperPattern|TestValidatePattern|TestGenerateWildcardPath|TestGenerateImportSQL)$' -count=1
# passed: all non-cloud-storage source tests
```

The unfiltered package command was also run and failed only in the five
fake-GCS suite cases because the environment has no Google Application
Default Credentials; those failures occur during `NewFileScanner` setup and
are unrelated to this receipt. No Rust code changed, so no Rust build gate was
needed. Not verified here: fake-GCS execution with injected credentials,
Bazel execution, downstream import consumers, or full workspace tests.
