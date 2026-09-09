# `pkg/domain/sqlsvrapi/mock` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The nested mock package contains exactly four tracked artifacts and 301 lines:
one Bazel target and three MockGen-generated Go files. Every artifact was read
in full before this receipt. There are no package-local tests, fixtures,
`testdata`, generated inputs, platform variants, benchmarks, fuzz targets, or
`OWNERS` files. The three Go files are generated outputs and were not
hand-edited; their source interface is the audited parent package
`pkg/domain/sqlsvrapi/server.go`.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 20 | `5bbe3b5c18d9da99074209dc77f0063dc10ec987` | `4081019c44a0e72624be3e2860ca33eaedb3856cea90ba86f22ad07f01edda05` | public generated-mock library target |
| `ksruntime_mock.go` | 102 | `613c9f5547615e56549300c484389cc2741e3cfa` | `bfb03ea2e18359ab5a23a71295eed785ae4035b02328a1a65e6c4a4b1e45ac3b` | GoMock for `KSRuntimeHandle` |
| `runtime_mock.go` | 90 | `95087533dc75ba418b6d4a0272bd1b316271868f` | `1d9517c05cda443e6d2113bd155cc75b7e7988d63ff8427fb8a671d97b451f87` | GoMock for `Runtime` |
| `server_mock.go` | 89 | `c1e9dcb9675e557a60a31c95f5303b52572300c7` | `c07a8e62821c7b2534e6129053b7ede91d3b0ca524684657d1cced2e92522c31` | GoMock for `Server` |

The generated inventory has 35 top-level type/function declarations. Current
files are byte-identical to every pinned Go-master artifact.

## Native integration decision

These mocks are Go build/test support for the parent SQL-server API and are
consumed by domain, session, and cross-keyspace tests. Rust has no compatible
GoMock/generated interface owner and no dependency-closed replacement for the
parent API. No generated output was manually changed and no Rust-only behavior
was found to remove.

## Validation and risk

Profile: **Ready** for this documentation-only generated-artifact boundary.
The parent and mock packages compile together:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    go test ./pkg/domain/sqlsvrapi ./pkg/domain/sqlsvrapi/mock

Rust formatting and `git diff --check` are shared receipt gates. No Go/Bazel
source changed, so `make bazel_prepare` is not required; if attempted in this
environment it reports the unavailable `bazel` executable.

The generated files should only be regenerated when `server.go` changes and
the repository's MockGen command is available. Treating them as an atomic
inventory avoids silently dropping methods from downstream test doubles.

## Outcome

The complete generated mock inventory and its explicit parent-package
dependency boundary are recorded. The rolling audit continues.
