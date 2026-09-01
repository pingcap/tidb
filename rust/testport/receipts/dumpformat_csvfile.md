# `pkg/dumpformat/csvfile` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly five tracked artifacts and 403 lines. The files
were read in full in a detached worktree at the pinned Go commit before any
editing. The package has no fixture/testdata directory, generated or
platform-specific variant, fuzz or benchmark input, or generator source.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 26 | `3fd3d0b6028f22177edfe7d9cc6ac4544669542a` | `916e7d72515eb0234c2518c416cc3ab6b362cc996f76b9bfd087549ad16417b1` | public library and 11-shard flaky test target |
| `csv.go` | 109 | `d3d46d6f6242a731f1e47408580f6280cbbe8c00` | `e9c4f0bb975ea589604541fcfbc6350e1ae03a93a8e5e7398e622bb06468afb1` | null/kind dispatch, enclosure doubling, and backslash escaping |
| `csv_test.go` | 138 | `de54131618ef1cbbae1b9b2f41624f8cbcd21168` | `7ef00af07ab8e055ace7d36d92fce8d05f3a3ad15967cbacfa0c3c7251c6ce33` | eleven focused writer behavior tests |
| `csvfile.go` | 45 | `77fed86dde6861259cfd9ce0c7cd8b0da475a6e9` | `ed13eb88b925361e880f2be5f2b346e589e148cfae1b3362746d2034cba24dd7` | binary-format enum and CSV framing config |
| `writer.go` | 85 | `69a49c3c56883acd0945f8883ed6c79324adc881` | `5827072d62c77013c1fbd046d59934ee6de29dec46400911dff55bf7ccc9ffe5` | streaming writer, header path, size accounting, and width error |

The production surface was audited function by function: `appendField`,
`appendEscaped`, `appendEscapedBackslash`, `NewWriter`, `Write`,
`WriteHeader`, `flush`, `EstimateFileSize`, and `Close`; the three binary
format constants and all six `Config` fields were checked as well. The eleven
tests cover NUL/CR/LF/backslash/delimiter escaping, quote doubling, NULL and
kind dispatch, hex/base64 bytes, empty rows, quoted and unquoted modes,
headers, byte-size accounting, and row-width errors. The exact detached Go
suite passes.

## Rust ownership and parity decision

The Rust workspace has no `dumpformat`/CSV crate, writer call site, or
dependency-closed SQL export owner. Parser support for `LOAD DATA` and CSV
identifiers is not a writer implementation and cannot own this package's
escaping, NULL, binary-format, and I/O contracts. No Rust-only behavior was
found to remove, and no speculative CSV facade or ignored test carrier was
added.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. Validation was
run against the exact Go-master package in a detached worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/csvfile -count=1
# ok github.com/pingcap/tidb/pkg/dumpformat/csvfile 0.270s

git diff --check
```

No Go source, import section, test, Bazel target, or module dependency
changed; `make bazel_prepare` is not required. Rust tests and a full workspace
build were not run because this package has no Rust owner or changed Rust
source. CSV output compatibility remains unverified on the Rust side; the
receipt records that boundary rather than claiming parity.
