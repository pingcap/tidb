# `pkg/domain/globalconfigsync` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly three tracked artifacts and 203 lines: one
production file, one test file, and one Bazel build file. Every artifact was
read in full before this receipt. There is no `doc.go`, fixture directory,
`testdata`, generated source or input, platform variant, benchmark, fuzz
target, or `OWNERS` file.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 34 | `a19a4ff533e17f7288f4170f839e42b32d99b021` | `623884fcd8bc65a96535aa0e8bef1aff13d4305e170903b7c17267a3ac8d04f7` | public library and two-shard flaky test target |
| `globalconfig.go` | 55 | `6d3bad4e88f6690dbbf2614e8828bef2ef3c9254` | `98792935e40bf6ebb2c9da53ee7cd6d71bc79799ce19fcf4749af750813eacd3` | PD global-config store and notification channel |
| `globalconfig_test.go` | 114 | `ad2cf0283534a59a254c058ae4890c7e18d21968` | `763305e829fc7eabc879c198b4226aeb437122dbd0b9a569955a99e0cb06d195` | PD persistence and session-variable synchronization tests |

The production inventory has four top-level declarations. The test inventory
has `TestMain` and two top-level tests (`TestGlobalConfigSyncer` and
`TestStoreGlobalConfig`). Current files are byte-identical to every pinned
Go-master artifact.

## Native integration decision

This is Go-native domain infrastructure that adapts TiDB session/global
variables to PD's `GlobalConfigItem` API. Its channel buffering, PD naming,
mockstore lifecycle, OpenCensus shutdown, and etcd integration are not owned by
a dependency-closed Rust crate. No Rust-only behavior or missing Go behavior
was found, so no production or test edit was justified.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The complete
package integration suite passes:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    go test ./pkg/domain/globalconfigsync -count=1
    # ok github.com/pingcap/tidb/pkg/domain/globalconfigsync 2.575s

The shared Ready gates were run in the adjacent infosync batch: `make lint`,
Rust formatting, and `git diff --check` passed. `make bazel_prepare` remains
blocked locally because the `bazel` executable is not installed; this
documentation-only package has no Go/Bazel source delta requiring regeneration.

There is no runtime compatibility or performance risk from this receipt-only
change. The explicit boundary prevents a speculative Rust adapter from
silently diverging from PD's global-config contract.

## Outcome

The complete globalconfigsync inventory and Go-only ownership boundary are
recorded. The rolling package audit continues.
