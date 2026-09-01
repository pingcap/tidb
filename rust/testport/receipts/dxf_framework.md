# `pkg/dxf/framework` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly two tracked artifacts and 190 lines. The package
documentation was read before inspecting its build metadata. There are no
production implementations, tests, fixtures, `testdata`, generated sources or
inputs, platform variants, benchmarks, fuzz targets, or `OWNERS` files.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `397bebfa2e049c009d848a1f53ea604769409d96` | `a53f7aeaed9d53ca2920d3ad070d814ddd27a0df4df45eaf8c5f8e14758cc63e` | public Bazel `go_library` target for the package documentation |
| `doc.go` | 182 | `7172c21d9c5c1c54642225cfe551328ee8f7399f` | `ceb17727f3b6fd7cff467603ea86f33d9b4e9f09de7d05935bdaa6a3042cb204` | package guide describing DXF ownership, slots, scopes, tasks, and state machines |

The current files are byte-identical to the pinned Go-master artifacts.

## Native integration decision

This is a documentation-only Go package boundary. Rust's `tidb-dxf` crate
contains selected task/resource/step values, but it is not a dependency-closed
owner for the Go package's framework-wide scheduler, executor, storage, and
session contracts. No Rust-only behavior was identified to remove, and no
speculative Rust facade or duplicate package guide was added.

## Validation and risk

Profile: **Ready** for a documentation-only package inventory. The Go package
has no executable declarations or tests; the package compile probe is:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework -count=1 -run '^$'
```

The required repository Ready gates are recorded in the package ExecPlan. No
runtime, compatibility, or performance behavior changed.

## Outcome

The complete top-level framework package is inventoried and explicitly
bounded as Go-native documentation. The rolling audit continues with the next
unreceipted package.
