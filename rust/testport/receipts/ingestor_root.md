# `pkg/ingestor` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The root package contains three tracked artifacts and 42 lines. The package
documentation was read first, followed by the Bazel target and ownership
policy, before this receipt was written. There are no production
implementations, tests, fixtures, generated sources, benchmarks, fuzz targets,
or platform variants at this package level. Go master updates only the
ownership filters; no executable behavior changed.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `c97477636e76da8a15264a42936c1af1270aab80` | `3f33d20393a604cd86588ec6b029bf577fb03f0adf063081e860b4de8856bb43` | public documentation-package target |
| `OWNERS` | 10 | `b0ec88fa7f4a673ce2756b5ee1df13109f419507` | `fbe91cbca5b32d4c464759828c2bda2fe64e8616ddad0693c7de035b3b28d3ae` | BUILD-specific and package ownership filters |
| `doc.go` | 24 | `bf4a000d2dd2d77ad5581aba266ebc3bdcb4c110` | `bb28d3f75966825fb12d0775b79e33234aea057453af3a2f6ee477f3ad1f8945` | package scope and gradual-migration documentation |

The doc establishes that implementations currently live in Lightning and
that this package is the intended home for direct SST ingest, local/global
sorting, region preparation, and import-mode setup. Those behaviors are
implemented in child packages and consumers, not in the root package itself.

## Rust ownership and explicit boundary

The Rust workspace has no root ingestor crate corresponding to this empty Go
landing package. DXF step metadata and parser/AST support are owned by their
respective crates, while physical ingest/sort/region preparation remains an
explicit unimplemented boundary. Creating a placeholder `tidb-ingestor`
crate would claim no executable Go behavior and would be speculative.

No Rust-only behavior was found to remove and no compatibility API was added.
The root package remains a documentation/ownership boundary; its child
packages must be inventoried atomically as their own owners.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed, so `make bazel_prepare` and Ready lint are not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/ingestor -count=1
# passed: package compiled; no test files
```

Not verified here: child ingestor implementations, ownership checks, Bazel,
or full workspace tests.
