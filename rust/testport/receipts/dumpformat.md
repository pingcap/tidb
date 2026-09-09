# `pkg/dumpformat` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

This is the package-root claim only. The CSV, Parquet, SQL, parser-definition,
and test-utils subpackages are separate package units and are not silently
included here.

## Complete inventory

Exactly three tracked artifacts (46 lines) were read before editing:

| artifact | lines | Git blob | SHA-256 | inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `c921d0615d773703f3f668508305b929151b5f2b` | `1f6fae3d7947933dd2d2994c2a9b4ae838622e10305be6146c155afa2caad4f3` | public `go_library` exposing `kind.go` |
| `OWNERS` | 10 | `9ae75f2e21d56448645b60ae870209397a07bf4e` | `4d6c70de483d6a2c6ec0ff28915bdd03be836883b0845514e624684277e24562` | community BUILD approvers and dumpling SIG source approvers |
| `kind.go` | 28 | `f89b4946aad0904ff493e716fa78d0e9a7303381` | `3d9340cd006c2a69c3c8c265213ed843531da79e042462541e0687e56d9a2f30` | `FieldKind` (`uint8`) and the `KindNumber`, `KindString`, `KindBytes` iota values |

The root has no tests, fixtures, generated/platform variants, benchmarks,
fuzz corpora, or generator inputs. Nested format artifacts are intentionally
outside this root package inventory.

## Owner comparison and parity decision

Go's root API is a format-agnostic three-way column classification consumed by
the nested CSV/Parquet/SQL writers. The Rust workspace has no dumpformat crate,
writer, or call site that can own this enum without fabricating an unused
parallel API. No Rust-only behavior was found to remove and no safe standalone
implementation was added. The nested packages remain explicit follow-up
claims where their writer behavior can be compared with real consumers.

## Validation (Ready profile)

The root package is new relative to the hparser branch, so the exact
Go-master root package was compiled in a detached worktree at the comparison
commit rather than against the branch working tree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat -count=1
? github.com/pingcap/tidb/pkg/dumpformat [no test files]
```

Repository formatting, lint, and diff hygiene were run for this docs-only
checkpoint:

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go source, import, Bazel, or module file changed, so `make bazel_prepare` is
not required.

## Risks and unverified surfaces

This root receipt does not validate the nested writer formats or their binary
fixtures. CSV/Parquet/SQL output compatibility, generated Spark-rebase data,
and test-only Parquet files require separate dependency-closed package audits.
The branch intentionally retains its existing Go tree; this Rust parity batch
does not copy new Go-master package files into the integration branch.
