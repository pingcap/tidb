# `pkg/util/gcutil` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
This package is the session/restricted-SQL bridge for GC enablement and
snapshot safe-point validation.

## Complete inventory

Both Go-master artifacts were read in full. There are no package docs, source
tests, fixtures, generated outputs, platform variants, benchmarks, fuzz
targets, or nested packages.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 18 | `74c823ffa5d4a29b90e8d223df35940490585ac8` | `a041f1ecd7679237387e14028ec87db97b2df2d98fec10415a371c9602c0f74c` | public library target with KV/meta/session/vardef/restricted-SQL deps inventoried |
| `gcutil.go` | 91 | `42bd02f2d5450a525774baaa6a1e87aa600d23ea` | `403d73bea7e1b2cb092dffcabf3fb412f42f54e2aa4ce2f869c00bacbc14490c` | GC enable toggles, safe-point SQL read, timestamp conversion, and snapshot validation inventoried |

Total: 109 textual lines and six exported functions: `CheckGCEnable`,
`DisableGC`, `EnableGC`, `ValidateSnapshot`,
`ValidateSnapshotWithGCSafePoint`, and `GetGCSafePoint`. The package reads and
writes the global `TiDBGCEnable` variable through a session accessor, executes
the internal `tikv_gc_safe_point` query with the GC internal-source marker,
parses compatible GC timestamps, converts them through the TiKV oracle, and
returns `ErrSnapshotTooOld` when the requested snapshot is below the safe
point. Missing or malformed rows are surfaced as errors.

## Rust ownership and integration decision

Rust preserves GC variable names/defaults, a PD/transaction safe-point cache,
and executor tests for table flashback and snapshot rejection. Those owners do
not expose the Go package's session-context global-variable mutator, restricted
SQL over `mysql.tidb`, `CompatibleParseGCTime` conversion, or public helper
surface. Adding a detached SQL helper would duplicate session and GC worker
ownership without the required `GlobalVarsAccessor` and TiKV client seams.
The package is explicitly unclaimed; no source change is justified.

## Validation

Profile: **WIP**. This is a complete two-artifact inventory and explicit
boundary audit with no code change, so `make bazel_prepare` and the Ready lint
gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/gcutil -count=1
# ? github.com/pingcap/tidb/pkg/util/gcutil [no test files]
```

## Risks and unverified behavior

- Correctness: safe-point comparison is a strict `safePointTS > snapshotTS`
  check and uses the source error; no Rust replacement is claimed.
- Compatibility: global variable access, internal source tagging, SQL row
  cardinality, GC timestamp parsing, and Oracle timestamp conversion remain
  cross-package contracts.
- Performance: no runtime code changed; the Go helper issues one restricted
  SQL query per validation.
- Not verified locally: live TiKV/mysql.tidb safe-point reads, session global
  variable writes, parser error text for malformed timestamps, and Bazel
  analysis.
