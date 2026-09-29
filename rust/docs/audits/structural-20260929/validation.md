# Validation receipt

This is an audit/tooling change. No production Go or Rust implementation was
changed, and no package transcreation completion is claimed.

## Reference and prerequisites

From `/Users/qiliu/projects/tidb`:

```sh
git fetch origin master hparser-integration
git merge --ff-only origin/hparser-integration
```

Integration was already current at `2c120bc7face45b069dc197ab98a8f001724c140`;
master was pinned at `12b639a1161cd5a60126a47277f5ad14c320fd4a`.
All 7,726 reference-archive blobs were checked against master after the Go test,
including symlink contents. No mismatch remained. An initial verification helper
incorrectly opened symlinked directories as files; the corrected check hashes the
link target bytes for Git mode 120000.

No Go/Bazel/module inputs changed in this checkout. The Go probe is a test overlay
in an existing temporary reference, not a Go-file addition to this branch.
`make bazel_prepare` is therefore not required by this change. The Go executor
package uses failpoints (`pkg/executor/adapter.go` is one source); its test used
the repository wrapper with default `intest,deadlock` tags. Final failpoint
refcount in the reference archive is zero.

## Inventory and audit-tool checks

```sh
cargo metadata --offline --locked --filter-platform aarch64-apple-darwin --format-version 1 --manifest-path rust/Cargo.toml > /private/tmp/tidb-structural-full-metadata.json
python3 rust/docs/audits/structural-20260929/inventory.py generate --metadata /private/tmp/tidb-structural-full-metadata.json
python3 rust/docs/audits/structural-20260929/inventory.py verify
python3 -W error::ResourceWarning rust/docs/audits/structural-20260929/test_inventory.py
python3 rust/docs/audits/structural-20260929/report.py
git diff --check
```

The complete Go/Rust artifact inventory and all 23 findings' exact source needles,
line anchors and blob identities verified. Crate ownership also accounts for the
nested difftest packages without duplicate file assignment. Two focused tooling
tests passed: nested ownership and negative verification of missing Go artifacts,
incomplete coverage and absent evidence. The latter deliberately corrupts copies
in a temporary directory. Consecutive inventory regeneration produced identical
SHA-256 hashes for all four generated inventory/candidate/ignore/summary files.

The source scan and Cargo graph are bounded as described in README.md. The
ignored-test index counts annotations, including benchmark/Go-skip reasons; it
does not claim 1,686 missing implementations. The metadata graph is conservative
possible non-dev dependency reach on one target, not a dynamic call graph.
An initial unfiltered offline metadata attempt could not unpack an Android cache
entry; the target-filtered metadata command above completed successfully.

## Paired runtime probes

The preserved Rust harness is `rust-probe.txt`; its runner temporarily extends the
existing prepared-transaction fixture and restores the original bytes afterward:

```sh
python3 rust/docs/audits/structural-20260929/run-probe.py
# The runner invokes:
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib audit_workspace_structural_boundaries -- --test-threads=1 --nocapture
```

Result: one collector test passed, 459 filtered out, no ignored tests in that run.
The existing compilation warnings are retained in `rust-probe.log.gz`. A passing
collector means observations were captured, not that Go/Rust parity passed.

For Go, create `/private/tmp/tidb-structural-go-overlay.json` with this replacement
(adjust absolute paths when reproducing elsewhere):

```json
{
  "Replace": {
    "/private/tmp/tidb-go-structural-20260927/pkg/executor/rust_workspace_structural_test.go": "/Users/qiliu/projects/tidb/rust/docs/audits/structural-20260929/go-probe.txt"
  }
}
```

From `/private/tmp/tidb-go-structural-20260927`:

```sh
GOTOOLCHAIN=go1.25.14 GOFLAGS='-overlay=/private/tmp/tidb-structural-go-overlay.json' ./tools/check/failpoint-go-test.sh pkg/executor -run '^TestRustWorkspaceStructuralBoundaries$' -count=1 -v
```

Result: passed. Full output is retained in `go-probe.log.gz`, including cleanup.
The archive lacks Git repository metadata; its version-query warnings did not
prevent execution. All tracked archive contents matched the master pin afterward.

| Probe | Go | Rust |
| --- | --- | --- |
| PREPARE SELECT id FROM t | success, 1 field | success, 1 field |
| PREPARE SELECT 1 UNION ALL SELECT 2 | success, 1 field | success, 0 fields |
| PREPARE SELECT missing_column FROM t | error 1054 | success, 0 fields |
| PREPARE SELECT * FROM missing_table | error 1146 | success, 0 fields |
| NEXTVAL after CREATE SEQUENCE START WITH 7 | 7 | 7 |
| information_schema count for that sequence | 1 | 0 |

Only Rust was executed for the captured CLUSTER_CONFIG row and historical-read
refusal. Those observations do not claim a paired Go runtime test. Other findings
are source-confirmed; older differential matrices are linked with their original
scope and were not all rerun in this audit.

## Repository gates and publication

```sh
make lint
```

Passed. This gate includes Rust protocol projection checks against this branch's
pins; S13 explains why passing it is not equivalent to master protocol completeness.

These additional gates are mandatory at publication:

```sh
TERM=xterm git -c core.hooksPath=hooks commit -m 'docs(rust): inventory workspace structural parity gaps'
# The pre-commit hook must successfully execute:
(cd rust && cargo build --locked -p tidb-server)
# Rerun immediately before push:
(cd rust && cargo build --locked -p tidb-server)
git push origin HEAD:hparser-integration
```

Their actual terminal outcomes are reported with the published commit. A failing
hook/build must stop publication; a previous unlocked or pre-hook build cannot
substitute for either gate.

Not verified: full package suites, every feature/platform/generated variant,
cluster owner/failure/cancellation cases, complete external-module parity,
all earlier differential matrices, or sysbench/TPCC/TPCH/YCSB performance. There
are no measured speedups and no runtime compatibility changes in this commit.
The correctness, compatibility and performance risks listed in the register
remain open.
