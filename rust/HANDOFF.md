# Continue the TiDB Rust rewrite

This is the active ExecPlan. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as required by
`PLANS.md` at repository root.

## Purpose / Big Picture

Deliver a standalone Rust TiDB SQL node without losing Go behavior or any
original test/support obligation. Work moves by complete Go package. The next
observable result is another dependency-ready package that passes all owning
crate tests and is current in `rust/ported-packages.json`.

## Progress

- [x] Replaced feature-slice scheduling with whole-package transcreation.
- [x] Removed queues, claims, campaigns, transfer ledgers, receipts, and copied
  source/test inventories from the normal development loop.
- [x] Reduced Cargo integration targets from 429 to 70 by aggregating ordinary
  test files while retaining every test source.
- [x] Recorded seven current packages in `rust/ported-packages.json`.
- [x] Transcreated `pkg/parser/charset`, the highest-value dependency-ready
  parser leaf, including all 11 production files, 2 test files, 10 test or
  benchmark obligations, and `BUILD.bazel`.
- [ ] Continue dependency-closed parser, datatype, protocol, storage, session,
  planner, and executor packages toward the deployable read-only node.

## Surprises & Discoveries

- Observation: per-package proof files copied hundreds of lines from the Go
  tree but did not prove that each Rust test mapped to each Go obligation.
  Evidence: the old acceptance tool required only one declared test target.
- Observation: workspace test startup dominated warm validation when every
  Rust test file was a separate integration binary.
  Evidence: aggregation reduced targets from 429 to 70 and the warm grouped
  checkpoint completed in 28.68 seconds.
- Observation: `encoding_rs` already implements GB18030-2022, so the charset
  package can verify TiDB's vectors against a maintained codec rather than
  hand-copying the large generated mapping table.
- Observation: the source HTML encoding table has 218 aliases over six runtime
  behaviors; generating aliases and implementing the six behaviors is both
  smaller and more reviewable than 218 hand-written branches.
- Observation: a custom workspace test scheduler added mechanism but was not
  part of the package loop. Ordinary Cargo over aggregated targets is simpler.

## Decision Log

- Decision: the minimum unit is one complete upstream Go package or module;
  one package may map to several Rust crates.
  Rationale: this preserves source/test closure while allowing Rust-native
  dependency and compile-time boundaries.
  Date/Author: 2026-07-22, Codex.
- Decision: keep one compact current-state manifest containing only source
  digest and owning crates.
  Rationale: Go is the inventory, Cargo is the test runner, and Git is the
  history; copying those facts creates drift and overhead.
  Date/Author: 2026-07-22, Codex.
- Decision: package recording runs owning-crate all-target tests; workspace
  formatting, Clippy, and all tests run once before push.
  Rationale: pay shared compile/startup cost once without weakening the final
  checkpoint.
  Date/Author: 2026-07-22, Codex.

## Outcomes & Retrospective

The implementation loop now has two operations in `rust/scripts/port.py`:
`inventory` and `record`. `pkg/parser/charset` is the eighth current package.
The next outcome is another complete package, not more workflow infrastructure.
The overall rewrite remains far from complete; current live SQL/PD/TiKV paths
are bounded evidence, not parity.

## Context and Orientation

The stable design is `docs/design/2026-07-11-tidb-rust-rewrite.md`. Rust code
lives in the Cargo workspace under `rust/crates/`. Differential suites live in
`rust/difftests/`. `rust/ported-packages.json` is the exact current package
set. `record` validates the selected package's dependency closure before it
tests and updates the manifest.

The workspace already contains a connected Rust MySQL listener, parser,
planner seed, executor seed, real PD/region/TiKV reads, and optimistic
prewrite/commit/rollback. These paths are useful for integration testing but do
not make their upstream Go packages complete.

Current recorded packages are:

- `pkg/parser/auth`
- `pkg/parser/charset`
- `pkg/parser/format`
- `pkg/parser/mysql`
- `pkg/parser/opcode`
- `pkg/parser/terror`
- `pkg/parser/util`
- `pkg/server/internal/handshake`

The largest remaining areas are root parser/AST, complete datatypes and codecs,
session/server behavior, planner/executor semantics, storage/distributed query,
and domain/metadata/statistics/DDL.

## Plan of Work

Next transcreate `pkg/parser/types`: 3 production files, 2 test files, 6
test/benchmark obligations, and `BUILD.bazel`. Its five internal dependencies
are current, and it unlocks AST/test-driver work with more downstream value
than the independent one-file `duration` or `tidb` leaves. Always translate the
complete package; never promote a partial feature.

## Concrete Steps

Run from repository root unless a command starts with `cd rust`:

    rust/scripts/port.py inventory pkg/parser/types --verbose
    cd pkg/parser && go test ./types
    cd rust && cargo test --offline --locked -j12 -p <owning-crate> --all-targets
    cd rust && scripts/port.py record pkg/parser/types -p <owning-crate>
    cd rust && cargo fmt --all -- --check
    cd rust && cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
    cd rust && cargo test --offline --locked -j12 --workspace --all-targets

Expected recording output ends with `recorded pkg/parser/types`. Pre-push
validation must report zero Clippy warnings, passing workspace and doc tests,
passing `test_port.py`, and clean `git diff --check` output.

## Validation and Acceptance

The package is acceptable only after every inventory entry has an explicit
Rust implementation/test disposition, the original Go package tests pass, all
owning Rust crate targets pass, the package is current in the manifest, and
pre-push Ready validation passes. Charset additionally matches TiDB's
invalid-byte replacement, UTF8MB3, GBK, GB18030, and 218 HTML-label vectors.

Repository Ready validation and `make lint` remain mandatory before claiming
the code is ready for review or push. Real PD/TiKV and MySQL-client validation
is required when a changed package reaches those boundaries.

## Idempotence and Recovery

Inventory and tests are read-only. `record` writes the manifest atomically only
after tests pass and is safe to rerun. Failed source work remains ordinary Git
working-tree state; repair it and rerun the same commands. Never bypass a stale
dependency with a downstream special case.

## Interfaces and Dependencies

`rust/scripts/port.py` owns inventory and current-state validation only.
`rust/ported-packages.json` maps a Go package digest to one or more Cargo crate
names. The Go tree owns source/test/support inventory. Cargo owns compilation
and test discovery. Git owns review, history, rollback, and atomic commits.
