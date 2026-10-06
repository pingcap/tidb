# Consolidate utility contract tests

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Retire duplicate utility test carriers and checks of standard-library behavior.
Work in /workspace/tidb on hparser-integration from
1620b1a549ac7d3ec27e6e707cd3086b36836cd5. Go master is
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. A carrier is an integration file
that repeats tests already owned by the implementation's unit tests.
Preserve distinct Go semantics and Rust correctness checks in their owners.

## Progress


- [x] Compare context, format, table-filter and tikvutil carriers with owners.
- [x] Migrate unique assertions and retire three redundant carriers.
- [x] Validate 22 grouped owner/consumer tests, lint, metadata and diff.
- [ ] Commit through actual locked-build hook; fresh locked build, push and verify.
- [ ] Save verified recovery bundle and reusable Cloud checkpoint.

## Milestones and Plan of Work


Remove context_contract.rs and format_contract.rs from tidb-util/tests and
its all.rs registrations. Move warning JSON empty levels, append caps and
callback assertions into context/warn.rs tests; preserve the context-ID check
in context/mod.rs. Plan-cache tests already cover the removed scenarios.
Move util escaping vectors into src/format.rs; the datatype formatter owner
already tests the shared formatter, and receives the unique empty-write error.
Remove table-filter's compile-only Send/Sync check while retaining its Unicode
and config tests. Remove tikvutil's AtomicI32 test: it writes the expected
initial value before reading it and otherwise tests Rust's standard library.
Retain the config test that proves the actual runtime atomic consumer.
No production semantics, dependencies or Go files change.

## Concrete Steps and Validation


Source /workspace/.cloud-setup/env.sh, set CARGO_BUILD_JOBS=1, and work in rust/:

    cargo test --locked -p tidb-util --lib -- context:: format::
    cargo test --locked -p tidb-util --test all -- table_filter_contract::
    cargo test --locked -p tidb-datatype --test all -- parser_format_package_source::
    cargo test --locked -p tidb-config --lib -- test_get_tikv_config_uses_the_runtime_committer_concurrency

Require nonzero passing tests for each selection. Run make lint and git diff
--check from root. Keep distinct failures visible. Do not broaden into unrelated
suites. The real pre-commit hook must run cd rust && cargo build --locked -p
tidb-server; rerun immediately before the authorized push to pingcap/tidb
hparser-integration and verify remote SHA. Never bypass hooks or force push.

## Surprises & Discoveries


The existing handler_ext_and_cap test named the cap without exercising it.
The retired carrier supplies that coverage. Util and parser OutputFormat differ
in backslash escaping, so util's distinct vectors must remain.

## Decision Log


Remove only proven duplicate or language-only checks. Preserve callback panic,
context-ID and invalid-UTF8 regressions even where Go has no matching test.
Absence of a Go test alone does not make a Rust correctness assertion useless.

## Outcomes & Retrospective


Implementation and validation are complete: 22 tests pass, lint and metadata
checks pass, 278 net Rust lines and three carriers removed. Publication pending.
No structural findings are closed by harness maintenance and no measured
performance improvement is claimed.

## Recovery, Artifacts and Dependencies


Restore individual before-images using git show 1620b1a:<path>, preserving
concurrent changes. Logs are under /workspace/.cloud-setup/utility-contract-cleanup.
Durable receipt: rust/docs/parity/current-audit/utility-contract-cleanup-validation.json.
No interface/dependency changes. Publish and fresh-task restoration are separate
from a saved Cloud draft. Update this document as checks complete.

Revision: replace completed JSON carrier plan with utility contract cleanup.
