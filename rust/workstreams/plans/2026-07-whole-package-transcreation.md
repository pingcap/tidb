# Execute whole-package Go-to-Rust transcreation

This ExecPlan is a living document. It follows `PLANS.md`.

## Purpose

Transcreate TiDB one complete Go package or module at a time, including every
original source, test, benchmark, fuzz target, example, fixture, support file,
build variant, and generated input. Rust crate boundaries follow Rust cohesion;
one Go package may map to several crates but has one acceptance proof.

## Progress

- [x] Established whole-package transcreation as the minimum unit.
- [x] Replaced the queue/claim/campaign/ledger/receipt workflow with one
  source-derived `package-port.py finish` command and one proof per completed
  package.
- [x] Migrated the six previously completed package results into compact proofs.
- [x] Transcreated `pkg/parser/auth` completely, including all original tests,
  MySQL native password, caching SHA-2, TiDB SM3, identity behavior, and the
  full arbitrary-byte Go string domain.
- [x] Reduced Cargo integration targets from 429 to 70 by aggregating ordinary
  test files while preserving topology-sensitive standalone targets.
- [x] Ran the grouped workspace checkpoint in 28.68 seconds warm, including
  strict Clippy and every workspace test binary.
- [ ] Continue with the next dependency-ready whole package that unblocks the
  deployable SQL-node path.

## Execution loop

1. Select a dependency-ready package with downstream value.
2. Audit the complete Go directory with `package-port.py inventory` and by
   reading the source/tests/support.
3. Transcreate production behavior structurally from Go.
4. Translate all original test and support obligations.
5. Run `package-port.py finish` to verify touched crates and write the package
   proof.
6. Commit code and proof together.
7. Run `package-port.py checkpoint`, repository Ready validation, and relevant
   live/differential suites before push.

## Decisions

- Git is the only transaction log, history, rollback, and review mechanism.
- A valid file under `rust/ports/<go-package>.toml` is the only package
  completion state.
- The proof inventory is derived from the Go tree; there is no hand-maintained
  global queue or ledger.
- Work in progress has no ownership state because there is one worker.
- Package verification is focused; workspace verification is grouped at
  pre-push/shared-foundation checkpoints.
- Many-file integration suites compile into crate-level aggregate harnesses;
  files that require integration-crate-root module topology stay standalone.

## Acceptance

A package is complete only when its proof is current, every direct internal Go
dependency has a current proof, all declared Rust paths exist, all original Go
test/support entries are inventoried, and the focused Rust checks pass. Runtime
packages additionally require the applicable differential, fault, protocol, or
real PD/TiKV proof before release readiness.
