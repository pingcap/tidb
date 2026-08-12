# Audit `pkg/util/logutil` against the Rust owner

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

Certify the complete Go package `pkg/util/logutil` against its Rust owner
`rust/crates/tidb-util/src/logutil`. The Go package's unit tests are the source
of truth for behavior and test parity. The nested `pkg/util/logutil/consistency`
directory is a separate Go package and remains outside this package-scoped
commit.

## Progress

- [x] (2026-08-12) Fixed the package boundary and source pin
  `3606de5c43fcf4fa5206596c41cd0793403b9818`; the top-level Go files are
  byte-identical to that pin.
- [x] (2026-08-12) Inventoried all top-level production/test files, Bazel
  inputs, Go tests, `TestMain` setup, imports, and Rust owner files.
- [x] (2026-08-12) Confirmed the Go package has no `doc.go`, failpoint,
  generated input, platform/build-tag variant, benchmark, fuzz target, or
  example; failpoint checks found no matches and no Bazel failpoint dependency.
- [x] (2026-08-12) Ran the Go package unit suite and the Rust logutil test
  slice on the clean target base; both pass before audit edits.
- [x] (2026-08-12) Reconciled the identified Rust semantic gaps and added
  focused regressions for slow-log field composition, RFC3339Nano output,
  sink/level identity, default global level control, proxy-variable priority,
  sampler buckets/windows/disabled levels, and replacement logger
  construction.
- [x] (2026-08-12) Completed Ready validation after the final fixes: Go normal
  and race suites, Rust logutil and full-crate suites, Clippy with warnings
  denied, formatting, semantic package gate, repository lint, source pin,
  inventory, failpoint decision, and diff checks all pass.
- [ ] Publish one package-scoped commit to `hparser-integration` and verify
  local, remote-tracking, and `ls-remote` SHAs.

## Surprises & Discoveries

- The Rust owner already ports all 12 Go tests (plus one file-rotation support
  test), but its `Logger::with_fields` unconditionally changes a slow-query
  logger from `Encoding::SlowLog` to `Encoding::Unified`. Go zap preserves the
  wrapped slow-log core when `With` is called.
- Rust creates distinct stdout sinks for equal empty filenames, although the
  Go initialization path reuses the global write syncer when the effective
  filenames are equal.
- Rust sampling counts only the message and exposes only `info`/`warn` methods;
  Go zap's sampler key is `(level, message)` and the package's
  `SampleErrVerboseLoggerFactory` is used by Go callers at `Error` level.
- Zap implements its `(level, message)` key as 4096 FNV-1a buckets per level,
  so colliding messages intentionally share a counter and memory is bounded.
  The initial Rust repair used an unbounded exact-key map; a generated collision
  (`message 24` and `message 200`) exposed and corrected that second-order gap.
- The Rust lazy default global stored one `AtomicLevel` in `Globals` but gave
  the stdout logger a different one, so `set_level` before explicit
  initialization changed `get_level` without changing emission filtering.
- The pinned `golang.org/x/net/http/httpproxy` implementation reads uppercase
  variables before lowercase variables. Rust had the priority reversed even
  though single-case Go package tests passed.
- Go's `ReplaceLogger` is intentionally different from `InitLogger`: it always
  reconstructs dedicated slow/general loggers, and those constructors each
  create an independent writer even when both filenames are equal. A temporary
  Go probe printed distinct lumberjack writer addresses for this case.
- The full Go package suite passes on the source pin, and the Rust logutil
  tests pass on the target base before changes. No Go testdata or integration
  result files belong to this package.

## Decision Log

- Decision: Treat `pkg/util/logutil` as one atomic Go package and do not include
  `pkg/util/logutil/consistency`.
  Rationale: the nested directory has its own BUILD target and package path;
  combining it would violate the requested Go-package commit unit.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep `tidb-util::logutil` as the Rust owner and fix behavior at its
  existing abstraction boundary.
  Rationale: the owner already carries the complete Go test inventory and uses
  `tidb-log` for the shared text/file primitives; introducing a second logger
  layer would duplicate global state and weaken test evidence.
  Date/Author: 2026-08-12 / Codex

- Decision: Preserve the explicit Rust omission of gRPC replacement,
  opentracing event/tag hooks, and Go runtime trace tee.
  Rationale: these are Go ecosystem integrations with no Rust consumer or
  package unit assertion; they do not justify inventing a cross-runtime API in
  this package audit. The omission remains an explicit integration decision.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep `replace_logger` as a separate construction path instead of
  delegating to `init_logger`.
  Rationale: Go `ReplaceLogger` calls `newSlowQueryLogger` and
  `newGeneralLogger` unconditionally, while `InitLogger` reuses the global
  syncer for empty/equal filenames. The Go source and temporary probe prove
  this distinction is observable through sink identity and level.
  Date/Author: 2026-08-12 / Codex

- Decision: Do not deduplicate independently constructed dedicated sinks by
  filename.
  Rationale: `pingcap/log.InitLogger` opens a new lumberjack writer on every
  constructor call. Deduplicating by path would change rotation/flush and
  syncer identity semantics even though bytes land in the same file.
  Date/Author: 2026-08-12 / Codex

- Decision: Reproduce zap's fixed per-level FNV-1a counter buckets rather than
  retain an exact-key map.
  Rationale: the Go package documents a maximum of 4096 log types and its
  pinned zap dependency defines the collision behavior. Exact keys would grow
  without bound and diverge under hash collision.
  Date/Author: 2026-08-12 / Codex

- Decision: Treat pinned dependency behavior reached directly by package code
  as part of the package contract.
  Rationale: `proxyFields` delegates all precedence to
  `httpproxy.FromEnvironment`, and `SampleLoggerFactory` delegates admission to
  zap. Reading those pinned sources avoids inventing approximations where the
  Go package tests exercise only a subset.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

Implementation and Ready validation are complete locally. All 12 Go tests pass
normally and under the race detector; the 19-test Rust logutil slice and full
`tidb-util` crate pass. The result fixes every discovered logger/sampler gap,
keeps the Go-only integration omissions explicit, and makes no change to the
separate consistency package. Publication and remote SHA verification remain.

## Context and Orientation

The complete Go package inventory is:

    BUILD.bazel
    general_logger.go
    hex.go
    hex_test.go
    log.go
    log_test.go
    main_test.go
    slow_query_logger.go

The Go test functions are `TestHex`, `TestPrettyPrint`,
`TestFieldsFromTraceInfo`, `TestZapLoggerWithKeys`, `TestZapLoggerWithCore`,
`TestSetLevel`, `TestSlowQueryLoggerAndGeneralLoggerCreation`,
`TestSlowQueryLoggerAndGeneralUseSameLogFileName`, `TestCompressedLog`,
`TestGlobalLoggerReplace`, `TestProxyFields`, and `TestSampleLoggerFactory`.
`TestMain` installs common test setup and goleak checks.

Rust maps these to `tidb-util::logutil` and `tidb-util::logutil::hex`; the
rotating sink is local support for the `pingcap/log` behavior used by the
logger package. The Rust owner currently has all listed source-test scenarios
as module tests.

## Plan of Work

1. Add a regression proving fields attached to a slow-query logger preserve the
   `# Time` encoding, then fix `Logger::with_fields` to retain the existing
   encoding.
2. Make equal empty effective filenames share the same sink, matching the Go
   initialization contract.
3. Make sampled admission key on level and message and expose the sampled
   `debug`/`error` paths needed by Go's logger surface; add deterministic tests
   for level separation, disabled-level admission, fixed FNV buckets, and
   independent windows.
4. Keep `replace_logger` separate from `init_logger`, matching Go's
   unconditional dedicated logger construction and preserving the prior
   error-verbose logger.
5. Run the Go package suite, Rust logutil tests, Rust formatting/clippy and
   repository lint. Recheck source pin, package inventory, diff scope, and
   Bazel prerequisite decision before one normal push.

## Validation and Acceptance

No failpoint lifecycle is needed: `rg` found no `failpoint.` or
`testfailpoint.` use in the package and the BUILD target has no failpoint
dependency. `make bazel_prepare` is not triggered by Rust/Markdown-only edits;
no Go source, import, Bazel file, Go test function, or module dependency is
changed.

The minimum Ready commands are the Go package suite with
`-tags=intest,deadlock` (plus its race variant), the Rust `tidb-util` logutil
test slice and full crate suite, all-target Clippy for the touched Rust crate,
`cargo fmt --check`, the semantic package gate, and `make lint`.
The final commit must contain only this package's Rust implementation/tests and
its audit evidence, and its parent must equal `origin/hparser-integration`
before the normal push.

## Idempotence and Recovery

All source-pin and inventory checks are read-only. If the target advances,
rebase this single package commit and repeat the Ready gates. Never force-push.

## Artifacts and Notes

Initial evidence on target base `0c6d021686d78e070bb88bb98863ac0a7646e747`:

    Go: go test -count=1 -tags=intest,deadlock ./pkg/util/logutil (pass)
    Rust: cargo test -p tidb-util logutil -- --test-threads=1 (12 passed)
    Failpoint checks: no matches in source/tests/BUILD

Post-fix regression evidence (the Rust logutil slice now has 19 tests):

    slow logger fields: pre-fix unified line, post-fix `# Time:` line
    RFC3339Nano: pre-fix `.120` and `+00:00`, post-fix `.12` and `Z`
    shared level: pre-fix Info under global Error, post-fix Error threshold
    ReplaceLogger: pre-fix shared sink, post-fix independent Info-level sinks
    dedicated same filename: pre-fix merged sink, post-fix independent sinks
    sampler buckets: pre-fix both colliding messages emitted, post-fix second dropped
    proxy precedence: pinned dependency is uppercase-first; Rust now matches
    default level: the lazy stdout logger now shares the exported level control

Final Ready evidence was repeated after rebasing onto
`45abc6e52a5b95f25742bc04ff6ffe07897f5c6e`:

    go test -count=1 -tags=intest,deadlock ./pkg/util/logutil (pass)
    go test -race -count=1 -tags=intest,deadlock ./pkg/util/logutil (pass)
    cargo test -p tidb-util logutil -- --test-threads=1 (19 passed)
    cargo test -p tidb-util (360 passed, 1 ignored; integration/doctests passed)
    cargo clippy -p tidb-util --all-targets -- -D warnings (pass)
    cargo fmt --all --check (pass)
    semantic-package-gate.py logutil.semantic.toml (1 package, 3 commands)
    make -o tools/bin/revive lint (pass)
    git diff --check (pass)
    source pin and top-level package inventory checks (pass)

Plan revision note (2026-08-12): created after complete Go/Rust inventory and
clean-base test comparison.
