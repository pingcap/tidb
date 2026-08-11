# Complete and certify `pkg/util/checksum` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's checksum package frames spill-file payloads in 1,024-byte physical blocks containing a four-byte little-endian IEEE CRC-32 and up to 1,020 payload bytes. Writer and Reader layers can be nested and composed with AES-CTR; corruption must be detected before bytes are exposed. This plan audits every Go production, test, harness, and build artifact, aligns every Go test with Rust, closes source-derived arithmetic and control-flow gaps, validates the live `tidb-chunk` consumer, and publishes the result as one Go package commit.

## Progress

- [x] (2026-08-11 16:45Z) Fixed the complete four-file Go inventory at `62d4284bee67e64f23b0a7b7f89488b26667c0eb`; current package bytes match that pin.
- [x] (2026-08-11 16:46Z) Confirmed there is no `doc.go`, benchmark, fuzz target, example, fixture, testdata, build tag, platform variant, generated input, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-11 16:51Z) Read all production/test/harness/build source, listed exactly 10 Go tests, and passed the complete Go source suite.
- [x] (2026-08-11 16:53Z) Mapped all 10 Go tests to same-named Rust tests; the 11-test focused Rust baseline passes and includes flushed-offset wrapping coverage.
- [x] (2026-08-11 16:56Z) Reviewed Go and Rust consumers, shared layered-I/O contracts, historical package receipts, and current integration tests.
- [x] (2026-08-11 16:58Z) Public Go probe fixed the source `ReadAt(MaxInt64)` cursor sequence as `[-9187201950435737600, -9187201950435736576]` with 893 payload bytes returned before the injected error.
- [x] (2026-08-11 17:00Z) Added a cursor-wrap regression and observed the old Rust debug arithmetic fail with `attempt to multiply with overflow`.
- [x] (2026-08-11 17:02Z) Preserved source wrapping multiplication/addition, required corruption tests to reach EOF, and passed focused Rust plus complete `tidb-chunk` WIP validation.
- [x] (2026-08-11 17:08Z) Added the current atomic receipt and completed pre-sync Ready validation for the owning crate plus the `tidb-chunk` consumer on base `b5d2f43a17aa42d3b83dbd2cc5896797ca9ef8ed`.
- [x] (2026-08-11 17:26Z) Repeated the complete Ready profile after the first fresh fetch, then caught a concurrent remote advance before push. Rebased cleanly over the independent `pkg/util/intset` commit `52423bab5c3d1cdecca834605e3cedd2115ae161` and repeated the complete Ready profile again. Publication follows this immutable evidence snapshot by ordinary push and fresh remote-SHA verification.

## Surprises & Discoveries

- Observation: all 10 top-level Go tests have direct Rust names and equivalent plaintext/encrypted case tables.
  Evidence: checksum nesting, byte insertion/deletion/mutation, empty files, three-byte mutation, cross-block reads, batched writes, manual flush, and auto-flush appear under the same test names. Rust adds one source-derived flushed `int64` counter wrap test.

- Observation: block cursor arithmetic still differs at the upper signed offset boundary.
  Evidence: Go computes `off / 1020 * 1024` and increments the cursor using wrapping `int64` arithmetic. A public probe at `math.MaxInt64` calls its underlying reader at `-9187201950435737600`, exposes 893 bytes from the first valid block, then calls at `-9187201950435736576`. Current Rust debug arithmetic panics before the first call; release happens to wrap.

- Observation: corruption tests can pass without proving the source loop eventually reaches EOF.
  Evidence: Go loops without an artificial upper bound and stops only on `io.EOF`. Rust checks at most 32 offsets and breaks on EOF but does not assert EOF was observed. A Reader returning checksum failures forever could therefore pass the Rust corruption assertions.

- Observation: Go writer count-plus-error results cannot be represented exactly by `std::io::Write`.
  Evidence: after earlier chunks flush, Go can return `(n > 0, err)` from one Write call. Rust `io::Result<usize>` must return either a count or an error. The current writer returns the error, preserving failure and sticky-error behavior for `write_all`; this is an explicit native-trait integration difference.

- Observation: negative Go read offsets have accidental slice/cursor behavior, while Rust rejects them.
  Evidence: Go's signed remainder may produce a negative payload index and can panic or expose a checksum byte depending on the underlying reader. Rust returns `InvalidInput` before I/O. No Go test or production caller relies on negative offsets, so the safe-domain rejection remains explicit rather than reproducing invalid indexing.

- Observation: Go `Close` is repeat-callable through a pointer, while Rust Close consumes the writer.
  Evidence: consuming ownership preserves the first close cascade and prevents use-after-close. No Rust consumer needs repeated Close, and destructor timing is not used as a substitute for explicit close.

- Observation: `main_test.go` provides Go process policy, not checksum semantics.
  Evidence: it installs TiDB common test setup and four goleak exclusions. Rust creates none of those Go background goroutines; the file remains pinned with an explicit no-port decision.

- Observation: checksum has a live Rust consumer beyond its owning crate.
  Evidence: `tidb-chunk` composes checksum and encryption writers/readers for spill storage and maintains contract tests for cached tails and on-disk rows. Its complete package tests are a required validation gate.

- Observation: combined all-target Clippy reaches an unrelated pre-existing `tidb-chunk` test-only lint.
  Evidence: `cargo clippy -p tidb-util -p tidb-chunk --all-targets -- -D warnings` reports `clippy::io-other-error` at `rust/crates/tidb-chunk/src/row_in_disk.rs:523`, a line unchanged by this package. Owning `tidb-util` all-target Clippy and production `tidb-chunk --lib` Clippy both pass; the complete consumer test suite also passes. The unrelated line is not folded into this atomic checksum commit.

## Decision Log

- Decision: Use `wrapping_mul` and `wrapping_add` for the physical cursor.
  Rationale: these operations exactly preserve Go's specified signed integer overflow behavior in both debug and release Rust builds without changing normal offsets.
  Date/Author: 2026-08-11 / Codex

- Decision: Add a recording-reader regression with the two literal offsets observed from Go.
  Rationale: hard-coded source observations prevent a Rust test from masking a shared formula mistake and prove the first partial logical copy count before the injected second-read error.
  Date/Author: 2026-08-11 / Codex

- Decision: Require every corruption case to observe EOF within the existing 32-offset safety bound.
  Rationale: the bound prevents a broken test from hanging, while the final assertion restores the source loop's mandatory termination condition.
  Date/Author: 2026-08-11 / Codex

- Decision: Retain the current Rust Write and Close trait mappings and document their native ownership/error differences.
  Rationale: replacing standard traits with a second I/O API would disrupt live consumers. Source unit-test behavior and operational error propagation are already preserved.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat `62d4284bee67e64f23b0a7b7f89488b26667c0eb` as the accepted Go package pin.
  Rationale: it is the latest commit changing a direct package artifact, contains all four current files, and current bytes match exactly.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The complete inventory, exact source pin, Go normal/race suite, assertion mapping, public overflow probe, failing-then-passing regression, source arithmetic fix, corruption termination check, focused Rust suite, complete `tidb-chunk` consumer suite, receipt, and pre- and post-sync Ready validation are complete. A pre-push fresh fetch caught the remote advancing by one independent package commit, so checksum was rebased and the full Ready profile passed again on the new base. Ordinary push and fresh remote-SHA verification are the terminal publication operations performed after this evidence snapshot.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/checksum/BUILD.bazel`, `checksum.go`, `checksum_test.go`, and `main_test.go`. `checksum.go` owns constants, a pooled read buffer, Writer construction/buffering/flush/cache/close behavior, Reader construction/checksum verification, and the checksum-failure sentinel. It depends on `pkg/util/zeropool`. The Bazel target embeds one flaky short test target with encryption, common setup, testify, and goleak dependencies.

The 10 Go tests cover four nested checksum layers, encrypted and plaintext mutations, empty reads, partial EOF counts, reads spanning two to eleven blocks, one-shot versus 100-byte writes, explicit flush, and auto-flush. Support types model positional files and mutation layers. `main_test.go` supplies TestMain.

Rust owns the mapping in `rust/crates/tidb-util/src/checksum/mod.rs`, exports it through `rust/crates/tidb-util/src/lib.rs`, and shares count-plus-error and close contracts from `rust/crates/tidb-util/src/layered_io.rs`. `rust/crates/tidb-chunk/src/chunk_util.rs` is the live spill consumer. The focused checksum module has the 10 source-named tests plus flushed-offset overflow coverage.

## Milestones

The source-oracle milestone inventories and pins all four Go artifacts, lists exactly 10 tests, and runs all tests without failpoints. A public recording-reader probe measures cursor overflow not asserted by those tests. Acceptance is the passing suite and exact two-offset/893-byte observation.

The parity milestone reviews every source assertion and control-flow condition against Rust. Acceptance is 12 focused Rust tests: 10 source-named tests, flushed writer-offset wrapping, and read-cursor wrapping. Corruption cases must prove EOF termination.

The integration milestone runs the complete `tidb-chunk` suite because spill-file Reader/Writer composition is live. Acceptance includes cached-tail, encrypted/plaintext, on-disk row, and contract tests without regressions.

The publication milestone adds the current receipt and this living plan, runs the complete Ready profile, synchronizes one commit to current `hparser-integration`, pushes without force, and verifies matching local and fresh remote SHAs.

## Plan of Work

First add a recording `ReadAt` double and the cursor regression without changing production; run the exact test in debug mode and retain its panic failure. Then change only cursor multiplication and increment to wrapping operations and add the corruption-loop EOF assertion. Run focused Go/Rust WIP checks and `tidb-chunk`.

Maintain a semantic receipt with the Go pin, Rust owner/export/shared I/O evidence, and both owning-crate and consumer commands. Complete Ready validation with Go normal/race tests, the public probe, full `tidb-util`, full `tidb-chunk`, formatting, owning-crate all-target Clippy, consumer production Clippy, repository lint, and the Bazel gate decision.

## Concrete Steps

From repository root, run the Go authority and public probe:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/checksum
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestChecksumReadAt|TestAddOneByte|TestDeleteOneByte|TestModifyOneByte|TestReadEmptyFile|TestModifyThreeBytes|TestReadDifferentBlockSize|TestWriteDifferentBlockSize|TestChecksumWriter|TestChecksumWriterAutoFlush)$' -tags=intest,deadlock -count=1 ./pkg/util/checksum
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^(TestChecksumReadAt|TestAddOneByte|TestDeleteOneByte|TestModifyOneByte|TestReadEmptyFile|TestModifyThreeBytes|TestReadDifferentBlockSize|TestWriteDifferentBlockSize|TestChecksumWriter|TestChecksumWriterAutoFlush)$' -tags=intest,deadlock -count=1 ./pkg/util/checksum
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-checksum-cursor-probe.go

From `rust`, run focused and Ready Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'checksum::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-chunk
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-chunk --lib -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/checksum.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

Go must list exactly the 10 named tests, and normal plus race-enabled runs must pass. The public probe must retain `n=893`, `stop after one block`, and both literal wrapped offsets. Focused Rust must pass exactly 12 tests, including the new cursor regression, and every corruption variant must reach EOF.

The receipt must accept one pinned Go package and two unique commands. Complete `tidb-util` and `tidb-chunk` tests, formatting, owning all-target Clippy, consumer production Clippy, and repository lint must pass. The final commit may contain only the checksum owner/test, receipt, and this plan. Publication must be one linear non-force update with matching fresh remote SHA.

## Idempotence and Recovery

All checks are safe to rerun. The Go probe lives under `/tmp` and never enters the repository; move it to Trash after evidence is recorded. If remote advances, rebase the one package commit and repeat Ready validation. If a corruption test fails to reach EOF, diagnose Reader progress or fixture length rather than increasing or removing the bound.

## Artifacts and Notes

Initial Go evidence on Go 1.25.10 `darwin/arm64`:

    go test -list: exactly 10 tests
    all 10 source tests: pass
    MaxInt64 cursor probe: n=893, err=stop after one block
    MaxInt64 cursor probe offsets: [-9187201950435737600, -9187201950435736576]

Initial Rust evidence:

    checksum::tests: 11 passed, 0 failed, 0 ignored

Regression and WIP evidence:

    old debug arithmetic: read_cursor_wraps_like_source_int64_arithmetic failed at cursor multiplication overflow
    fixed debug arithmetic: the identical exact regression passed
    checksum::tests after fix: 12 passed, 0 failed, 0 ignored
    corruption cases: plaintext and encrypted scans all reached EOF within the existing bound
    tidb-chunk: 239 passed, 0 failed, 0 ignored; doctests pass
    all 10 Go source tests under -race: pass

Pre-sync Ready evidence on base `b5d2f43a17aa42d3b83dbd2cc5896797ca9ef8ed`:

    all 10 Go source tests, normal and -race: pass
    Go cursor probe: n=893 and exact two wrapped offsets unchanged
    checksum::tests: 12 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 2 unique commands
    complete tidb-util suite: 339 passed, 0 failed, 1 ignored; integration tests and doctest pass
    complete tidb-chunk suite: 239 passed, 0 failed, 0 ignored; doctests pass
    cargo fmt --all --check: pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    cargo clippy -p tidb-chunk --lib -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, target-list, or Rust manifest trigger
    not verified: tidb-chunk test-target Clippy is blocked by the pre-existing row_in_disk.rs:523 lint described above

Post-sync Ready evidence after fetching the unchanged remote base `b5d2f43a17aa42d3b83dbd2cc5896797ca9ef8ed`:

    all 10 Go source tests, normal and -race: pass
    Go cursor probe: n=893 and exact two wrapped offsets unchanged
    checksum::tests: 12 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 2 unique commands
    complete tidb-util suite: 339 passed, 0 failed, 1 ignored; integration tests and doctest pass
    complete tidb-chunk suite: 239 passed, 0 failed, 0 ignored; doctests pass
    cargo fmt --all --check: pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    cargo clippy -p tidb-chunk --lib -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, target-list, or Rust manifest trigger
    not verified: tidb-chunk test-target Clippy is blocked by the pre-existing row_in_disk.rs:523 lint described above

Post-rebase Ready evidence on remote base `52423bab5c3d1cdecca834605e3cedd2115ae161`:

    pre-push race check: remote advanced by the independent pkg/util/intset package; no checksum path overlap
    rebase: clean; checksum commit still contains exactly its three owner/receipt/plan files
    all 10 Go source tests, normal and -race: pass
    Go cursor probe: n=893 and exact two wrapped offsets unchanged
    checksum::tests: 12 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 2 unique commands
    complete tidb-util suite: 340 passed, 0 failed, 1 ignored; integration tests and doctest pass
    complete tidb-chunk suite: 239 passed, 0 failed, 0 ignored; doctests pass
    cargo fmt --all --check: pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    cargo clippy -p tidb-chunk --lib -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, target-list, or Rust manifest trigger
    not verified: tidb-chunk test-target Clippy is blocked by the pre-existing row_in_disk.rs:523 lint described above

Failpoint decision:

    no failpoint, testfailpoint, or Bazel failpoint dependency match in checksum or its encryption test dependency

## Interfaces and Dependencies

The public Rust constants, generic `Writer<W: CloseWrite>`, `Reader<R: ReadAt>`, cache accessors, underlying accessor, `std::io::Write`, and explicit consuming Close interface remain unchanged. The implementation retains `crc32fast`, the existing zero pool, layered I/O, encryption composition, and `tidb-chunk` consumers; no dependency or manifest changes are planned.

Plan revision note: created after complete source/test reads, exact package inventory and history, failpoint decision, Go list/tests, Rust focused baseline, assertion and control-flow mapping, public Go cursor probe, historical receipt review, consumer inventory, and change-instruction critique.
