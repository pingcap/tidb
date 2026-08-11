# Complete and certify `pkg/util/format` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's util formatter turns nested diagnostic text into indented or flattened output and escapes strings printed inside SQL literals. A complete Rust package must preserve the current Go package's formatter state, structural indent commands, ordinary formatted values, byte counts, writer failures, and exact `OutputFormat` replacements. This plan inventories the entire Go package, proves the native Rust typed-fragment boundary against public Go observations, validates the live default-value printer, and publishes all evidence as one Go-package commit.

## Progress

- [x] (2026-08-12) Fixed the complete four-file Go inventory and accepted source pin `59dfa4d3b214ded26f957249efbda21f95149bb5`; current package bytes match the pin.
- [x] (2026-08-12) Confirmed there is no `doc.go`, generated input, fixture, testdata, build/platform variant, benchmark, fuzz target, example, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-12) Read every Go source/test/build artifact, the Rust util facade, the datatype formatter owner and tests, the direct executor consumer, and formatter port history.
- [x] (2026-08-12) Passed Go normal/race baselines, the seven Rust owner tests, the original public contract, and a focused literal-default consumer test; recorded the public Go behavior probe.
- [x] (2026-08-12) Resolved every representable source behavior and documented the three native Rust type/API boundaries.
- [x] (2026-08-12) Added two public writer regressions, strengthened shared-owner state evidence, added direct consumer escape evidence, and created the four-command semantic receipt.
- [x] (2026-08-12) Completed Ready validation: Go authority/probe, focused and full Rust tests, fmt, four Clippy surfaces, semantic gate, repository lint, and atomic diff checks all pass.
- [ ] Publish the synchronized package commit (completed: explicitly fetched actual remote tip, rebased only this package, and repeated Ready; remaining: non-force push and fresh-remote SHA verification).

## Surprises & Discoveries

- Observation: the util package does not own a second formatter state machine in Rust.
  Evidence: `rust/crates/tidb-util/src/format.rs` re-exports `tidb_datatype::{FlatFormatter, FormatFragment, Formatter, IndentFormatter}` because Go `pkg/util/format` and `pkg/parser/format` share the same state machine; util owns only its extra backslash replacement.

- Observation: the formatter's Rust API uses typed fragments rather than parsing Go printf strings at runtime.
  Evidence: `FormatFragment::{Text,TextBytes,Value,ValueBytes,Indent,Unindent}` separates structural commands from opaque formatted values. The audit must prove that this native boundary preserves Go-observable state and output rather than compare the two function signatures literally.

- Observation: the source package entered this branch at `59dfa4d3b214ded26f957249efbda21f95149bb5` and has not changed afterward.
  Evidence: that commit adds all four package artifacts, and `git diff --exit-code 59dfa4d3b214ded26f957249efbda21f95149bb5..HEAD -- pkg/util/format` is empty.

- Observation: the shared Rust owner retried short writes and skipped empty writes, unlike Go's single `fmt.Fprintf` writer call.
  Evidence: the Go probe reported `short-write n=3 err=<nil> calls=1 output="abc"` and `empty-write-error n=0 err=empty write failed calls=1`. Against the old `write_all` implementation, the new public regressions failed with returned count 6 instead of 3 and `Ok(0)` instead of the empty writer's error.

- Observation: Go advances formatter state before calling the writer, even when that writer fails, while a direct `Write` bypasses formatter state.
  Evidence: after a failed `Format("a%i\n")`, the probe's next `Format("b\n")` wrote `"  b\n"`; after command-only `Format("%i")` and direct `Write("raw\n")`, the next formatted line produced `"raw\n  tail\n"`. The shared owner test now fixes both behaviors.

- Observation: three Go inputs have no literal Rust API equivalent, but none weakens a live consumer.
  Evidence: malformed runtime printf templates such as trailing `%` are replaced by typed fragments; invalid UTF-8 accepted by Go strings is excluded by `&str` for `output_format` while raw formatter bytes remain available through `TextBytes`/`ValueBytes`; and Rust `Write::write` cannot return a partial count together with an error. The live formatter consumers use valid typed fragments and ordinary writers, and the live util escape consumer receives valid Rust strings.

- Observation: `tidb-datatype` aggregates standalone integration sources into the Cargo test target named `all`.
  Evidence: a direct `--test parser_format_package_source` baseline was rejected because no such target exists; `cargo test -p tidb-datatype --test all 'parser_format_package_source::'` runs exactly the eight formatter tests and is the receipt command.

- Observation: this clone's `origin` fetch refspec tracks only `master`, so `git fetch origin hparser-integration` updated `FETCH_HEAD` but left `origin/hparser-integration` stale at `9e065257f13f9425ebccf4ff8a535a566b64ac1a`.
  Evidence: a normal push based on that stale tracking ref was safely rejected without changing the remote. `git ls-remote origin refs/heads/hparser-integration` and an explicit `refs/heads/hparser-integration:refs/remotes/origin/hparser-integration` fetch resolved tip `a53757da3b1513df2e3dcb674b970db710977515`; a later explicit pre-push fetch advanced it once more to `ecbe19475bb43f940536e8ff285c148911e47d4d`, adding only the unrelated random-bytes package commit.

## Decision Log

- Decision: Use the branch source-introduction commit `59dfa4d3b214ded26f957249efbda21f95149bb5` as the package source pin.
  Rationale: it is a complete package snapshot on the target branch and is byte-identical to the current Go package.
  Date/Author: 2026-08-12 / Codex

- Decision: Treat the typed fragment representation as the native Rust formatting boundary, subject to behavioral probes.
  Rationale: Rust's compile-time formatting surface cannot accept Go's runtime variadic printf string directly. Typed text, value, and command fragments prevent user values containing `%i`, `%u`, or newlines from being reinterpreted while retaining native width, precision, indexing, radix, `Display`, and `Debug` behavior.
  Date/Author: 2026-08-12 / Codex

- Decision: Replace the owner's `write_all` with exactly one `Write::write` call.
  Rationale: this matches Go's observable call count, short-write count, zero-write behavior, and empty-write error propagation while preserving the existing Rust `io::Result<usize>` interface.
  Date/Author: 2026-08-12 / Codex

- Decision: Do not emulate malformed Go printf templates or accept invalid UTF-8 in the util escape API.
  Rationale: typed fragments make structural commands unambiguous and `&str` makes Rust text valid by construction. Raw formatter bytes remain supported where byte preservation matters, while adding a second runtime printf parser or a byte-returning util escape API has no source-test or live-consumer justification.
  Date/Author: 2026-08-12 / Codex

- Decision: Use an explicit hparser refspec and `ls-remote` for publication checks, then rebase only the new `pkg/util/format` commit onto the actual remote tip.
  Rationale: a named fetch alone is insufficient with this clone's master-only refspec. The final package commit's direct parent is explicitly fetched remote SHA `ecbe19475bb43f940536e8ff285c148911e47d4d`; the earlier modify/delete receipt conflict was resolved by adding this audit's complete source pin and four verified commands, because the later remote history had intentionally removed the old receipt.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

The complete inventory, source pin, source/build/test reads, formatter-owner review, history review, direct-consumer mapping, normal/race baselines, public probe, failing writer-regression evidence, minimal owner fix, boundary decisions, public contract, shared-owner state coverage, direct consumer test, compact receipt, pre-sync Ready validation, one-package commit, explicit remote correction, actual-tip rebase, final post-sync Ready validation, and atomic diff checks are complete. Only the non-force push and fresh-remote verification remain.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/format/BUILD.bazel`, `format.go`, `format_test.go`, and `main_test.go`. `IndentFormatter` and `FlatFormatter` are stateful `io.Writer` wrappers. Their `Format` method recognizes `%i` and `%u` in the format template as structural indent and unindent commands, lets ordinary verbs flow to `fmt.Fprintf`, and retains line and indent state across calls. `OutputFormat` replaces NUL, single quote, line feed, carriage return, and backslash. The only top-level source test is `TestFormat`; `TestMain` configures TiDB's common test process and leak checker.

Rust exposes the util package through `rust/crates/tidb-util/src/format.rs`. The shared formatter implementation is `rust/crates/tidb-datatype/src/format.rs`, with source-oriented owner tests in `rust/crates/tidb-datatype/tests/parser_format_package_source.rs`. `rust/crates/tidb-util/tests/format_contract.rs` maps the original util test through the public facade. The live consumer is `rust/crates/tidb-executor/src/column_default.rs`, where literal defaults use util `output_format` for `SHOW CREATE TABLE` text; its nearest tests are in `rust/crates/tidb-executor/src/driver/tests/column_defaults.rs`.

## Milestones

The source-oracle milestone passes `TestFormat` normally and under the race detector and records a public Go probe for cross-call state, beginning-of-line commands, flat mode at zero and nonzero depth, empty/trailing templates, opaque values, byte counts, writer failures and short writes, UTF-8, invalid UTF-8 where relevant, negative indentation, and all escape replacements.

The parity milestone adds only source-backed public Rust assertions missing from existing owner coverage. Any production edit must first have a regression that fails against the old Rust implementation while the Go probe establishes the expected behavior.

The integration milestone validates the literal-default consumer with quotes, backslashes, NUL, line feed, and carriage return, so the util-only backslash behavior is exercised through a real call path. The focused executor test now fixes the exact clause `"'slash\\\\quote''\\0nul\\nline\\rcarriage'"`.

The publication milestone adds the compact semantic receipt, runs the complete Ready profile, synchronizes a single package commit to current `hparser-integration`, pushes without force, and verifies matching local and freshly fetched remote SHAs.

## Plan of Work

Run the Go source test list, targeted test, race test, and a temporary public probe outside the repository. Compare every representable row with the shared Rust owner and util facade. Extend `rust/crates/tidb-util/tests/format_contract.rs` for the source writer boundary, correct the shared owner's single-write behavior, add direct literal-default consumer coverage, and add `rust/crates/tidb-util/tests/format.semantic.toml` with the source pin, complete Rust ownership, public contract, and live consumer evidence. Do not edit Go, Bazel, Cargo manifests, or unrelated Rust modules.

## Concrete Steps

From repository root, run the Go authority and public probe:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/format
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^TestFormat$' -tags=intest,deadlock -count=1 ./pkg/util/format
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^TestFormat$' -tags=intest,deadlock -count=1 ./pkg/util/format
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-format-probe.go

From `rust`, run owner, public, and consumer gates with `CARGO_INCREMENTAL=0` and the shared `CARGO_TARGET_DIR`:

    cargo test --offline --locked -j12 -p tidb-datatype --test all 'parser_format_package_source::'
    cargo test --offline --locked -j12 -p tidb-util --test format_contract
    cargo test --offline --locked -j12 -p tidb-executor --lib column_default::tests::literal_show_create_clause_uses_util_output_format -- --exact
    cargo test --offline --locked -j12 -p tidb-session --lib tests_column_defaults::a_folded_default_stays_a_settled_literal -- --exact
    cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    cargo clippy --offline --locked -j12 --no-deps -p tidb-datatype --all-targets -- -D warnings
    cargo clippy --offline --locked -j12 --no-deps -p tidb-executor --lib -- -D warnings
    cargo clippy --offline --locked -j12 --no-deps -p tidb-session --lib -- -D warnings

From repository root, validate the receipt, lint, and atomic diff:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/format.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint
    git diff --check

## Validation and Acceptance

Go must list exactly `TestFormat`, and that test must pass normally and under `-race`. Every public probe row must be accounted for either by an exact Rust assertion or by an explicit native type/API boundary with live-consumer justification. Rust must pass the complete formatter owner target, the util public contract, focused executor consumer coverage, the full owning `tidb-util` crate, formatting, owner/direct-consumer Clippy, the compact semantic receipt, repository lint, and `git diff --check`. Publication is accepted only after a normal push and a fresh fetch show the same SHA at `origin/hparser-integration`.

## Idempotence and Recovery

All tests and checks are safe to rerun. The Go probe lives only under `/tmp` and must be moved to Trash after its evidence is recorded. Cargo uses the explicit shared target with incremental compilation disabled; do not clean that shared target. If the remote advances, rebase the one package commit and repeat Ready before pushing. Never force push.

## Artifacts and Notes

Initial Go evidence on Go 1.25.10 `darwin/arm64`:

    test list: exactly TestFormat; source test passes normally and under -race
    short writer: n=3, nil error, one call, output "abc"
    zero writer: n=0, nil error, one call; empty failing writer: n=0, propagated error, one call
    writer error advances formatter state before the next call
    cross-call output: "top\n  child\n  next\ntail\n"
    nested beginning-of-line commands: "\n    x\n\n"; negative flat depth: " x \ny\n"
    typed values remain opaque to `%i`, `%u`, and embedded newlines
    util escape output: "plain\\\\slash''quote\\0nul\\nline\\rcarriage\t雪"

Old-Rust failing regression evidence:

    short-writer contract: returned 6 instead of Go's 3
    empty failing writer contract: returned Ok(0) instead of propagating the writer error

Ready evidence before remote synchronization:

    Go test list: exactly TestFormat
    Go TestFormat: pass normally and under -race
    Go public probe: every recorded formatter, writer, byte, and escape row remains exact
    shared datatype formatter owner: 8 passed
    util public contract: 3 passed
    executor direct escape consumer: 1 passed
    session end-to-end literal-default consumer: 1 passed
    full tidb-util: 344 passed, 1 existing subprocess helper ignored; all integration targets and doctest passed
    cargo fmt --all --check: pass
    tidb-util all-target Clippy with -D warnings: pass
    tidb-datatype all-target --no-deps Clippy with -D warnings: pass
    tidb-executor and tidb-session library --no-deps Clippy with -D warnings: pass
    semantic package gate: 1 package, 4 unique commands
    repository make lint with revive 1.2.1: pass
    git diff --check: pass

Final post-sync Ready evidence on actual remote base `ecbe19475bb43f940536e8ff285c148911e47d4d`:

    Go list, targeted TestFormat, -race TestFormat, and complete public probe: pass with exact outputs
    shared datatype formatter owner: 8 passed
    util public contract: 3 passed
    executor direct escape consumer and session end-to-end literal-default consumer: 1 passed each
    full tidb-util: 344 passed, 1 existing subprocess helper ignored; all integration targets and doctest passed
    cargo fmt --all --check: pass
    tidb-util and tidb-datatype all-target Clippy plus executor/session library Clippy with -D warnings: pass
    compact semantic package gate: 1 package, 4 unique commands
    repository make lint with revive 1.2.1: pass
    the rebased package commit has the explicitly fetched actual remote SHA as its direct parent

Failpoint decision:

    No package file contains failpoint imports, calls, or Bazel failpoint dependencies; use ordinary targeted Go tests.

Build metadata decision:

    make bazel_prepare is not required: the final diff changes Rust production/test files, one semantic receipt, and this plan, with no Go/Bazel/module/manifest edit, Go import change, or new Go test.

## Interfaces and Dependencies

The public util facade remains `output_format(&str) -> String` plus the re-exported `Formatter::format(&mut self, &[FormatFragment]) -> io::Result<usize>`, `IndentFormatter`, and `FlatFormatter`. `format` now performs exactly one underlying `Write::write`, including for an empty rendered buffer. The owner uses only `std::fmt` and `std::io::Write`. The live consumer remains `ColumnDefault::show_create_clause`; no new dependency or manifest change is added.

Plan revision note: created after the complete inventory and initial mapping; updated after the source probe, failing regressions, minimal fix, boundary decisions, consumer evidence, semantic receipt, and pre-sync Ready profile.
