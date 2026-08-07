# Lock down Go time semantics against the Rust datatype implementation

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

TiDB's `pkg/types/time.go` controls the values accepted for DATE, DATETIME,
TIMESTAMP, and TIME, their byte-visible metadata, DST behavior, and SQL-mode
error rules. A wrong answer can silently store a different temporal value or
make the same SQL differ after a timezone transition. This lockdown makes the
complete Go source file an explicit Rust claim: every production declaration,
branch rule, and source-owned test/support declaration is classified beside the
Rust landing code, and any Go-source or PORTED-symbol drift fails locally.

The source has 151 functions. It is one atomic Go file even though the natural
Rust implementation is distributed across `mysql_time.rs`, `time_parse.rs`,
`duration.rs`, `str_to_date.rs`, and `core_time.rs`. Splitting the claim by
Rust file would leave Go rules unowned, so this unit owns that one source file
and lists every Rust landing symbol explicitly.

## Progress

- [x] (2026-08-06) Chose the first ranked temporal gap after confirming that
  `tidb-expr` and `tidb-executor` are owned and `tidb-datatype` is free.
- [x] (2026-08-06) Created the dedicated worktree at campaign tip
  `32d0096e93a1b530ad34b093045c2febe83220c5` with exclusive target directory
  `/private/tmp/cargo-target-task325-time-go-lockdown`.
- [x] (2026-08-06) Counted 151 `pkg/types/time.go` production declarations and
  attributed 75 source-adjacent test/support declarations: 60 in
  `time_test.go`, 13 in `core_time_test.go`, and two direct consumers in
  `format_test.go`.
- [x] (2026-08-06) Read the existing source audit and selected its boundary
  probes as hypotheses, not authority.
- [x] (2026-08-06) Ran a disposable Go probe for malformed durations,
  fractional precision, DST parsing, and negative-half duration rounding.
- [x] (2026-08-06) Captured and fixed the first measured divergence: negative
  exact-half `Duration.RoundFrac` now follows Go's toward-positive-infinity tie
  rule instead of Rust's former away-from-zero result.
- [x] (2026-08-06) Audited the contiguous Go owner prefix through line 951:
  42 of 151 declarations and 78 control/representation rules now have explicit
  verdicts, running boundary-test references, and PORTED compile anchors.
- [x] (2026-08-06) Extended the contiguous audit through Go line 2247: 102 of
  151 declarations and 286 rules are classified. Full `tidb-datatype --lib`
  validation passes 285 tests in the exclusive target directory.
- [x] (2026-08-06) Re-read and classified all 151 Go declarations and all 561
  syntactic control-flow loci; source drift, exact range coverage, verdict,
  and PORTED-symbol gates pass.
- [x] (2026-08-06) Ran Go boundary probes, captured fail-before behavior, and fixed all real
  mismatches at shared parsing, validation, or representation layers.
- [x] (2026-08-06) Ported every source-owned Go test/support artifact or recorded a concrete
  DECLINED/UNREACHABLE verdict in the inventory.
- [x] (2026-08-06) Killed 20 independent mutations spanning duration rounding,
  SQL microsecond bounds, zero-date diagnostics, YEAR width, raw-byte spaces,
  DST value/diagnostic handling, interval overflow, signed formatting,
  exhausted STR_TO_DATE tokens, Unicode categories, receiver mutation,
  calendar-vs-instant duration conversion, source drift, branch deletion,
  PORTED-symbol disappearance, Go-test receipt disappearance, DST diagnostic
  conversion, parser-warning separation, write-path event preservation, and
  temporal error identity.
- [x] (2026-08-06) Ran repository Ready lint and the three touched-crate test
  suites. A first clean-workspace gate exposed and fixed the dropped DST
  diagnostic at the datatype-to-write boundary.
- [ ] Re-gate the exact final SHA in a clean worktree, verify ratchet constants
  by direct grep, return the SHA for the campaign gate, then dual-push and
  reclaim after approval.

## Surprises & Discoveries

- Observation: the ranked `TestTimeOverflow` and `TestCheckTimestamp` surface
  is not an isolated pair of helpers.
  Evidence: both call rules inside the 3,554-line `pkg/types/time.go`, which
  also owns parsing, SQL-mode validation, duration arithmetic, formatting, and
  `STR_TO_DATE`; a narrow test-only port would not satisfy the source-file
  claim.

- Observation: the current Rust tree has no checked-in `time.go` lockdown
  inventory.
  Evidence: no `time` inventory or lockdown module exists under
  `rust/crates/tidb-datatype/src`; existing tests cite individual Go functions
  without gating the complete source declaration set.

- Observation: the prior temporal audit is partially stale in both directions.
  Evidence: current Go and Rust already agree on short trailing duration input,
  invalid minute/second NULL behavior, a bare fractional dot, and DST carry.
  The same direct Go probe confirmed a real negative-half rounding mismatch:
  `-00:00:00.0015` at FSP 3 becomes `-00:00:00.001`, while pre-fix Rust
  produced `-00:00:00.002`.

- Observation: a workspace-wide `cargo fmt --check` is not a valid unit gate
  at the campaign tip.
  Evidence: it reports pre-existing formatting deltas only in owner-external
  `tidb-executor`, `tidb-expr`, and `tidb-session` files. `rustfmt --check` on
  the two edited `tidb-datatype` time files passes.

- Observation: direct invalid-FSP calls to Go `NewTime` can manufacture
  corrupt or aliased packed states rather than returning an error.
  Evidence: the disposable source probe measured `fsp=7` as `TypeDate`/FSP 0,
  `fsp=8` as DATETIME/FSP 0 with a changed core bit, and `fsp=-2` as raw
  `18446744073709551612`. Rust keeps its checked typed constructor; this exact
  malformed-input rule is DECLINED rather than mislabeled PORTED.

- Observation: `MySqlDuration::convert_to_time` used instant arithmetic where
  Go assigns calendar clock fields for positive values below 24 hours.
  Evidence: on the 2011-03-13 Los Angeles spring-forward day, pre-fix Rust
  returned `04:00:00` for TIME `03:00:00`; `CoreTime::mix_duration` now returns
  the source value `03:00:00`.

- Observation: Go duration parsing recognizes raw bytes `0x85` and `0xA0` as
  internal spaces through `unicode.IsSpace(rune(byte))`.
  Evidence: pre-fix Rust parsed `1\x85:\x852` as compact one second; after the
  source-space fix it parses `01:02:00`. Valid UTF-8 outer whitespace follows
  Unicode `strings.TrimSpace` semantics separately.

- Observation: Go windows only one- and two-character YEAR inputs.
  Evidence: the `"0000"` regression failed before the fix because Rust returned
  year 2000; four-character forms now range-check the raw value and reject it.

- Observation: Go returns an adjusted TIMESTAMP value beside its DST diagnostic.
  Evidence: `2018-03-11 02:00:16` in Los Angeles failed pre-fix with
  `NonexistentLocalTime`; it now returns `2018-03-11 03:00:00` with
  `ParsedTime::dst_adjusted == true`.

- Observation: preserving the diagnostic in `ParsedTime` was necessary but not
  sufficient; two downstream conversion seams discarded it.
  Evidence: the first clean-workspace run failed the existing DDL and INSERT
  Los Angeles spring-gap cases. `Datum::convert_to_in` discarded
  `dst_adjusted`, and `cast_value_shaped` returned a temporal value before
  consuming its conversion event. After both repairs, DDL reports 1067 and
  INSERT reports 1292 while the adjacent valid timestamps still pass. Three
  independent mutations respectively restored silent acceptance, dropped the
  write event, and changed 1292 into 1366; all were killed.

- Observation: `ParsedTime::truncated` and `ParsedTime::dst_adjusted` represent
  different Go channels and cannot share one conversion event.
  Evidence: ordinary trailing input is appended to Go's parser context as a
  warning while `parseTime` still returns nil error; DST transition returns
  `ErrTimestampInDSTTransition` beside the adjusted value. Combining them
  changed the catalog fingerprint from `6068344160096210003` to
  `17951428125835141995` inside `ddl/column` by dropping a default instead of
  preserving the existing result. Propagating only `dst_adjusted` restores the
  exact catalog ratchet while retaining DDL 1067 and INSERT 1292 for the gap.

- Observation: a literal one-process workspace run is order-sensitive because
  the existing charset tests mutate process-global collation mode.
  Evidence: `charset::tests::source_registry_vectors` observed `gbk_bin` after
  another test installed `gbk_chinese_ci`; the resulting poisoned lock caused
  two additional collation failures. The final clean gate therefore runs the
  four interacting tests in isolated processes and all remaining workspace
  tests together. This partition changes process isolation, not test coverage.

- Observation: Go's general integer-width formatter pads the already-stringified
  signed value rather than formatting sign-aware digits.
  Evidence: the new regression failed with Rust `-001` versus Go `00-1` for
  `FormatIntWidthN(-1, 4)`; the shared helper now follows the source algorithm.

- Observation: exhausted STR_TO_DATE input is processed after the next format
  token is decoded, and the decoded `%p`, `%h`, or `%H` token still affects the
  final meridiem fix.
  Evidence: pre-fix Rust accepted empty input with a dangling `%` and lost the
  empty-token context. Boundary tests now distinguish reset, untouched, parsed
  failure, warning, and success receiver states.

- Observation: STR_TO_DATE skip tokens use Go Unicode Number, Punctuation, and
  Letter categories, not ASCII punctuation or Rust's broader Alphabetic
  property.
  Evidence: pre-fix Rust rejected an em dash under `%.`; category-exact regex
  predicates now pass punctuation, Roman-number, and combining-mark boundaries.

## Decision Log

- Decision: lock down `pkg/types/time.go` as one Go-source unit despite its
  multiple Rust landing modules.
  Rationale: Go's source file is the completion boundary. The one-owner rule is
  preserved because this unit exclusively owns `tidb-datatype`; forcing a
  one-Rust-file claim would hide source omissions.
  Date/Author: 2026-08-06 / Codex

- Decision: treat the prior divergence audit as a probe queue, not as proof.
  Rationale: its own text says much of it was not re-derived and this campaign
  has repeatedly falsified stale gap reports. Every listed difference must be
  re-measured from Go before it is fixed, declined, or called unreachable.
  Date/Author: 2026-08-06 / Codex

- Decision: implement rounding using Euclidean division after the half-unit
  offset.
  Rationale: ordinary negative values must still round to the nearest lower
  unit, while an exact negative half must move toward positive infinity. The
  prior signed `value - half` formula conflated the two cases.
  Date/Author: 2026-08-06 / Codex

## Outcomes & Retrospective

No completion claim yet. The final receipt will record the complete source and
test counts, source hash, every classification, direct Go-probe output,
fail-before evidence, mutation results, Ready and clean-worktree commands,
remote SHAs, and reclaimed disk space.

The production and original-test completeness boundary is now closed: 151
functions, 561 exact control-flow loci, and 75 test/support declarations have
one nonempty verdict. The touched-crate suites pass with 294 `tidb-datatype`,
507 `tidb-executor`, and 989 `tidb-session` tests; all 20 deliberate mutations
were observable. No oracle ratchet moved; lockdown completeness is the
deliverable and is a successful result. This is not yet a final completion
claim because the exact final SHA still needs the clean-worktree partition,
direct ratchet grep, and returned receipt.

## Context and Orientation

The worktree is `/private/tmp/codex-task325-time-go-lockdown`, branch
`codex/task325-time-go-lockdown`, based on the validated campaign tip above.
The main checkout is divergent and must never be read. Run Cargo only with
`CARGO_BUILD_JOBS=12` and the exclusive target directory named above.

The authoritative production source is `pkg/types/time.go`. It imports
`core_time.go` types but contains its own temporal parser and SQL-mode rules.
The ranked Go tests live in `pkg/types/time_test.go`; `core_time_test.go` is
also source-owned where it exercises declarations defined in `time.go`.
`mysql_time.rs` holds the central `Time` representation, while `time_parse.rs`,
`duration.rs`, `str_to_date.rs`, and `core_time.rs` hold supporting rules.

In this plan, a lockdown inventory is a test-only Rust module that records each
Go declaration and branch as PORTED (with the exact Rust symbol), DECLINED
(with a Go quotation or direct Go probe), or UNREACHABLE (with a type or call
path proof). It must reject unclassified items, source hash drift, declaration
drift, and disappearance of a PORTED symbol.

## Plan of Work

First enumerate the Go declarations mechanically, then read their bodies in
coherent parser, representation, duration, validation, extraction, and format
sections. Map each to the actual Rust symbol, not a similarly named function.
Read both Go test files row-by-row and attribute every declaration to the
source owner or a neighboring Go source, so the inventory cannot claim tests it
does not own.

Second, build disposable Go probes outside the repository. Probe SQL mode,
zero dates, invalid calendar fields, prefix and trailing-input parsing, bare
fractions, signed half rounding, DST gaps, numeric zero, packed encoding, and
format-token exhaustion. A probe carries boundary inputs that distinguish the
rule from a recorded expected string.

Third, add `time_go_inventory.rs` and register it test-only from
`tidb-datatype/src/lib.rs`. Its source-hash/declaration gate covers the full
owner. The test table has no TODO, blank reason, or unreferenced PORTED symbol.
Where an existing private helper is the right anchor, add a test-only anchor
rather than exposing a new production API.

Fourth, fix measured divergences in the lowest shared layer. Parser differences
belong in duration/time parsing; DST and calendar admission belong in temporal
conversion/validation; formatting differences belong in the common renderer.
Do not emulate Go from SQL callers or add per-test exceptions.

Finally, mutation-probe each parser, rounding, timezone, validation, duration,
formatting, and inventory branch. Commit only after the full claim is
classified, then gate the resulting SHA in a fresh clean worktree, push both
remotes, verify the same SHA, and reclaim only this unit's artifacts.

## Concrete Steps

All commands run from `/private/tmp/codex-task325-time-go-lockdown` unless a
different directory is named.

    rg -n '^func |^func \(' pkg/types/time.go
    rg -n '^func Test|^func Benchmark|^func Fuzz' pkg/types/time_test.go pkg/types/core_time_test.go
    shasum -a 256 pkg/types/time.go
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-time-go-lockdown cargo test -p tidb-datatype --lib

For final validation from `rust/`, use the same exclusive target for scoped
checks and a new target in a clean detached worktree for the full workspace:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<clean-target> cargo test --workspace
    make -j12 lint

## Validation and Acceptance

Acceptance requires every one of the 151 Go declarations and every discovered
branch/test-support row to have exactly one nonempty verdict. Mutating the Go
source, deleting a PORTED symbol, or deleting a branch row must fail a running
gate. The exact Go boundary probes must fail against the pre-fix Rust behavior
and pass after shared-layer fixes. The final SHA needs a clean status, direct
source-hash/count grep, clean-worktree workspace validation, lint, and matching
`origin` and `ngaut` refs.

## Idempotence and Recovery

Source reads, declaration scans, probes, and gates are safe to repeat. All
probes and mutations live in exact disposable paths under `/private/tmp`; if a
mutation cannot be cleanly reversed, discard that worktree instead of restoring
unknown content. Never read the divergent main checkout. Do not delete a
target or worktree until its final SHA is present on both remotes and the exact
path has been measured.

## Artifacts and Notes

Pinned evidence: `pkg/types/time.go` is 3,554 lines and 109,954 bytes at this
tip. The final attributed test/support surface is 75 declarations across
`time_test.go`, `core_time_test.go`, and `format_test.go`.

## Interfaces and Dependencies

No new production dependency is expected. The inventory will use the existing
test-only SHA-256 dependency and `include_str!` to read the Go owner. It will
compile-anchor the actual public and test-only Rust interfaces used by the
claim, including `Time`, `Duration`, `parse_time`, `parse_duration`, date/time
validation, timestamp conversion, and `str_to_date` helpers.

Revision note: initial plan written 2026-08-06 after ownership and source-size
audit.

Progress 2026-08-06: direct disposable Go probes falsified several stale audit
hypotheses: malformed duration suffix handling and DST fractional carry already
match the current Go owner. They also found two real shared-layer mismatches.
`Duration.RoundFrac` rounded the exact negative half away from zero, while Go
`time.Time.Round` rounds `-1_500_000ns` to `-1_000_000ns`; the focused Rust
test failed before the `div_euclid` formulation and passed afterward. Go
`checkDateRange` compares the entire `CoreTime` to `MaxDatetime`, whereas the
Rust packed representation admitted microsecond `1_000_000`; a boundary test
for `9999-12-31 23:59:59.1000000` failed before validation rejected that
out-of-SQL-range field and passed afterward. The broad workspace fmt check is
currently blocked only by pre-existing formatting changes in the other active
crate owners; this unit runs file-scoped rustfmt checks instead. Numeric zero
parsing was re-probed. Go returns a zero value and a typed truncation error
when `FlagIgnoreZeroDateErr` is clear. The retained solution keeps the old
default-statement parser entry point stable, adds a flags-aware datatype entry
point that carries the returned zero plus `ParsedTime::truncated`, and makes
`Datum::convert_to` emit `ScalarConversionEvent::Truncated` from that signal.
This captures `time.go`'s value-plus-diagnostic boundary without touching the
concurrently owned expression or executor crates. Its final statement-level
error naming still belongs to those owner units and must be verified only
after their worktrees are released.

Mutation evidence: replacing the flags-aware numeric-zero `truncated` signal
with `false` made `numeric_zero_time_conversion_keeps_go_value_and_diagnostic`
fail with `left: None`, `right: Some(Truncated)`. The signal was restored
before further validation.

Inventory progress: `tidb-datatype/src/time_go_inventory.rs` now gates the
exact source SHA-256, 3,554-line count, and ordered receiver-qualified list of
all 151 `time.go` declarations. The audited prefix through Go line 2247 contains
102 exact declaration rows and 286 control/representation rows. Its module-level
WIP marker deliberately prevents mistaking that partial verdict inventory for
the final claim: the remaining 49 declarations, starting with `ExtractDatetimeNum`,
and their branches still require classification.

The invalid-FSP probe used a temporary `pkg/types` test only after the package
failpoint scans returned no matches. The exact command was
`go test -run '^TestTimeLockdownProbeNewTimeInvalidFSP$' -tags=intest,deadlock -count=1 -v`;
the temporary file was deleted immediately after the measurement. Targeted
Rust gates currently pass seven `time_owner_slice_*` boundary tests and three
`time_go_inventory::*` drift/classification/symbol tests in the exclusive
target directory.

Four later fail-before/pass-after loops closed YEAR `"0000"`, DST calendar
mixing, Latin-1 parser spacing, and adjusted TIMESTAMP value preservation. The
exact full scoped command
`CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-time-go-lockdown cargo test -p tidb-datatype --lib -- --nocapture`
passes all 285 library tests at this checkpoint.

The full source `TestParseTimeFromNum` three-type matrix was added as a direct
Rust test and passes with no production change. It proves that the existing
numeric parser preserves the source's independent DATETIME values, TIMESTAMP
UTC-range errors, and DATE clock-discarding behavior; this is completeness
evidence, not a ratchet movement.

Test-support attribution baseline: `time_test.go` contains 57 Test/Benchmark/
Fuzz declarations and adjacent `core_time_test.go` contains 13. The final
inventory must classify all 70 explicitly; a declaration owned by
`core_time.go` is a DECLINED adjacent-support row, never a silently omitted
test.
