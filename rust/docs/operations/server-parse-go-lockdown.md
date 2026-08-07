# Lock down the server parse source against Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to
that contract.

## Purpose / Big Picture

MySQL clients send connection responses and cursor-fetch commands before or
during normal SQL execution. A parser difference can accept a malformed
pre-auth packet, lose connection attributes, choose the wrong authentication
identity, or let a client request an unbounded cursor batch. After this work,
Rust will reproduce every observable rule in
`pkg/server/internal/parse/parse.go`, including COM_STMT_FETCH capping,
handshake header/body mutation, length-encoded fields, attribute truncation and
metrics, warning decisions, malformed-packet handling, and zstd negotiation.

The checked-in inventory beside the Rust implementation will classify every
Go function, both outcomes of every syntactic control locus, and every original
source-owned test/support declaration. It will fail on source drift, missing
classification, duplicate keys, empty evidence, or disappearance of a PORTED
Rust symbol. This is a source-file lockdown only; it does not claim the whole
`pkg/server` package is transcreated.

## Progress

- [x] (2026-08-07) Verified `163559e78020c57547e437089be1f28c3552f7a9`
  as the accepted dual-remote campaign tip and created branch
  `codex/task325-parse-go-lockdown` in an isolated worktree.
- [x] (2026-08-07) Published the environment-independent collaboration
  contract and visible ownership claim to both remotes.
- [x] (2026-08-07) Resolved the complete Go owner: 315 lines, 9,322 bytes,
  SHA-256 `f99c3f11f808ab3d477f5edcdaf7174b5f408972ffe29cb58fdf3b4a341394fe`,
  eight functions, and 41 syntactic control loci / 82 outcomes.
- [x] (2026-08-07) Determined failpoints are not used by
  `pkg/server/internal/parse`; targeted Go tests run without failpoint
  enablement.
- [x] (2026-08-07) Captured the first fail-before/pass-after divergence:
  Rust returned fetch count 1025 while Go caps 1025 and `u32::MAX` to 1024.
- [x] (2026-08-07) Probed handshake/body/attribute-policy boundaries directly
  against Go with a disposable overlay; invalid wire strings, NULL and special
  auth markers, malformed decoded attributes, and truncation metrics matched
  the measured oracle.
- [x] (2026-08-07) Ported all reachable rules and checked in an inventory for
  eight functions, 82 outcomes, 11 attributed declarations, and both owning
  Bazel support artifacts, with drift, receipt, and compile-symbol gates.
- [x] (2026-08-07) Killed 33 independent mutations spanning fetch framing,
  endian and cap; header boundary and offsets; auth modes and every lenenc
  width; optional database/plugin/attrs/zstd fields; raw bytes; policy,
  warning, duplicate, reserved-key, metric, and concurrency rules; source
  drift; and PORTED-symbol disappearance. One initial reserved-key mutation
  survived and the test was strengthened before the rerun killed it.
- [x] (2026-08-07) Completed Ready validation and an independent clean-worktree
  gate: the workspace excluding locked `tidb-datatype`, the datatype all-target
  suite excluding its two polluted observers, and each observer in its own
  fresh process all passed. Direct inventory greps and
  `rust/scripts/check-source-size.sh` passed.
- [ ] Dual-push the exact receipt SHA, verify both refs, and reclaim only this
  unit's worktrees and exclusive targets.

## Surprises & Discoveries

- Observation: the ranked `pkg/server/conn_test.go` handshake tests do not own
  their implementation in `conn.go`; they call
  `pkg/server/internal/parse/parse.go`.
  Evidence: `TestMalformHandshakeHeader` and `TestParseHandshakeResponse` call
  `parse.HandshakeResponseHeader` and `parse.HandshakeResponseBody`, whose
  definitions are at `parse.go:51` and `parse.go:76`.

- Observation: one complete Go source spans two Rust crates.
  Evidence: response and attribute parsing land in
  `tidb-server/src/handshake.rs`, while `StmtFetchCmd` lands in
  `tidb-protocol/src/prepared_statement.rs` and is consumed directly by
  `tidb-server/src/mysql_connection.rs`.

- Observation: the first attempted exact Rust test filter ran zero tests
  because the aggregate harness prefixes the module name.
  Evidence: `running 0 tests; 91 filtered out`. That result was discarded; the
  corrected fully qualified filter ran one test and failed at request 1025.

- Observation: Rust did not port Go's `maxFetchSize = 1024` rule.
  Evidence: the new boundary test observed `Ok((7, 1025))` before the fix and
  `Ok((7, 1024))` after the decoder began applying the source cap.

- Observation: the old Rust response parser collapsed Go byte strings through
  lossy UTF-8 and rejected three packets Go accepts.
  Evidence: fail-before tests observed replacement bytes for user `ff`, errors
  for an unterminated final plugin and malformed decoded attribute row, and no
  default 4096-byte attribute truncation. The byte-authoritative response and
  source-shaped parser now pass all four boundaries.

- Observation: Go's `if !truncated` at `parse.go:265` has an unreachable false
  outcome.
  Evidence: the only assignment `truncated = true` is immediately followed by
  `continue`; accumulated size is monotonic, so every later iteration also
  takes the earlier overflow branch and continues before line 265.

- Observation: the first reserved `_truncated` mutation survived because the
  test's limit rejected the client marker itself before a later overflow.
  Evidence: replacing `insert` with `or_insert` still passed at limit 20. At
  limit 25 the client marker is admitted, the second pair triggers truncation,
  and the same mutation fails because Go requires the server's value `18` to
  overwrite `client-value`.

- Observation: Ready clippy exposed a layout regression from the complete
  byte-authoritative response: `AuthHandshakePacket` grew to 288 bytes.
  Evidence: `clippy::large-enum-variant` failed on the inline authentication
  arm. The handshake phase already boxed the identical request, so boxing the
  packet arm removed the duplicate large shape; 350 server tests and scoped
  warnings-denied clippy then passed.

- Observation: two repository-wide Rust hygiene checks are currently blocked
  outside this unit's crate ownership.
  Evidence: dependency-inclusive clippy fails in
  `tidb-executor/src/driver/from.rs:800` on `type_complexity`; `cargo fmt --all
  -- --check` reports only committed files in `tidb-executor`, `tidb-expr`, and
  `tidb-session`. Scoped rustfmt for every file owned here and `clippy
  --no-deps` for `tidb-protocol`/`tidb-server` pass. Those external files were
  not changed because the collaboration contract reserves their crates.

- Observation: the literal clean-worktree `cargo test --workspace` does not
  pass at the accepted campaign tip because an already locked datatype test
  mutates the global collation registry and restores the wrong default.
  Evidence: `charset::tests::source_registry_vectors` observed `gbk_bin`
  instead of `gbk_chinese_ci`; the next wildcard test then saw its poisoned
  lock. The complete test set passes when partitioned into a workspace run
  excluding `tidb-datatype`, a datatype all-target run excluding those two
  observers, and one fresh process for each observer. This is explicitly not
  represented as a literal aggregate-command pass, and the locked datatype
  owner was not reopened.

## Decision Log

- Decision: use one atomic source-file lockdown even though the Go file lands
  in both `tidb-server` and `tidb-protocol`.
  Rationale: splitting out `StmtFetchCmd` would leave one Go function silently
  outside the inventory. One unit reserves both otherwise-unowned crates.
  Date/Author: 2026-08-07 / Codex.

- Decision: enumerate both outcomes of every syntactic control locus rather
  than summarizing branch families.
  Rationale: the source contains 36 `if` statements, three loops, one logical
  short-circuit, and one deferred recovery path. Family summaries could hide a
  missing false, exit, short-circuit, or panic outcome.
  Date/Author: 2026-08-07 / Codex.

- Decision: fix fetch capping inside `decode_prepared_statement_fetch`.
  Rationale: this is the one parser boundary every caller uses; a conditional
  in `mysql_connection` would leave other consumers with non-Go values.
  Date/Author: 2026-08-07 / Codex.

- Decision: preserve user, database, plugin, and connection-attribute keys and
  values as raw bytes at the parser authority, while retaining explicit text
  compatibility views for current consumers.
  Rationale: Go strings preserve arbitrary bytes. Lossy decoding inside the
  parser destroys identity and duplicate-key semantics and cannot be repaired
  downstream.
  Date/Author: 2026-08-07 / Codex.

## Outcomes & Retrospective

The source-completeness, mutation, and scoped Ready phases are closed: all eight functions,
82 control outcomes, 11 attributed test/support declarations, and two build
artifacts have verdicts, and all 33 final mutations die. Rust corrected the
fetch cap, default attribute policy/metrics, malformed optional-attribute
recovery, unterminated plugin handling, NULL semantics, and byte-string
authority. The complete workspace test set and ratchet pass in the documented
fresh-process partition. Dual-push verification and reclamation remain; no
remote receipt claim is made yet.

## Context and Orientation

`pkg/server/internal/parse/parse.go` is the sole Go source owner. Its public
`StmtFetchCmd` decodes the eight-byte fetch payload. Its public
`HandshakeResponseHeader` and `HandshakeResponseBody` mutate a
`handshake.Response41`. Five private helpers decode connection attributes,
apply the configured aggregate-size policy, generate warnings, and update two
atomic status counters.

The Rust response parser currently lives in
`rust/crates/tidb-server/src/handshake.rs`; the response representation is in
`handshake_response.rs`. The fetch decoder lives in
`rust/crates/tidb-protocol/src/prepared_statement.rs` and is re-exported from
that crate's `lib.rs`. `mysql_connection.rs` is the production consumer of both
the handshake response and fetch decoder.

Original Go tests are in `pkg/server/internal/parse/parse_test.go`,
`pkg/server/internal/parse/handshake_test.go`, and the source-calling portions
of `pkg/server/conn_test.go`. The adjacent `BUILD.bazel` files are build
artifacts, not production semantics, but must be inventoried as package support.

A control locus is one syntactic decision: an `if`, loop, `&&` short-circuit,
or deferred panic recovery. Each has two outcomes in the inventory. A mutation
probe changes one rule in a disposable worktree; its intended fully qualified
test must fail. A passing mutation means the test is weak.

## Plan of Work

First, derive Go oracle evidence for every response shape: short and complete
headers, every auth-length capability, NULL and special auth markers, absent
and malformed terminators, optional database/plugin/attribute fields, each
length-encoded integer width, invalid UTF-8 bytes, attribute decode failure,
duplicate keys, limits 0/negative/exact/below/above, truncation ordering,
reserved-key overwrite, warning combinations, 64 KiB metric boundary, CAS
max behavior, zstd presence, trailing bytes, and mutation state after errors.

Second, change the Rust landing APIs at their owning layers. The fetch decoder
applies the cap. The response parser must expose any Go mutation or diagnostic
state that the current constructor-only API cannot express. Connection
attribute policy and metrics must be explicit and testable without relying on
process-global test ordering, while the production default remains Go's 4096
bytes. Raw wire bytes must not be silently collapsed if Go distinguishes them.

Third, add `parse_go_inventory.rs` beside the server parser and register it
test-only. It will pin the full Go source, enumerate all eight functions and 82
outcomes, classify all source-owned test/support declarations, compile-anchor
every PORTED Rust symbol (including the protocol fetch decoder), and reject
duplicates, empty evidence, unknown verdicts, or count drift.

Fourth, create an immutable provisional commit and a disposable detached
mutation worktree. Mutate every independent family: fetch length/cap/endian,
header length/offset/collation, each auth mode, optional field handling,
malformed recovery, every length-encoding width, raw-byte handling, attribute
decode/policy/warnings/metrics, zstd, source hash, and symbol anchors. Reverse
each mutation explicitly and verify cleanliness after every probe.

Finally, use the Ready profile. Run both affected crates' all-target tests and
clippy, formatting and diff gates, then create a new detached clean worktree at
the receipt SHA with a new target directory for full workspace tests and
`make -j12 lint`. Verify inventory/ratchet constants directly, push the same
SHA to both remotes, verify both refs, and reclaim exact task-owned paths.

## Concrete Steps

All commands run from the isolated worktree. Cargo commands run from `rust/`
and use one exclusive target directory; they run serially.

Targeted Go source tests (no failpoints detected):

    go test -p 12 -tags=intest,deadlock ./pkg/server/internal/parse \
      -run '^(TestParseStmtFetchCmd|TestParseAttrsUnderscoreWarning|TestAuthSwitchRequest)$' -count=1

Targeted Rust loops:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo test --offline --locked -j12 -p tidb-protocol --test all \
      <fully-qualified-test> -- --exact

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo test --offline --locked -j12 -p tidb-server --test all \
      <fully-qualified-test> -- --exact

Ready and clean-gate commands:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo test --offline --locked -j12 -p tidb-protocol -p tidb-server --all-targets
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo clippy --offline --locked -j12 -p tidb-protocol -p tidb-server --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<fresh-clean-target> \
      cargo test --offline --locked -j12 --workspace
    make -j12 lint

`pkg/server` tests are run through
`tools/check/failpoint-go-test.sh` because that larger package uses failpoints;
the leaf `pkg/server/internal/parse` package does not.

## Validation and Acceptance

Acceptance requires all eight Go functions and all 82 control outcomes to have
exactly one evidence-backed verdict. Every directly source-owned original test
or support declaration must be present. The full-source hash, byte/line counts,
ordered declaration scan, branch-key set, and compile-time PORTED symbols must
all be executable gates.

Behavioral acceptance requires the exact Go values, errors, mutation states,
attributes, warnings, and metrics at every listed boundary. Each real mismatch
has recorded fail-before/pass-after evidence. Every mutation must die in the
intended test; a zero-test filter or surviving mutation is rejection evidence.

Final acceptance additionally requires a clean final SHA, successful Ready and
clean-worktree gates, direct ratchet verification, identical `origin` and
`ngaut` refs, and removal of only this unit's worktrees and exclusive targets.

## Idempotence and Recovery

Source inspection, probes, tests, hashes, and greps are safe to rerun. Go
oracle probes are disposable and must leave no Go file changed. Mutations occur
only in a detached disposable worktree. If a mutation cannot be reversed with
certainty, remove that exact worktree and recreate it from the provisional SHA;
never use destructive reset or checkout commands.

Do not remove an exclusive target or worktree until the exact final SHA is on
both remotes. Never inspect the divergent shared checkout, touch another
agent's crate, or delete another unit's artifacts.

## Artifacts and Notes

Baseline Go source tests passed. A disposable Go overlay independently pinned
invalid user bytes, unterminated plugin behavior, NULL and one-byte auth
markers, malformed decoded attributes, and truncation metrics. The fetch-cap
regression initially failed at request 1025 and passed after the decoder fix.
Four parser divergences failed together before the response/parser correction
and passed afterward. One accidental zero-test filter is recorded as discarded
evidence. The first affected-crate all-target run was sandbox-blocked at four
loopback socket binds; the identical permission-enabled rerun passed 448 tests
with one pre-existing ignored protocol test.

## Interfaces and Dependencies

`tidb-protocol::decode_prepared_statement_fetch(&[u8])` remains the native
fetch decoder and returns `(statement_id, capped_fetch_size)`. A named exported
constant anchors the 1024 cap.

The server parser will retain `parse_response`, `parse_response_header`, and
`parse_response_body` compatibility while adding only the source-shaped state
or context interfaces required to expose Go mutation, attribute policy,
warnings, and metrics. `HandshakeResponse41` remains the response authority;
any raw-byte extension must keep existing UTF-8 consumers explicit rather than
silently changing their identity inputs.

Revision note: initial plan created 2026-08-07 after resolving the ranked test's
actual Go owner, publishing the two-crate claim, and capturing the fetch-cap
fail-before/pass-after loop.
