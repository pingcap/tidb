# TiDB → Rust rewrite — Agent Handoff

_Last updated: 2026-07-18. This document preserves durable decisions and
historical context. Read generated `STATUS.md` first for current counters,
campaigns, and queue state; do not scan every historical wave before dispatch._

## 1. The goal (standing instruction)

Rewrite TiDB's SQL layer in Rust, faithfully, verified differentially against the real Go implementation. The recurring driving prompt is:

> implement the design: `docs/design/2026-07-11-tidb-rust-rewrite.md`, do not miss any test that original TiDB has. refactor the project structure so we can use an agent team to work in parallel.

Work proceeds as **multiple non-overlapping, source-owned increments in parallel**. Each increment is still a closed verification loop: pick a bounded Go domain → port its normal structure and tests → prove it with the relevant differential ring → add a regression → submit a narrow integration request. Root stewards serialize only the four shared seams named in `PARALLEL.md`; agents are organized by feature domain, not by horizontal file type.

Read the design doc (`docs/design/2026-07-11-tidb-rust-rewrite.md`) for the long-term plan: cluster-level strangler, **NO cgo anywhere** (hard rule — every cross-language boundary is a serialized network protocol), port-don't-redesign, and verification via four differential rings (parser / plan / result / transaction).

## 2. THE governing principle (read this twice)

**TiDB's Go code is the source of truth — for the ENTIRE project, every layer.**

Every Rust file is a *port* of a specific Go implementation. `godump`/`gorun` (the differential tools below) only **confirm** a port on the cases you probe — they do **not** define the rule. Inferring behavior from a handful of differential outputs is a guess and *will* ship edge-case bugs (this already happened — see task #160: LPAD negative-length and CONV sign handling were both wrong until read from Go).

**Workflow for any behavior:** find the owning Go code → read it → port *that* logic → use `godump`/`gorun` only to confirm. When they disagree, **the Go code wins**; investigate why the tool differed (version/config/mock backend), don't trust the observed output.

Where the authoritative Go lives, by layer:
- Lexer/tokens → `pkg/parser/lexer.go`, `misc.go`, `charset.go`
- Grammar / AST / restore → `pkg/parser/parser.y`, `pkg/parser/ast/*.go` (each node's `Restore`)
- Expression eval / type inference → `pkg/expression/builtin_*.go`
- Types / charset / collation / decimal → `pkg/types/*`, `pkg/util/collate/*`
- Planner → `pkg/planner/**`; Execution → `pkg/executor/**`
- DDL / schema / catalog → `pkg/ddl/**`, `pkg/meta/**`, `pkg/infoschema/**`
- Session / vars / txn → `pkg/session/**`, `pkg/sessionctx/**`, `pkg/store`/`pkg/kv`

Also honor the repo's `AGENTS.md` (correctness first, no speculative behavior, minimal diffs) and the user's global rules: no workarounds/band-aids, verify before claiming done, eliminate edge cases rather than special-casing, `make -j12` for Go builds.

## 3. Where the code is — the Rust workspace (`rust/`)

The workspace has fifteen behavior crates and four evidence packages, tracked
from the Campaign 07 baseline with the toolchain pinned in
`rust-toolchain.toml`:

| Package | Role | Go counterpart |
|---|---|---|
| `tidb-lexer` | tokenizer | `pkg/parser/lexer.go` |
| `tidb-ast` | AST nodes + `restore_into` (SQL text regeneration) | `pkg/parser/ast/*` |
| `tidb-parser` | recursive-descent parser | `pkg/parser/*_parser.go` |
| `tidb-proto` | generated prost protocol leaves shared by real consumers | `tipb`/`kvproto` serialized contracts |
| `tidb-pd-client` | bounded plaintext PD gRPC control plane with discovered-member retention, foreground refresh, role-aware direct-endpoint failover, and cluster/region/store metadata | pinned `github.com/tikv/pd/client` GetMembers/GetRegion/GetStore and service-discovery paths |
| `tidb-protocol` | source-backed uncompressed MySQL framing, result packets, column metadata projection, and dependency-closed typed text-row scalar formatting | `pkg/server/internal/packetio.go`, `pkg/server/internal/column/**`, `pkg/format/textrow/**` |
| `tidb-distsql` | request/response metadata plus the lazy direct-unary task, retry/rebuild, paging, warning, request-owned active cancellation, bounded optimistic lock recovery, and detach-state runtime | `pkg/distsql/**`, `pkg/store/copr/**` |
| `tidb-expr` | typed expression construction, evaluation, and builtins | `pkg/expression/*` |
| `tidb-datatype` | sole shared SQL scalar authority: `Datum`, `FieldType`, charset/collation, exact decimal | `pkg/types/**` and TiKV query datatypes |
| `tidb-stats` | source-backed CMSketch/TopN/FMSketch/loading-status statistics primitives | `pkg/statistics/**` |
| `tidb-codec` | byte-exact comparable scalar and datum-key encoding | dependency-closed paths in `pkg/util/codec/**` |
| `tidb-txnkv` | transaction primitives plus the live address-keyed `tikvpb.Tikv/Coprocessor` RPC leaf, API-v1 PD region loader, exact region-error recovery, store hydration, bounded region backoff, and sole leader-routing RegionCache | `pkg/kv/**`, pinned `tikv/client-go/v2` transport and locate paths |
| `tidb-exec` | a seed stateful executor plus shared-cluster/session, aggregate leaves, result metadata bridge, typed scalar result encoding, and framed COM_QUERY boundary | `pkg/session/**`, `pkg/executor/**`, `pkg/server/**` |
| `tidb-server` | source-shaped unframed and framed connection dispatch over protocol, DistSQL, and the seed session | `pkg/server/conn.go`, `pkg/server/conn_stmt.go` |
| `difftest` | shared differential library, Go helpers, corpora, inventory/ledger generators, and two infrastructure tests | — |
| `difftest-parser-tests` | parser-only oracle replay, topology gate, and stable selector shards | `pkg/parser/**`, parser fixtures |
| `difftest-result-tests` | expression/query/table result rings | `pkg/expression/**`, `pkg/executor/**` |
| `difftest-transaction-tests` | source-translated transaction evidence plus owner-run PD/RealTiKV routing and movement proofs | `pkg/kv/**`, pinned client-go/PD/TiKV paths |

`tidb-exec` is a **seed executor**: a flat in-memory catalog (no real TiKV,
databases, views, or users), a deliberately incomplete `Datum` domain, and no
planner. `Cluster`/`Session` establishes the multi-session ownership seam for
bounded source tests, but it is not the `tidb-txnkv` protocol. The separate
`tidb-proto`, `tidb-codec`, and `tidb-txnkv` now form a real dependency chain
for generated request-tag wire contracts, comparable keys,
`Int`/`Common`/`Partition` handles, a live TiKV Coprocessor unary RPC leaf, and
source-shaped RegionCache/leader routing. Campaign 10 made that cache the sole
DistSQL topology authority; Campaign 11 retains discovered PD members, performs
foreground role-aware endpoint refresh/failover, consumes exact nested region
errors, atomically recovers cache routes, and retries/rebuilds only failed work
under one bounded per-region budget. Campaign 12 adds request-scoped peer
selection, generation-aware transport recovery, shared-store invalidation, and
foreground health. MVCC, lock resolution, background health, TLS/forwarding,
and commit remain unimplemented. Many statements are honestly `Unsupported`
at execution while being fully
parse+restore-faithful. That's intentional — don't fake success.

The first connected local read-only seam is now real: `tidb-protocol` frames
and validates uncompressed MySQL packets, `Session::execute_framed_query`
accepts only `COM_QUERY`, `tidb-parser` and the shared session execute the SQL,
and `tidb-distsql::DistSqlContext` receives the original SQL metadata.
`Session::execute_framed_query_text_rows` adds the bounded row leaf, while
`Session::execute_framed_query_text_result_set` consumes caller-supplied
`ColumnInfo` and `ResultSetOptions` to frame the source-shaped column-count,
metadata, legacy-EOF, row, and terminal-EOF/OK sequence. The separate
`tidb-server::Connection` now consumes the command decoder and owns the
explicit COM_QUERY/PING/QUIT connection lifecycle. This proves the
transport/session/server composition. The isolated executor result metadata
adapter now converts source-shaped ResultFields into those protocol columns,
with a direct framed-result regression. The connected result path formats
integer, float, dependency-closed decimal, and byte Datum values against the
caller-supplied field type, while `tidb-server::Connection::dispatch_framed`
reframes response packets at server sequence one. The new
`dispatch_framed_auto` path closes the safe parser→ResultField resolver→
`ColumnInfo` sequence for plain table-less and single-table catalog-backed
`SELECT`, and now proves bounded catalog-backed INNER/CROSS/LEFT/USING
bare-wildcard metadata/rows, including null extension and coalesced field
order. Explicit projections and other planner-dependent shapes remain
explicitly rejected until their output-name contract is wired.
`tidb-server` also has source-shaped initial handshake construction/response
parsing, an idempotent real-TCP listener lifecycle with bind/active/shutdown/
closed states and health ordering, and a generic injected accept loop.
`tidb-distsql` owns serial, per-channel, ordered response-event lifecycle, and
raw SelectResponse/Chunk/StreamResponse envelope validation; `tidb-exec` now attaches dependency-closed statement status to the
shared Session and exposes a lossless status-to-protocol snapshot bridge;
`tidb-protocol` owns source-shaped ERR payload bytes plus typed error-kind to
MySQL errno/SQLSTATE conversion; `tidb-proto` owns the authoritative
SelectResponse/StreamResponse response contract; `tidb-distsql` validates raw
SelectResponse/Chunk row boundaries; and `tidb-exec` exposes bounded
multi-relation ResultField and planner-owned join-output metadata, and
`tidb-exec::error_conversion` maps rendered ExecError categories into the
protocol descriptor. `tidb-server::error_response` now attaches those
caller-rendered errors to sequence-one ERR frames without synthesizing
context, and `tidb-codec::value` validates default-row tagged boundaries while
leaving typed Datum conversion explicit. These are
still bounded leaves: typed default/columnar/CHBlock codecs, general planner
ON/USING typing and explicit projection output names, and full
session/error-context attachment remain open alongside authentication,
TLS/compression, temporal/JSON/enum/set/vector and full session charset
conversion, Unix sockets/PROXY/connection admission, background PD/store health,
router service, production cache TTL/concurrency, forwarding/proxy and
label/load/slow/busy policy, initial PD-discovery cancellation,
pessimistic/async-commit lock recovery, TLS policy, and deployable bootstrap.

### Historical Campaign 10-13 boundary

Campaign `2026-07-read-path-10` is integrated and both receipt-backed claims
are released as `partial`. Campaign 09 first made the checked read chain reach
a real, address-keyed `tikvpb.Tikv/Coprocessor` gRPC socket with lazy channel
reuse, typed timeouts, exact request-context replacement, and raw response
bytes. Campaign 10 deletes the separate typed request sender and static route
maps. One shared RegionCache now owns half-open multi-region discovery, strict
continuity/progress, exact-version invalidation, cache reuse, and leader/store
selection. The concrete API-v1 loader bootstraps cluster identity and resolves
region/store metadata over the exact pinned PD gRPC methods while preserving
removed stores and forward-extensible peer-role integers.

The official 12-job Campaign 10 integration gate issued and consumed
`integration_receipt 2`. The pinned client-go connection, RegionCache,
single-store sender, replica-selector, context-attachment, and PD option
anchors pass. The pinned TiKV v8.5.6 live probe receives only the PD endpoint,
discovers cluster/region/leader/store/address in Rust, crosses DistSQL through
the unary leaf, and receives a structured TiKV application response. Teardown
proves both PD and TiKV endpoints are unreachable and leaves no dynamic owned
process, TiUP registry row, or tag directory.

Campaign 11 builds on that single topology authority without adding another
sender. Commits `d3060d12ed`, `1ab75ec3ab`, and `6f54db744a` integrated the PD,
txnkv, and DistSQL slices; `58c3ea76f8` closed cross-slice source-transit and
validation gaps. The same retained Rust client/cache passed the owner-run live
movement proof:

    Campaign 11 movement proof passed: PD http://127.0.0.1:26379 -> http://127.0.0.1:26382; TiKV 127.0.0.1:44162 -> 127.0.0.1:44161

The runner removed its TiUP tag, processes, phase directory, and endpoints.
The official 12-job gate issued `integration_receipt 3`; the campaign is
integrated and all three receipt-backed claims are released as `partial`. The
remaining routing boundary is background PD/store health, router service,
cache TTL/concurrency, generic TiKV connection-failure retry, locks/MVCC,
active in-flight cancellation, TLS/forwarding, and commit protocols.
Full table/DAG lowering and COM_QUERY integration also remain open.

Campaign 12 closes that historical generic unary connection-failure boundary
without adding a duplicate topology map or same-address retry counter. One
canonical store registry and request-scoped leader-semantics selector preserve
exact peer/store/address/channel generations. DistSQL owns failure precedence,
exact remote-Canceled close, foreground health, shared-store invalidation,
backoff, reselection, failed-task-only rebuild, and success-state mutation.
Tonic remains a fact-producing RPC leaf rather than a second retry policy.

The retained-process three-PD/three-TiKV proof bound two regions while they
shared one leader store, stopped that store before either RPC dispatch, and
then observed:

    region=28 127.0.0.1:47162#1 -> 127.0.0.1:47160
    successful_survivor_responses=2
    stale_future_dispatches=0
    recovered_store_liveness=Unreachable
    structured_results=2

The same campaign repairs the Go-test ledger so reachable testify suite
methods are tracked as exact parent-qualified obligations rather than hidden
receiver declarations. The final 12-job gate issued `integration_receipt 3`;
all three claims are released as `partial`, membership is archived, and the
queue has zero active claims. Commit `b57736524d` records the root integration.
The next boundary is follower/stale/learner and forwarding policy, active
in-flight cancellation, background recovery, TLS health, batch/stream/TiFlash,
locks/MVCC, slow-score policy, production concurrency/TTL, full DAG/table
lowering, and COM_QUERY integration.

Campaign 13 closes the bounded synchronous replica-policy, bound-unary active
cancellation, and optimistic read-lock gaps without introducing a second
client, topology map, deadline, or publication path. Request metadata now owns
the legal leader/follower/stale access path, learner admission, and rotating
seed; known `NotLeader` and `DataIsNotReady` feed the same selector. One execution
cancellation parent mints a registered child for each bound transport request,
so executor cancellation fans out while response close stays local. The one
bind-anchored deadline covers Cop, retry waits, CheckTxnStatus, ResolveLock, and
TTL wait. Initial PD discovery still owns its separate PD timeout and remains
an explicit later boundary.

The retained production constructor installs the bounded optimistic resolver
over the same unary client and RegionCache. It handles committed, rolled-back,
and live-TTL status, uses fresh TSO for remaining TTL, resolves only the locked
secondary key, and retries only the unconsumed Cop task. The final live replica
proof preserved leader peer 15 while selecting nonleader peer 25/store 2. The
final coherent v8.5.7 lock fixture split row 2 into a secondary region batch;
the current failpoint-enabled TiDB committed row 1 and skipped secondary commit,
then Rust recorded one status RPC, one exact-key resolve RPC, two Cop attempts,
and one publication:

    campaign13_lock_recovery status=committed lock_start_ts=467767409883480085 caller_start_ts=467767409923063811 locked_key_hex=7480000000000000775f728000000000000002 primary_key_hex=7480000000000000775f728000000000000001 primary_route=127.0.0.1:51160 commit_ts=467767409883480086 resolve_route=127.0.0.1:51160 resolve_key_hex=7480000000000000775f728000000000000002 cop_route=127.0.0.1:51160 cop_attempts=2 publications=1

Both live runners removed owned processes, TiUP state/data, and endpoints. The
Ready lint and frozen 12-job gate passed and issued `integration_receipt 4`;
all four receipt-backed claims are released as `partial`, exact membership is
archived, Campaign 13 is `integrated`, and the queue has zero active claims.
Pessimistic/async-commit locks, TxnNotFound retry, forwarding/proxy metadata,
label/load/slow/busy policy, background health/recovery, TLS,
batch/stream/TiFlash, production cache concurrency/TTL, full DAG/table
lowering, and COM_QUERY integration remain explicit. Read `STATUS.md` for the
generated membership and queue state.

## 4. The differential tools (how you verify)

Two prebuilt Go binaries at the **repo root**, untracked:
- **`./godump restore`** — reads SQL on stdin, prints the real Go parser's canonical restored SQL. Format: `#IDX n` / restored line / `#END`. Use to verify the Rust parser's `restore()` byte-for-byte.
- **`./gorun`** — reads SQL statements on stdin, runs them through a real (mock-backed) TiDB session, prints `OK` (side-effect), `RS:col|col;row2...` (result set), or `ERR`. Its stderr is noisy DDL logs — always `2>/dev/null | grep -E '^(RS:|OK|ERR)'`.

Both run from the **repo root**, not `rust/`. **cwd gotcha:** after running
`./godump`/`./gorun` the shell cwd is the repo root; the next Cargo command
must run from `$(git rev-parse --show-toplevel)/rust` or it fails with
"could not find Cargo.toml". This applies equally to per-slice worktrees.

### Differential test rings
- `difftests/parser-tests/tests/lexer_diff.rs` — token stream vs Go scanner.
- `difftests/parser-tests/tests/parser_diff.rs` — parse+restore vs the curated parser corpus. The curated
  corpus is not the whole parser-ring obligation: `integration_parser_inventory`
  inventories every SQL input that the pinned mysql-tester runners dispatch to
  TiDB in upstream `tests/**/t/*.test` fixtures (currently 51,598 inputs).
  `integration_runner_directive_inventory.tsv` separately accounts for the
  7,380 recognized client-side runner commands, so controls such as
  `connection`, `connect`, and `disconnect` are not silently dropped or
  mistaken for parser gaps. `integration_parser_diff.rs` replays a
  checked, byte-framed Go parse/restore oracle for every one without starting
  Go during normal tests. The current reviewed snapshot is 51,498 Go-accepted,
  99 Go-rejected, and one Go restore failure; Rust matches 51,488 accepted
  single-statement restores and 10 complete multi-statement restores. This is
  a measured gap, not a parity claim: no Go-accepted inputs remain Rust parse
  failures, no restore mismatches remain, no Rust accepts remain that Go
  rejects, and one hits the pinned Go restore-failure boundary. The remaining
  99 Go/Rust dual rejections are
  explicit rejection parity, not parser-porting work. Unsupported CREATE TABLE
  tails now fail rather than being silently erased, so the higher parse-failure
  count is a correctness tightening, not a coverage regression.
  `integration_parser_queue --check` turns this static replay into a
  deterministic, source-anchored task queue grouped by outcome and leading SQL
  shape; it omits explicit Go/Rust rejection parity from actionable work while
  reporting it in the summary. Use it to select parallel parser slices rather
  than mining fixtures opportunistically.
  Companion tool `dump_unhandled` lists statements Go parses but Rust fails:
  ```
  cd rust && PARSER_COV_STMTS=difftests/corpus/real_statements.txt \
    PARSER_COV_GOLDEN=difftests/corpus/real_golden.txt \
    cargo run --locked --example dump_unhandled -p difftest 2>/dev/null | wc -l
  ```
  Do not treat an unhandled count, an `Unsupported` boundary, or a fixture
  inventory as parser parity. Every input remains an obligation until its
  parser-ring evidence is explicitly triaged.
- `difftests/result-tests/tests/query_diff.rs` — table-less `SELECT`
  execution vs `corpus/query_golden.txt`.
- `difftests/result-tests/tests/table_diff.rs` — stateful
  CREATE/INSERT/SELECT scripts, one topic per file pair under `corpus/table/`.
- `difftests/result-tests/tests/expr_diff.rs` — expression corpus.

The root `difftest` package owns shared helpers and generators only. Keep
parser selectors out of its dependency graph so parser agents never compile
`tidb-expr` or `tidb-exec`.

### Adding a query_diff regression (the current per-increment ritual)
```bash
# 1. append statements to the corpus
cat >> rust/difftests/corpus/query_statements.txt <<'EOF'
select your_new_expr(...)
EOF
# 2. regenerate golden FROM REPO ROOT (existing lines must stay unchanged; only your new ones append)
grep -v '^##' rust/difftests/corpus/query_statements.txt | ./gorun 2>/dev/null | grep -E '^(RS:|ERR)' > /tmp/g.txt
diff rust/difftests/corpus/query_golden.txt /tmp/g.txt   # sanity: only additions
cp /tmp/g.txt rust/difftests/corpus/query_golden.txt
```

## 5. Current phase & immediate next work

> **⚡ PARALLEL, SOURCE-FIRST MODE:** read **`rust/PARALLEL.md`** before dispatching. `difftests/corpus/coverage/go_test_inventory.tsv` makes every Go test entry point, lifecycle hook, shell program, SQL fixture/result, `testdata` file, and support artifact below repository test suites visible; `integration_parser_inventory.tsv` inventories every SQL input the fixture runner dispatches; and `integration_runner_directive_inventory.tsv` accounts for runner-only commands. These are obligations, not parity claims.

The active high-throughput work is split across six non-overlapping lanes:

1. source-queue parser families, each owning typed AST, parsing/restore,
   executor capability classification, mirrored tests, and a selector;
2. source-owned expression/result families behind narrow dispatch seams;
3. the plan ring, beginning from exact planner Go tests and adding a planner
   crate only when its first real API and consumer move together;
4. shared cluster/session state plus `tidb-txnkv`, proceeding from complete
   `pkg/kv` source/test units toward real transaction buffers and protocols;
5. datatype-owned `Datum`, typed `BuildContext`, and one `EvalContext`; the
   normal public AST path now resolves source-visible string/binary signatures
   before evaluation, while schema-derived `FieldType` and full
   statement-context propagation remain active work;
6. evidence/workspace stewardship: stable selector shards, byte-lossless
   labels, exact source-unit queues, and strict executable-corpus/evidence
   namespaces.

The first source-domain extractions are physical, not ownership comments:
partition DDL, account security, GRANT/REVOKE, SQL bindings, ordinary SHOW,
ordinary SET/session statements, ADMIN, ANALYZE TABLE, standalone FLUSH,
TRAFFIC/REFRESH STATS, CREATE/ALTER/DROP RESOURCE GROUP,
CREATE/ALTER/DROP PLACEMENT POLICY, masking-policy grammar, LOAD DATA,
sequence grammar/AST restore, BinaryLiteral, FSP, EvalType, time and math
builtins, KV request-support checking, transaction-source bitfields, and seed
transaction state now live outside their former roots. The complete account,
authentication, password-policy, role, and user-variable parser surface now
lives in `user.rs`/`set.rs`; parser `lib.rs` is 963 lines and its mixed
statement test file is 546 lines. Executor DDL effects now live in
`ddl/create_table.rs`, `ddl/index.rs`, `ddl/table.rs`, and `sequence.rs`;
`database.rs` is 261 lines. Executor `lib.rs` is 739 lines after set-operation,
literal, table-reference, result, and table-less SELECT behavior moved to
their physical owners. ENUM/SET now consume a single byte-preserving
general-CI/UCA-4.0 collation authority generated from all 65,536 source
weights and all 22 long-rune expansions. Datatype and codec
crate roots are declarations and re-exports only; AST, parser, and executor
roots have also shed their transaction/explain/user-variable payloads,
top-level statement dispatcher, result/error/session-setting contracts,
ADMIN/session runtime, and mirrored tests into independent leaves. Private
pre-split executor re-exports and the obsolete IMPORT aliases were deleted;
internal code imports the physical source owner directly.
The parser evidence package has no `tidb-expr` or `tidb-exec` dependency. The
shared-cluster seam publishes catalog effects even when a source-faithful
statement returns an error after a partial DDL effect, and retries the whole
statement after a stale version. `tidb-codec` and `tidb-txnkv` provide real
comparable-key and row-handle contracts rather than protocol stubs. These are
the patterns to repeat; do
not grow the roots again.

For parser work, start with `integration_parser_queue --check`, choose one non-overlapping leading SQL shape, then port the owning Go parser routine plus its AST `Restore` method structurally. Use a selector covering the exact fixture family and report its controlled before/after outcome delta. Parser-only syntax must execute as `Unsupported` before it mutates catalog or transaction state; never emulate real TiDB behavior with an in-memory shortcut.

The reviewed oracle currently has 51,598 inputs: Go accepts 51,498, rejects 99, and has one restore failure. Rust exactly restores 51,488 accepted single statements and 10 complete multi-statement rows; no Go-accepted inputs remain Rust parse failures or restore mismatches, no Rust accepts remain that Go rejects, and one reaches the pinned Go restore-failure boundary. All 99 Go/Rust rejections match and are excluded from the actionable queue, leaving 1 actionable nonmatch.

The 853-row creation-partition selector is now 853/853 exact with no parse failure or restore mismatch; the 11-row binary-string selector is all exact. Direct Go `parseStringOptions` porting moved 20 global rows to exact restore (11 parser failures and 9 mismatches), while preserving every Go-rejection category; it models `BINARY` before/after charset, `BYTE`, `ASCII`, and `CHARACTER SET binary` storage-type normalization. The shared column-CHECK port added 108 exact global matches, including 17 partition-selector rows, with no new mismatch or false-accept outcome. The direct field-type port added 48 exact global matches and two partition-selector rows by transferring Go aliases plus the final ordered `UNSIGNED`/`SIGNED`/`ZEROFILL` state; it also preserves Go's YEAR modifier-consumption boundary. The direct table-option port adds 18 exact AFFINITY rows, creation-side SPLIT adds four exact rows, inline key adds 110 exact rows (109 bare `KEY`, one `KEY GLOBAL`), and the typed column-options leaf adds 53 exact rows (4 SERIAL, 49 AUTO_RANDOM). CREATE USER statement-global REQUIRE/WITH/RESOURCE GROUP adds 17 exact rows, table-level ALTER ATTRIBUTES adds 13, partition ATTRIBUTES adds six, SHOW TABLE NEXT_ROW_ID adds 13, direct ALTER INDEX visibility adds 22, terminal ALTER TABLE PARTITION BY adds 49, ALTER CHECK enforcement adds 16, SHOW TABLE STATUS adds three, ALTER COLUMN SET/DROP DEFAULT adds 25, and SHOW STATUS adds three; all preserve Go parser/restore canonicalization without widening execution. Every one rejects unsupported executor semantics before mutation. CTAS adds one exact parser match and retains a pre-commit unsupported execution boundary, matching TiDB's current planner contract. The accepted-Go-rejected queue is now empty: unsigned LIMIT overflow, negative datetime precision, and invalid ALTER-column collation are all rejected at their Go-owned parser boundaries. The current parser wave also rejects unsupported CHAR/CONVERT USING charsets, legacy charset introducers, one-argument DOUBLE, and adjacent builtin-function CREATE TABLE names. The full Go collation catalog is centralized in the lexer metadata layer and consumed by column, table, ALTER, database, and CONVERT TO branches. Update the snapshot only after reviewing the full change in every outcome category.

The raw-invalid-byte ENUM boundary is now explicit and lossless. `ColumnType`
uses a typed `ColumnTypeArg::Bytes(Vec<u8>)` for binary ENUM/SET members, and
`Stmt::restore_bytes()` is the byte-preserving counterpart to the UTF-8-only
`restore()` convenience API. The parser differential ring compares that byte
sink, closing the two GBK `0x91` rows (and the other binary-member rows) without
replacement text, invalid Rust strings, or escape-syntax workarounds.

The final two restore mismatches are now source-owned: executable no-ID `T!`
comments retain Go's `SHARD_ROW_ID_BITS`/`PRE_SPLIT_REGIONS` table options, and
the parenthesized outer query after `WITH` retains Go's `SelectStmt.IsInBraces`
restore boundary. Both have direct tests, static selectors, and evidence
fragments; the only remaining queue row is Go's own `json_memberof()` restore
failure.

The subsequent direct-source wave adds 21 exact `RENAME {KEY|INDEX}` restores
from Go `parseAlterRename` and 3 exact `ADMIN SHOW DDL JOBS` restores from Go
`parseAdminShow`. Both typed leaves retain their original parser rejection
boundaries and reject unsupported execution before mutation.

The next direct-source wave adds 21 exact single-action `DROP {CHECK|CONSTRAINT}`
restores from Go `parseAlterDrop` and one `ADMIN SHOW DDL JOB QUERIES` restore
from Go `parseAdminShow`. `LOCK = DEFAULT, DROP CHECK ...` initially remained
an explicit LOCK-prefix dependency, not a false DROP-CHECK coverage claim.

The following source-only wave closes that composition: Go
`parseAlterTableOptions` now owns typed `LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}`
with all twelve direct TestDDL spellings plus the exact LOCK-plus-DROP-CHECK
row, while bare `ADMIN SHOW DDL` is a distinct unit leaf from the same
`parseAdminShow` source family. Neither form appears as a standalone checked
integration-oracle row, so that wave itself left the global parser snapshot at 51,076 exact
restores; the direct original-test assertions are the evidence rather than a
fictional fixture delta.

The next parallel-source wave advances the reviewed static oracle by 32 exact
restores without changing any other outcome: typed `DROP FOREIGN KEY` adds 16,
SHOW VARIABLES WHERE adds 9, SHOW STATS_TOPN adds 4, and ADMIN
CANCEL/PAUSE/RESUME DDL job control adds 3. The three job-control rows preserve
Go's unusual discarded noun token (`JOB`, `JOBS`, or `FOO`) rather than
inventing a cleaned-up alias. Every leaf rejects unsupported execution before
mutation.

The current ALTER TABLE option wave advances the reviewed static oracle by 20
exact restores: AUTO_INCREMENT contributes four selected records, table-level
COMMENT contributes nine, and SHARD_ROW_ID_BITS contributes five. Two additional
source-shaped composite option records close through the same shared
`SetTableOptions` envelope. The aggregate snapshot is now 51,128 exact
accepted restores, 386 total Rust parse failures (324 Go-accepted), 8 restore
mismatches, 37 false accepts, and 370 actionable nonmatches. Every new option
leaf retains Go's canonical restore and rejects unsupported execution before
mutation.

The following parallel source wave adds ten exact table-level placement-policy
restores, three `SHOW STATS_LOCKED` restores, and 25 exact no-`ON`
role-membership restores (21 GRANT and four REVOKE records; the direct R1
selectors retain five GRANT and three REVOKE anchors). The role parser keeps
ordinary privilege, PROXY, and `REVOKE ALL, GRANT OPTION` branches separate.
The current wave adds six direct partition-placement restores, four `ADMIN
FLUSH SESSION|GLOBAL PLAN_CACHE` restores, and two `SHOW STATS_BUCKETS`
restores. The next wave adds 53 table-level `ALTER TABLE CACHE/NOCACHE`
restores, two `ANALYZE INCREMENTAL` restores, and two `SHOW OPEN TABLES`
restores. The following source wave adds DROP PRIMARY KEY, ALTER
TTL/REMOVE TTL, AUTO_ID_CACHE/AUTO_RANDOM_BASE, dynamic REVOKE privileges,
and explicit CREATE DEFINER view forms, moving 59 additional rows to exact
restore. The grouped ADD COLUMN literal-default source slice then adds 13
more exact integration rows, followed by 16 exact RENAME COLUMN rows. The
latest source wave ports ENUM/SET binary members, bare CHECK time functions,
`SET TRANSACTION ... AS OF` snapshot syntax, joined UPDATE bare `DEFAULT`
assignments, SHOW CHARACTER SET/CHARSET, and dynamic RESOURCE_GROUP privilege
names, adding 29 exact restores while preserving rejection-direction
categories. It also removes stale lexer debug output and the invalid
short-input token cap exposed by long integration statements. SHOW ENGINES then
closes one exact row. The current parallel rings add seven qualified CREATE
TABLE column rows, seven compatibility options, the adjacent ALTER TABLE and
EXPLAIN/LEADING slices, two partition actions, and three SET restore
mismatches. The follow-up ring adds the bare `ADD PARTITION` action and the
`CREATE TABLE ... ENGINE = MERGE UNION = (...)` option. The next partition ring
adds `DISCARD PARTITION ... TABLESPACE` and all 25 accepted direct
`FIRST/LAST PARTITION LESS THAN` rows in the checked integration corpus. The
MERGE FIRST, SPLIT MAXVALUE, and parenthesized set-operation rings then close
four additional accepted rows. The validation, ENGINE_ATTRIBUTE, and SHOW
MASTER/PRIVILEGES rings then close 22 more accepted rows. The ADMIN CLEANUP,
ordinary SHOW, and DROP DATABASE rings close four more accepted rows. Grouped
ALTER ADD COLUMN, reserved-name USE, and ADMIN ALTER DDL JOBS close eight more.
The EXPLAIN VALUES and REVOKE edge rings close seven additional accepted rows.
The ALTER generic-options, CREATE GLOBAL BINDING with DML, binary EXPLAIN
charset, and recursive LATERAL CTE rings then close eight more accepted rows.
The subsequent ANALYZE PARTITION, INSERT ... WITH ... TABLE, EXISTS set-op,
and comma-separated ENGINE/ROW_FORMAT rings close five more accepted rows.
The scalar-subquery set-operation ring then closes two more rows through the
same typed QueryStmt envelope. The byte-preserving ENUM/SET restore ring then
closes seven additional accepted rows through typed binary members and the
lossless `Stmt::restore_bytes()` sink. The decimal AST-format ring then closes
one restore mismatch by removing leading integer zeros while preserving
fractional scale. The NATIONAL/NCHAR/NVARCHAR field-type ring then closes one
accepted CREATE TABLE family, quoted column `COLLATE` closes the remaining
partition CREATE row, and EXPLAIN hint-name decoding closes one restore
mismatch. Shared table-option charset validation moves sixteen Go-rejected
The aggregate snapshot is now 51,488 exact single-statement restores plus 10
complete multi-statement restores, 99 Rust parse failures (all dual
rejections), 0 restore mismatches, 0 false accepts, and 1 actionable
nonmatch. The unsigned LIMIT,
datetime precision, and ALTER-column collation rings now close the final false
accepts; the multi-statement envelope and binary INSERT escape ring close one
parse-failure family and one restore mismatch. The CHAR/CONVERT USING and legacy charset-introducer rings, strict
DOUBLE arity, and CREATE TABLE builtin-name boundary are now source-owned as
well. All current
families retain
source-shaped ASTs and unsupported-before-mutation executor boundaries.

The planner ring now adds a real `tidb-planner` crate plus its independent
`difftest-planner-tests` consumer for all 12 source vectors in
`ApplyExponentialBackoff`, preserving Go's successive-root order and
`math.Min`/`math.Max` NaN and signed-zero behavior with an explicit external
boundary regression. The next planner slice ports all six row-size formulas
from `pkg/planner/cardinality/row_size.go` through a typed statistics adapter,
including fixed/variable widths, null-only columns, chunk/disk accounting, and
TiKV/TiFlash row-key sizing. `TestAvgColLen` has all 28 post-analyze value
assertions in an independent `row_size` test target; the real PlanContext,
HistColl, expression-column, and mock-store/analyze adapters remain explicit
partial work. The transaction ring also adds the exact
`IsUserKS`/`IsSystemKS` predicates and source tests to `tidb-txnkv`, while
keeping the rest of `pkg/kv/utils.go` explicitly partial rather than inventing
a storage client.

The follow-up transaction ring now adds source-faithful `IncInt64`/`GetInt64`
through a narrow `CounterStorage` contract, preserving missing-key zero,
decimal parsing, no-mutation-on-error, and signed overflow behavior. The
expression ring also closes the complete `TestSign` source table, including
NULL, signed/real values, numeric-prefix strings, and `UInt64(2^63)`, against
the production `SIGN` path.

The datatype ring now adds checked integer and duration arithmetic from
`pkg/types/overflow.go`, with all four original `TestAdd`, `TestSub`, `TestMul`,
and `TestDiv` tables reproduced in a standalone overflow test module. The
source-shaped overflow error preserves the exact BIGINT UNSIGNED boundary
message while the broader `dbterror` hierarchy remains an explicit partial
seam.

The next source-owned wave adds a byte-preserving ASCII encoding leaf from
`pkg/parser/charset/encoding_ascii.go`. `AsciiEncoding` keeps Go's UTF-8
lead-byte grouping, replacement/truncation operation bits, and dual
bytes-plus-error transform result without manufacturing a Rust `str`; the
shared charset registry and the other encodings remain explicit partial work.
The result ring also audits the existing LIKE leaf against Go's compiler and
matcher, adding custom-escape, `ESCAPE ''`, trailing-escape, NULL, and numeric
coercion regressions. Its scalar matcher is source-faithful, while vectorized
allocator, session collation, and warning/error state remain partial.

The transaction ring now owns source-shaped `NextUntil` and `WalkMemBuffer`
through explicit `KvIterator`/`KvRetriever` traits. `walk_mem_buffer` closes
the iterator on success and every callback/advance error, matching Go's
`defer`; the leaf does not invent a TiKV client or in-memory storage protocol.
Its seven direct integration tests run in a separate transaction test target.

The datatype ring now owns byte-first UTF-8 and strict utf8mb3 encoding leaves
from `encoding_utf8.go`: Go-compatible lead-byte `Peek`, decoder-width
`MbLen`, malformed grouping, three-byte validation, and the dual bytes-plus-
optional-error transform result. The shared encoding base/registry, GBK and
GB18030 families, and session warning channels remain explicit partial seams.

The shared datatype policy is now factored into `encoding_base.rs` and reused
by the ASCII and UTF-8 leaves. `TransformOp`, the generic bytes-plus-error
result, and `TransformPolicy` preserve Go's operation bits, first-error
retention, replacement/truncation, and source-over-converted collection
precedence; charset-specific decoder wiring and the registry remain partial.

The result ring adds the bounded ILIKE scalar leaf. `ilike_match` ports Go's
ASCII-only lowercasing and `LowerOneStringExcludeEscapeChar` escape state
before reusing the source-owned wildcard matcher. The complete scalar
`TestIlike` table is direct evidence; function-class/cache lifecycle,
session-selected collations, chunk/vectorized paths, and warning/error state
remain partial rather than being approximated.

The transaction ring also closes the portable `pkg/kv/key.go` helper family:
byte-preserving `Next`, carry-aware `PrefixNext`, comparison, prefix checks,
clone/string formatting, and safe half-open point boundaries. Typed
`tidb-proto` conversion now covers the portable part of
`TestKeyRangeDefinition`; the forbidden unsafe Go alias and Go-specific
104-byte layout assertion keep that test honestly `PARTIAL`.

The executor ring closes a typed peer-identity edge in the ranking window
family. `RANK`, `DENSE_RANK`, and `PERCENT_RANK` now compare adjacent ORDER BY
keys through the same typed comparator used by sorting, so equivalent INT and
UINT values remain peers while rank gaps, dense increments, and the single-row
percent denominator match Go. ROW_NUMBER remains the stable physical position
path; Go's aggregate allocator/memory hooks are still partial.

The planner ring adds `cardinality::join::estimate_full_join_row_count`, a
typed arithmetic leaf for Go's full-join estimator. It preserves Cartesian
products, equi-versus-NA key selection, larger-NDV division, the reorder
threshold, the exact left-key 0.9 exponent (including the non-equi fallback),
and Go NaN/signed-zero max behavior. Real `PlanContext`, expression-column,
schema, `StatsInfo`, and join-operator adapters remain explicit partial work.

The expression ring reserves the complete `pkg/expression/builtin_control.go`
file as one checked domain and closes the scalar `IFNULL` path with direct
source-table tests. Typed temporal/JSON/SET/error rows remain partial because
the seed evaluator lacks the required FieldType/session or non-SQL error
contracts; no eager-evaluation workaround was added. The representable
`TestCoalesce` source rows now have their own direct tests as well; Go's
FieldType-driven mixed numeric promotion and typed temporal rows remain
partial until the shared expression context carries that metadata.

The latest parser rings close the single accepted `SHOW ENGINES` integration
row through a dedicated typed AST/parser leaf, source-shaped restore coverage,
and a static selector. `LIKE`/`WHERE` filters remain represented in the AST,
while executor behavior is intentionally unsupported before transaction or
catalog mutation because the Rust seed has no engine registry. The aggregate
oracle then advances through seven qualified CREATE TABLE column rows, seven
CREATE TABLE compatibility options, the adjacent ALTER TABLE and
EXPLAIN/LEADING queue slices, two partition actions, and three SET restore
mismatches. The later generic ALTER-options, binding DML, binary EXPLAIN
charset, and recursive LATERAL CTE rings close eight more accepted rows; the
ANALYZE PARTITION, INSERT ... WITH ... TABLE, EXISTS set-op, and comma-separated
ENGINE/ROW_FORMAT rings close five more; the scalar-subquery set-operation
ring closes two more. The byte-preserving ENUM/SET restore ring then closes
seven more accepted rows, followed by one decimal AST-format restore match.
It now reports 51,488 exact single-statement restores plus 10 complete
multi-statement restores and 1 actionable nonmatch; the unsigned LIMIT,
datetime precision, collation-validation, and binary INSERT escape rings closed
all false accepts and one restore mismatch. The shared SHOW identifier source file remains intentionally
unpartitioned in the checked domain queue, so sibling entries stay independently
claimable.

The 2026-07-16 parallel source wave repaired the planner ownership route from
the obsolete `tidb-plan` name to the actual `tidb-planner` crate and kept the
source ledger green. Parser DDL-index ownership now covers the five exact
`ddl_test.go` anchors at lines 106, 121, 139, 149, and 165, including the
previously missing table-to-table rename test. The planner NDV wave records
the skew-ratio and Issue54812 anchors as honest `PARTIAL` evidence until
SessionVars, testkit/analyze statistics, and EXPLAIN integration exist. The
expression wave adds bounded ROW/IN and user-variable DECIMAL/DOUBLE casts;
VALUES remains partial and plan-cache parameters remain blocked. The focused
lanes and the full Rust workspace test, strict Clippy, and formatting checks
pass; the parser crate now has 520 unit tests. The next product milestone is
completing this connected read-only path, not another isolated leaf collection.

The next converged source wave extends that path instead of adding another
disconnected leaf. `tidb-protocol` now owns length-encoded integers, text-row
framing, column metadata, OK/EOF packets, and the source-shaped result-set
sequence; `tidb-distsql` adds `RequestEnvelope` concurrency/limit policy on
top of its request metadata; and `tidb-planner` now owns a typed physical-plan
metadata tree with Go-compatible ExplainID suffix behavior. The executor
integration proves framed `COM_QUERY -> Session -> DistSqlContext -> metadata
and text rows -> EOF` when the result-field owner supplies metadata. The
shared-session capability envelope accepts pure no-table queries, covered by
its regression. The fresh Rust WIP gate passes all workspace tests and strict
Clippy. The test ledger is now 16,135 `UNTRIAGED`, 298 `PARTIAL`, 140
`COVERED`, and 12 explicitly dependency-blocked; evidence contains 168 test
fragments. Executor-owned result-field resolution/registry wiring, dynamic
warning/status lifecycle, typed formatting, TiKV, and deployable server wiring
remain open.

The production-source ledger exposes all 2,390 non-test Go files and 956,318
lines, including 44 generated files. Status is 2,264 `UNTRIAGED`, 102 `PARTIAL`,
24 `COVERED`, and 0 `BLOCKED`; 64 files / 18,648 lines remain honestly
unassigned where the current crate map lacks a clean owner. Routing is not
coverage.

The independent upstream-test ledger exposes 16,585 obligations: 1,901 Go
test files, 10,583 valid AST-derived test/benchmark/fuzz/example entry points,
26 lifecycle hooks, 961 literal `t.Run` subtests, 381 dynamic/table-driven
`t.Run` generators, 19 exact fixture paths, 109 explicit unresolved fixture
accesses, 570 Bazel targets, 53 Make test/lifecycle targets, 262 shell
programs, 263 SQL inputs, 267 result files, 188 `testdata` files, and 1,002
other executable/config/data suite artifacts. A checked Go-AST declaration
inventory records all 15,546 functions across `*_test.go`, so comments,
strings, invalid runner signatures, and build tags cannot create phantom or
missing executable-test entries. Status is 16,132 `UNTRIAGED`, 301 `PARTIAL`,
140 `COVERED`, and 12 explicitly dependency-blocked; none of these counts is a
parity claim. Shared files can be divided by an exact test-domain manifest;
every unclaimed anchor stays a generated `UNTRIAGED` obligation, and evidence
without a unique claim fails the ledger gate. Dynamic/glob/helper/escaping
fixture paths remain explicit unresolved obligations rather than disappearing.

Coverage claims are now source-owned too: 52 production fragments, 171 test
fragments, 59 one-Go-source parser fragments, and one result-ring support
fragment live under `difftests/corpus/coverage/evidence/`. The generators
reject duplicate or stale anchors and missing artifacts before producing the
shared inventories.

The latest source wave keeps the connected path honest while adding two
parallel leaves. `tidb-protocol::decode_command` now ports the Go server's
command-byte split and one trailing-NUL `COM_QUERY` rule, preserving raw
payloads and making unknown commands explicit. `tidb-exec::result_metadata`
ports the dependency-closed `ConvertColumnInfo` arithmetic (names,
`EmptyOrgName`, flags, collation IDs, default widths, decimal adjustments,
character byte multipliers, duration decimals, and `VARCHAR` remapping) as an
isolated leaf; executor ResultField resolution, typed formatting, and session
charset conversion remain unwired. The parser FieldType wave adds the Go
geometry aliases and exact differential ownership for that source family.
The preceding protocol/result wave reported 2,390 production Go files at
2,265 `UNTRIAGED`, 101 `PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and 16,585
test/support obligations at 16,135 `UNTRIAGED`, 298 `PARTIAL`, 140 `COVERED`,
12 `BLOCKED`; 168 test and 51 production evidence fragments were registered.
The current integration-wave counts are recorded immediately below. This
remains a WIP checkpoint, not read-only-node or TiKV parity.
Parser ownership is currently 5 `ported`, 47 `partial`, and 3 `unassigned`;
these are source-ownership states, not parser parity.

The current integration wave adds four bounded owners. `tidb-server` now
consumes `tidb-protocol::decode_command` and owns a source-shaped
`Connection::dispatch` lifecycle for COM_QUERY, COM_PING, and COM_QUIT, with
malformed, invalid-UTF-8, closed-connection, and unsupported-command states
explicit; it does not encode response packets or claim auth/TLS/compression.
`tidb-distsql::KvRequestBuilder` ports the pre-transport `kv.Request` defaults,
one-use build invariant, non-partitioned key-range envelope, closest-read
labels, resource-group/session projection, DAG limit/concurrency policy, and
an explicit unbound transport marker. `tidb-exec::result_metadata` now owns
the source-shaped `colNames2ResultFields` naming adapter (default database,
expression original-name fallback, independent 256-byte alias truncation,
original identifiers, and FieldType propagation); full ResultField registry
resolution remains open. The parser column-options leaf closes Go's duplicate
COLLATE ordering asymmetry, and the parser crate now has 520 unit tests.
Focused lanes, full workspace tests, strict Clippy, formatting, ledgers,
parser inventory/golden/queue, plan inventory, and parser-package isolation
all pass. The current generated counts are 2,264 production `UNTRIAGED`, 102
`PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and 16,132 test/support `UNTRIAGED`,
301 `PARTIAL`, 140 `COVERED`, 12 `BLOCKED`; 52 production and 171 test
evidence fragments are registered. This remains a WIP checkpoint, not a
deployable server or TiKV parity claim.

The next connected response wave adds three smaller source-owned leaves.
`tidb-exec::columns_from_adapted_fields` now feeds the existing framed
result-set API with Go-shaped field names, flags, EmptyOrgName, and type
metadata, proving that adapter output reaches column-count/metadata/EOF/row
packets without inventing schema resolution. `tidb-exec` also routes COUNT's
signed partial state through a dedicated leaf: NULL-skipping ordinary updates,
partial addition, and merge semantics are source-shaped while spill,
typed-Eval dispatch, and distributed executor lifecycle remain open.
`tidb-protocol::textrow` ports Go's numeric text formatter, float exponent and
precision rules, year zero formatting, byte-preserving string values, and
explicit unsupported-type errors; charset/session Datum conversion remains a
separate owner. The current generated counts are 2,261 production
`UNTRIAGED`, 105 `PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and 16,128
test/support `UNTRIAGED`, 303 `PARTIAL`, 142 `COVERED`, 12 `BLOCKED`; 54
production and 173 test evidence fragments are registered. The full WIP gate
passes, but this is still not deployable-server or TiKV parity.

The current response/server wave extends that connected path instead of
adding another isolated leaf. `tidb-protocol::column` now preserves full
schema/table identifiers while applying Go's 256-byte display/original-name
rule, vector-float32 metadata overrides, and explicit default markers;
`tidb-protocol::textrow` adds MEDIUMINT and dependency-closed decimal text
with explicit temporal/JSON/enum/set rejection. `tidb-exec` now maps typed
integer, float, decimal, and byte `Datum` values through those source-shaped
column types, and `tidb-server::Connection::dispatch_framed` returns
sequence-one response frames for COM_QUERY/PING while treating COM_QUIT as an
explicit no-response close. The generated counts are 2,260 production
`UNTRIAGED`, 106 `PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and 16,128
test/support `UNTRIAGED`, 303 `PARTIAL`, 142 `COVERED`, 12 `BLOCKED`; 55
production and 173 test evidence fragments are registered. Temporal/JSON/
enum/set/vector formatting, charset/session conversion, automatic
ResultField derivation, authentication, compression, and listener lifecycle
remain open.

`pkg/parser/ddl_index_parser.go` remains one partial source owner. Its parser
leaf owns the shared index parts, index-constraint parsing, and foreign-key
actions; the root implementation and its `CreateIndexPart` alias are gone.
The related standalone `CREATE INDEX` path from `ddl_misc_parser.go`, CREATE
TABLE, ALTER TABLE ADD constraint, foreign-key AST payloads, and executor
readers now use the same full source-shaped `tidb-ast::ddl_index` model end to
end. The model covers all kinds, `IndexOption` fields, online modifiers,
typed index parts, reference actions, Go's marker-only restore edge,
vector-column acceptance, and column-level `REFERENCES`. The direct
Go-source leaves execute all 173 checks across the 15 attributable AST/parser
anchors, plus 64 exact `TestDDL` index/FK parser-table rows (50 accepted
restores and 14 rejections). Restore uses the generic
`RestoreContext`/`RestoreFlags` boundary for TiDB special comments; ordinary
restore remains its default mode. Keep `ddl_index_parser.go` partial: the
source/test ledgers still name broader untriaged parser obligations, and
parser coverage does not make unsupported catalog execution a ported
capability. Do not add an adapter or reduced field to claim it closed.

For expression/result work, use the same source-first discipline: read the owning `pkg/expression/builtin_*.go` routine before adding a Rust function, then add a focused Go-backed query corpus. Earlier audit issues such as TO_BASE64 line wrapping, three-argument FORMAT locale handling, CHAR byte order, and DATE_FORMAT week specifiers have source-backed ports and focused tests now. Collation/session-warning behavior is still outside the seed value domain unless the required context is ported; do not silently approximate it.

The latest parallel scalar/parser wave closed three concrete source edges. `INTERVAL`
now saturates overflowing string-to-real prefixes like Go's `StrToFloat` and
covers the original signed/unsigned, nullable, mixed, and precision rows. Integer
`TRUNCATE` now checks an unsigned scale's FieldType before narrowing its value, so
`u64::MAX` scale leaves the input unchanged instead of inventing a negative
precision. Partition-key `ALGORITHM` range rows and the complete interval-partition
parser table now have isolated source tests; the existing partition parser leaf
already restores/rejects all of those rows. The expression families remain partial
for function-class, warning/session, FieldType, vectorized, and catalog/execution
semantics, and partition execution/semantic validation remains outside the seed
parser.

The following wave added three more bounded leaves. LEAST/GREATEST now use Go's
string aggregate signature when a mixed numeric/string argument list is present,
including the `('123a','b','c',12)` row. Table-level index hints accept Go's
quoted index-name token and restore it canonically. Transaction retry arithmetic
now exposes the capped exponential upper bound without pretending to implement
Go's random sleep or `RunInNewTxn` storage loop; those orchestration obligations
remain partial.

The current result wave closes three scalar signature gaps. `FIELD` now selects
one Go-compatible signature for the entire argument list, so mixed string/numeric
inputs use numeric-prefix comparison consistently. `JSON_LENGTH` has a direct
38-row source table covering scalar roots, child counts, exact paths, NULL/missing
paths, and multiple-selection errors. `TestCompare` now has a direct real/decimal
promotion vector; temporal, JSON, collation, warning, session, and vectorized
layers remain explicit partial boundaries.

The follow-on result wave closes two source-owned scalar edges. `BIT_COUNT` now
uses Go's ETInt string-cast boundary: positive strings retain all UINT64 bits
before population count, overflow clamps to UINT64_MAX, negative overflow clamps
to INT64_MIN, and numeric prefixes survive malformed byte suffixes. The IPv6
scalar leaf now ports `INET6_ATON`/`INET6_NTOA` with Go's four-byte plain IPv4
versus sixteen-byte colon-containing representation, canonical mapped output,
invalid-length NULLs, and NULL propagation. Function-class construction,
warnings/session charset, and vectorized paths remain explicit partial seams.

The control slice now has source-backed `TestCaseWhen` coverage for simple and
searched CASE ordering, NULL conditions, real truthiness, and lazy dead
branches; typed JSON/error and result-promotion behavior stays partial.
`LENGTH`/`OCTET_LENGTH` now have direct rows for Go's evaluated-byte counts,
including numeric coercion, NULL, binary values, and incomplete UTF-8 suffixes;
typed datetime/SET/duration, connection-charset GBK, and warning context remain
outside the seed value domain.
The mixed string/numeric `GREATEST`/`LEAST` corpus pair now has its two missing
golden rows (`STR:c`, `STR:12`), so the expression result ring has no stale
topic-count mismatch.
`NAME_CONST` now preserves its second Datum across the representable integer,
unsigned, real, string, binary, decimal, and NULL rows. `TIDB_SHARD` now ports
Vitess' all-zero-key DES-ECB `HashUint64` algorithm and the Go ETInt coercion
before the 256-bucket reduction; the RustCrypto DES dependency is pinned in the
workspace lockfile rather than replaced by an output-derived lookup.

The same source-owned result lane then closes `TestIf`'s condition boundary:
the Rust evaluator now uses Go's integer-prefix `EvalInt` truth test and lazily
evaluates only the selected branch, including `1abc`, `0.1`, NULL, and
division-by-zero dead-arm rows. `IS_IPV4_MAPPED` and `IS_IPV4_COMPAT` now test
the raw sixteen-byte `ETString` prefixes (`::ffff:/96` and `::/96`) without
UTF-8 decoding. Typed result promotion, warning/session state, and vectorized
execution remain explicit partial seams.

The latest LIKE/REGEXP slice closes the original two-argument `TestRegexp`
vectors through the parser's `[NOT] REGEXP` dispatch, including malformed
pattern errors, while preserving the separate `REGEXP_LIKE` flag contract.
`TestCILike` now exercises all 25 source rows through the registered
general-CI and Unicode-CI wildcard collators, including accent/eszett folding
and supplementary-rune boundaries; the 0900 collation column, function-class
and session state, vectorized execution, and warning channels remain explicit
partial seams rather than being approximated.

The scalar operator slice now follows the source ETInt/ETReal coercion for
shifts, bitwise operators, unary `NOT`, `IS TRUE`/`IS FALSE`, and logical
operators. One shared numeric-prefix truthiness helper keeps malformed text,
`'0.3'`, NULL, and the representable decimal/real rows consistent across
those operators; typed duration/time/JSON, warning/session, FieldType, and
vectorized layers remain explicit partial seams.

The deterministic crypto slice now hashes the exact byte payload Go's
`EvalString` supplies: text, raw binary, and GBK bytes all reach MD5/SHA/SHA1,
SHA2, and PASSWORD without an accidental UTF-8 rejection. Numeric values keep
their decimal ETString rendering; SHA2 retains ETInt hash-length coercion; and
PASSWORD preserves empty/NULL behavior plus the uppercase, star-prefixed
double-SHA1 format. The deprecation warning, connection-charset conversion,
AES, compression, random, and SM3 families remain explicit statement,
nondeterminism, or dependency-boundary work.

The next result wave ports the byte-oriented `CONCAT` and `CONCAT_WS` leaves
from `builtin_string.go`: scalar and decimal arguments retain Go's ETString
rendering, binary payloads remain byte-for-byte lossless, `CONCAT` propagates
NULL, and `CONCAT_WS` skips only NULL fields while retaining empty fields and
returning NULL for a NULL separator. The shared `coerce_str_bytes` helper is
kept separate from Unicode text coercion so character functions cannot inherit
binary replacement behavior; typed temporal/duration values, packet-limit
warnings, function-class metadata, and vectorized execution remain partial.

The same source-owned wave ports `UUID_TO_BIN` and `BIN_TO_UUID` from
`builtin_miscellaneous.go`. UUID text accepts the Google UUID spellings used by
TiDB (canonical, uppercase, compact, and braced), rejects surrounding
whitespace, preserves raw sixteen-byte binary data, applies Go's two distinct
swap permutations, and propagates NULL. Warning/session state for malformed
swap flags and function-class/vectorized execution remain explicit boundaries.

The CEIL/FLOOR result boundary now follows Go's `getEvalTp4FloorAndCeil`
precision rule: a DECIMAL source with more than 18 integer digits remains a
DECIMAL result even when the exact rounded value fits `i64`. Decimal coefficient
metadata carries that source-width decision without parsing display text; the
existing exact digit-string ceiling/floor arithmetic remains unchanged.

The next result slice closes `TestRepeat` and the representable `TestRepeatSig`
rows. `REPEAT` now uses the source ETString/ETInt boundary, preserves raw
String/Bytes payloads, caps the count at Go's `math.MaxInt32`, returns an empty
value for non-positive counts, and applies the seed evaluator's default
64-MiB packet boundary. Custom `max_allowed_packet` warning state remains a
statement-context boundary rather than being silently fabricated.

The planner cardinality slice now ports `ScaleNDV`'s uniform probability,
skewed linear estimate, lower/upper clamps, and caller-provided risk-ratio
blend with all nine original Go vectors. Its dependency-closed multi-column
NDV leaf also preserves exact GroupNDV matching, conservative max estimates,
exponential backoff, empty/unknown/single-column behavior, and the full
`TestEstimateColsNDVWithExponentialBackoff` arithmetic table. SessionVars,
property registration, histogram statistics, and full planner integration
remain explicit partial seams.

The datatype type-predicate slice now carries every MySQL field-type byte
partition used by `pkg/types/etc.go`: blob/char/varchar/unspecified,
prefixable/fractionable/time/float/integer/stored-as-integer/numeric/temporal,
binary versus non-binary strings, and the registered-collation
`NeedRestoredData` split. Unknown type bytes round-trip through a typed
`Unknown(u8)` variant instead of being silently accepted; GBK/GB18030 and
`utf8mb4_0900` remain explicit collation boundaries.

The math result slice now executes complete source tables for DEGREES, SQRT,
PI, RADIANS, SIN, COS, ACOS, ASIN, ATAN, TAN, COT, EXP, LOG, LOG2, LOG10,
POW, ROUND, TRUNCATE, CRC32, and CONV through the existing math dispatch.
The source's exact UInt64 negative-scale truncation is preserved without an
intermediate f64 conversion; NULL/domain/error behavior and raw numeric-prefix
coercion are covered. Go statement warning counts, FieldType metadata, and
vectorized execution remain value-evaluator boundaries.

The JSON membership/path slice now ports `JSON_MEMBER_OF`, `JSON_CONTAINS`,
`JSON_CONTAINS_PATH`, and `JSON_OVERLAPS` from `builtin_json.go`: SQL-string
candidates retain their JSON-string identity, documents use parsed JSON
values, recursive object/array containment and shared-key/array overlap
follow BinaryJSON rules, and path selection preserves wildcard errors,
ONE/ALL short-circuiting, and missing-path NULL. `JSON_DEPTH` now has the
complete scalar/container depth table as well. The direct source tests are
owned; typed BinaryJSON, warning/session, function-construction, and
vectorized semantics remain explicit partial boundaries.

The JSON scalar slice now owns the complete representable `TestJSONType`,
`TestJSONQuote`, and `TestJSONUnquote` tables. JSON type classification,
control/unicode escaping, NULL propagation, unquoted passthrough, malformed
root errors, and invalid JSON-string handling are asserted against the source
shapes; typed BinaryJSON, charset validation, warning/session state,
function-construction, and vectorized execution remain explicit boundaries.

The JSON extraction/validity slice now owns the complete representable
`TestJSONExtract` and `TestJSONValid` tables. Exact path extraction, scalar and
container selection, invalid-document outcomes, and NULL propagation execute
through the shared JSON path walker; typed BinaryJSON inputs, warning/session
state, function construction, and vectorized execution remain explicit
boundaries.

The JSON key/removal slice now owns `TestJSONKeys` and `TestJSONRemove`.
Object-key sorting, exact path selection, missing/scalar targets, sequential
array-index shifts, no-op removals, NULL propagation, and wildcard/range/root
rejections follow the source path rules. Typed BinaryJSON, warning/session
state, function construction, and vectorized execution remain explicit
boundaries.

The string result slice now executes complete representable `TestSubstring`
and `TestLocate` tables, including Unicode, NULL, binary, empty-needle, and
start-position rows. It also preserves Go's positive-length `int64` overflow
boundary (overflow returns an empty substring rather than the tail) and
parameterizes `SPACE` for the source `max_allowed_packet` NULL boundary.
Session collation, warning collection, and vectorized execution remain
explicit partial boundaries.

The byte-string slice now preserves Go's byte-domain behavior for `ASCII` and
`BIT_LENGTH`: invalid UTF-8 `Datum::Bytes` values are accepted, ASCII returns
the first raw byte, and bit length counts raw bytes. The complete
default-charset source tables plus binary regressions execute through the
shared expression dispatcher; connection-charset conversion and typed
FieldType/session metadata remain partial.

The remaining byte-string slice now preserves Go's raw-byte coercion for
`UNHEX` and `OCT`: malformed binary payloads return the source NULL/zero
outcomes instead of being rejected as UTF-8, while numeric HEX/OCT signatures,
odd-digit padding, long Unicode input, and binary `CHAR_LENGTH` construction
are source-tested. Injected errors, connection charset conversion, and full
schema FieldType inference remain outside the seed evaluator.

The positional regexp slice now owns scalar `REGEXP_SUBSTR`, `REGEXP_INSTR`,
and `REGEXP_REPLACE`: UTF-8 character positions, occurrences, return options,
Go's backslash capture replacement tokens, and `i/c/m/s` flags are source
tested. Regex cache, warning/session, collation, vectorized, and typed binary
paths remain outside the seed evaluator.

The time-calendar source slice now owns the complete representable `TestDayName`,
`TestDateFormat`, and `TestWeek` tables plus the normal and malformed-input
portion of `TestLastDay`. It ports Go's directive formatting, week-mode
normalization, numeric temporal coercion, and strict invalid-time rejection;
SQLMode-dependent zero-day output, typed MySQL `Time`, and warning/session
state remain explicit value-domain boundaries.

The JSON mutation slice now owns the representable scalar rows of
`TestJSONArrayAppend` and `TestJSONArrayInsert`. Root/scalar wrapping,
nested paths, sequential path/value pairs, array-index shifts, out-of-range
insertion, missing-path no-ops, NULL propagation, and invalid path handling
follow the source implementation; typed BinaryJSON values, ParseToJSONFlag,
warning/session state, function construction, and vectorized execution remain
partial seams.

The string-miscellaneous source slice now owns the representable rows from
`TestChar`, `TestFindInSet`, `TestFormat`, `TestExportSet`, and `TestToBase64`.
It preserves Go's byte-order CHAR conversion, PAD-SPACE list matching,
locale-aware formatting, signed bit export, and 76-column Base64 wrapping;
connection charset conversion, warning/session state, collation metadata, and
vectorized execution remain outside the seed evaluator.

The control-flow slice now claims the exact `TestCaseWhen` Go test anchor.
Searched CASE first-match ordering, NULL and numeric truthiness, lazy dead
branches, and the unreachable JSON result row are source-tested; selected JSON,
injected-error, typed result-promotion, and function-class/session behavior
remain explicit partial boundaries.

The encryption slice now ports the representable default-charset portions of
`TestSQLDecode` and `TestSQLEncode`, including the deterministic MySQL 3.21
password-keyed stream cipher, empty/numeric-password cases, arbitrary decoded
bytes, round-trip encoding, NULL propagation, and arity. GBK connection
charset conversion, collation metadata, and deprecation warnings remain
session boundaries.

The concat slice now owns the representable `TestConcat` and `TestConcatWS`
rows: NULL propagation versus skipped NULL fields, retained empty fields,
numeric/decimal and rendered temporal values, separator coercion, binary bytes,
and arity are source-tested. Injected errors, packet-limit warnings,
function-class metadata, and vectorized execution remain outside the value
evaluator.

The result-ring transport now escapes valid UTF-8 cells containing CR/LF as
`BYTES_HEX:<UPPERCASE HEX>` on both the Go oracle and Rust renderer. This keeps
line framing lossless for binary/string builtins such as `FROM_BASE64` while
leaving ordinary text and existing marker escaping unchanged; the affected
base64 golden is regenerated from the rebuilt oracle.

The JSON checksum slice now owns the representable `TestJSONSumCrc32` rows:
homogeneous numeric/string arrays, empty arrays, IEEE CRC32 accumulation with
Go `%v` number spelling, NULL, root/type/mixed/nested/invalid errors, and arity.
Typed JSON array `FieldType` conversion, explicit path construction, warning
state, and vectorized execution remain partial boundaries.

The LEFT/RIGHT slice now ports the source `str_take` value contract: numeric
prefix count coercion, negative/zero/NULL handling, Unicode character slicing,
and raw-byte binary slicing are covered for both directions. FieldType
signature selection, warnings, injected errors, and vectorized execution stay
outside the value evaluator.

The FROM_DAYS slice now owns all integer and ETInt string-prefix source rows,
including leap/non-leap dates, zero-date fallback, valid bounds, the real
3,652,425–3,652,499 NULL band, and NULL input. The typed DATE result and
SQLMode/warning context remain explicit partial seams.

The JSON modification slice now owns the scalar value-domain rows for
`TestJSONSetInsertReplace`, `TestJSONMerge`, `TestJSONMergePreserve`, and
`TestJSONMergePatch`. Root/member insertion and replacement, sequential path
pairs, recursive RFC-7396 patch/deletion, scalar wrapping, invalid documents,
and NULL truncation are source-tested; typed BinaryJSON construction,
deprecation/session warnings, function-class metadata, and vectorized
execution remain explicit partial seams.

The JSON search slice now owns the representable `TestJSONSearch` rows:
one/all traversal, recursive and wildcard paths, SQL LIKE `%`/`_` matching,
custom escapes, path filtering, canonical result paths, NULL propagation, and
invalid mode/path/escape errors. Typed BinaryJSON/session warning and
vectorized behavior remain outside the seed evaluator.

The JSON depth/storage slice now owns `TestJSONStorageFree` and
`TestJSONStorageSize`, alongside the full scalar/container `TestJSONDepth`
shape table. It mirrors BinaryJSON depth and encoded-size accounting for
strings, scalars, arrays, and objects, while typed JSON construction,
warnings, and vectorized/function-class behavior remain partial.

The JSON constructor slice now owns the representable scalar rows for
`TestJSONArray` and `TestJSONObject`: zero/even/odd arity, numeric and string
values, NULL values, string keys, and NULL-key errors. Parser-originated
booleans and typed BinaryJSON arguments remain explicit value-domain seams.

The JSON Schema slice records `TestJSONSchemaValid` and
`TestJSONSchemaValidCache` as explicitly blocked. Their non-NULL rows require
the upstream JSON Schema validator, typed document/schema coercion, session
function-class cache, and failpoint lifecycle; the value-only evaluator does
not invent a hand-written subset or pretend cache semantics are covered. The
three directly representable NULL-propagation rows remain anchored in the
blocked boundary artifact.

The TO_DAYS/TO_SECONDS slice now owns the source numeric and string tables,
including two-digit years, year-zero January 1, strict malformed-time
rejection, and NULL results. Typed temporal conversion, SQLMode warning flags,
and session metadata remain partial.

The TIME_TO_SEC/SEC_TO_TIME slice now owns the exact scalar duration rows,
including delimited and compact inputs, signed values, integer and natural
float conversion, scientific-notation strings, overflow clamping, and NULL.
Expression-level decimal/FSP overrides and warning metadata remain explicit
typed boundaries.

The RIGHT/RPAD slice now owns character-valued RIGHT count coercion and
Unicode slicing plus RPAD truncation, repetition, empty-pad, NULL, negative,
binary-byte, and arity rows. Session FieldType/collation selection,
max_allowed_packet warnings, and vectorized chunk metadata remain partial
boundaries.

The CONCAT signature slice now owns the scalar `TestConcatSig` and
`TestConcatWSSig` rows through the shared value evaluator, including ASCII and
Unicode concatenation, separator coercion, NULL-skipping, and empty fields.
Chunk-column packet warnings, result FieldType metadata, and vectorized
execution remain partial boundaries.

The INSTR slice now owns all scalar source rows for 1-indexed substring
positions, numeric/string coercion, empty needles, Unicode text, binary bytes,
case-sensitive matches, and NULL propagation. Invalid-byte/session collation,
FieldType construction, warning, and vectorized paths remain outside the
value-only evaluator.

The QUARTER slice now owns the normal date boundaries, invalid month, NULL,
numeric coercion, and Go's IgnoreZeroInDate month-zero quarter result. Typed
temporal conversion, SQL-mode warning state, and session context remain
outside the value-only Datum boundary.

The ORD/BIN slice now owns the source numeric, Unicode, binary-byte, invalid,
empty, negative, NULL, and arity rows. BIN's contradictory upstream direct
empty-string expectation versus the SQL oracle's zero result is documented as
an evidence boundary rather than implemented as a special case.

The string replace/substring/trim slice now owns the source rows for
`TestReplace`, `TestSubstringIndex`, and `TestTrim`, including binary byte
preservation, repeated whole-token trimming, direction/NULL behavior, and
numeric-prefix count coercion. Charset/collation metadata, warning/session
state, and vectorized execution remain partial seams.

The date-difference slice now owns `TestDateDiff` and `TestTimeDiff`, including
zero-month/day `calcDaynr` behavior, fractional microseconds, mixed invalid
inputs, and Go's `TIME` maximum clamp. Typed temporal values, warning/session
state, and function-class/vectorized behavior remain outside the value-domain
port.

The timestamp-difference slice now owns the scalar `TestTimestampDiff` rows:
YEAR/MONTH/MINUTE/DAY units, fractional datetime parsing, invalid zero-date
NULLs, and NULL arguments. Typed temporal conversion, SQLMode warning flags,
and vectorized/function-class behavior remain explicit partial seams.

The case/compare string slice now owns `TestLower`, `TestUpper`, and
`TestStrcmp`, including Unicode case conversion, raw binary preservation,
numeric coercion, NULL propagation, and byte-wise comparison. Connection
charset/collation metadata, warning/session state, and vectorized execution
remain outside the seed evaluator.

The parser field-type slice now owns Go's terminal `BYTE`/`ASCII` string
options, NATIONAL/NCHAR aliases, compatibility aliases, UUID MariaDB gating,
and the bounded `TestType` field-type rows. The option parser returns at the
same terminal points as Go, so a following charset or second `BINARY` token is
not accidentally consumed; the declared `ddl_fieldtype_parser.go` owner
remains `partial` until its remaining sibling grammar is partitioned.

The parser column-options slice now adds typed secondary-engine-attribute
equals/no-equals and rejection rows plus source-order assertions for NULL,
AUTO_INCREMENT, COMMENT, COLLATE, COLUMN_FORMAT, and STORAGE. The checked
owner remains partial: table/partition/index secondary-engine attributes and
the remaining `parseColumnOptions` siblings still need their own exact source
domains.

The parser restore wave now owns three expression AST anchors from
`expressions_test.go` (unary operations, column names, and IS NULL predicates),
four SELECT/DML AST anchors from `dml_test.go` (LIMIT, wildcard fields, select
fields, and field lists), and four function anchors from `functions_test.go`
(function calls, casts, aggregates, and CONVERT). Their source-shaped Rust
tests add exact restore vectors, malformed-input boundaries, and parser-ring
corpora where the source family is independently replayable. The parser now
rejects four-component name paths at the same grammar boundary as Go. The
CONVERT source test remains `PARTIAL` only because Rust reports a structured
parse error instead of Go's numeric wrapper; valid charset canonicalization
and invalid charset rejection are covered. These are test/source ownership
claims, not execution or whole-parser parity.

The DML restore wave also owns five table/join anchors in `dml_test.go`: table
names, table-name index hints, table sources, ON conditions, and joins. Their
26 source vectors cover quoting, alias/index-hint canonicalization, derived
and UNION sources, join precedence, and USING/ON forms through the production
AST; the parser ring and nested Go baseline both pass.

The KV fault-injection slice now ports `pkg/kv/fault_injection.go` behind
narrow Rust storage/read traits. A shared, thread-safe configuration injects
get/batch-get and commit errors before delegation, `None` clears the error,
and begin/snapshot creation plus post-clear delegation preserve the source
contract. Full TiDB context/options, `ValueEntry`/map types, and the Go
Begin-on-error wrapper shape remain explicit partial boundaries.

The transaction retry slice now exposes the deterministic part of
`TestRetryExceedCountError`: retryable failures continue through the first
`MaxRetryCnt - 1` indexes and stop at the final index, while non-retryable
failures stop immediately. `RunInNewTxn` storage begin/rollback/commit,
failpoints, jittered sleep, and session error propagation remain partial until
the real client/session protocol is ported. `TestResourceGroupTagEncoding` now
has a dependency-closed protocol leaf: generated `tidb-proto`/prost preserves
the nullable=false `table_id` wire, `tidb-codec` decodes legacy table IDs, and
`tidb-txnkv` labels row/index/unknown keys while direct tests cover digest,
keyspace, and 510-byte vectors. Request-envelope extraction, API-V2 keyspace
prefixes, global next-generation kernel state, and the standalone
resourcegrouptag decoder remain explicit partial seams.

MPP version and exchange-compression tests remain intentionally blocked in
`tidb-txnkv`: `TestMppVersion` needs generated kvproto `mpp.TaskMeta` plus a
real `tidb-distsql` task serialization consumer, while
`TestExchangeCompressionMode` needs `tidb-session` variable parsing and the
generated tipb `CompressionMode` consumed by an ExchangeSender protobuf path.
No enum-only transaction facade was added.

Parser desugar gotcha: `INSERT(...)` parses as `Expr::Func{name:"INSERT_FUNC"}` and `CHAR(...)` as `CHAR_FUNC` (reserved-keyword renames). If a builtin "doesn't dispatch", inspect the parse tree before changing evaluation.

Update note (2026-07-16): integrated three bounded follow-on leaves without
sharing their implementation files. `tidb-protocol::ResultEncoder` now ports
the Go result-charset precedence decision, registered binary/ASCII/Latin1/
UTF-8 byte policy, and explicit unknown-charset/collation errors; GBK and full
session charset conversion remain open. `tidb-exec::result_field_resolver`
ports table-less expression display names, aliases, qualified names with
authoritative type hints, literal/operator/function type metadata, and
explicit wildcard/schema/type errors. `tidb-distsql::TransportRequest` makes
the post-build transport ownership boundary immutable and explicit: bind
returns a new snapshot, send-before-bind and repeated bind fail, and no fake
endpoint/protobuf/RPC state is introduced. Focused protocol, DistSQL, and
resolver tests pass; the current generated ledgers are 2,259 production
`UNTRIAGED`, 107 `PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and 16,128 test
`UNTRIAGED`, 303 `PARTIAL`, 142 `COVERED`, 12 `BLOCKED`, with 56 production
and 173 test evidence fragments. Automatic catalog-backed ResultField
derivation, GBK/full session conversion, temporal/JSON/enum/set/vector
formatting, TiKV/RPC, and deployable listener/auth/TLS/compression remain open.

Update note (2026-07-16): integrated the next connected server wave in three
non-overlapping leaves. `tidb-exec::result_response` parses a strict plain
table-less `SELECT`, resolves source-shaped fields, adapts them through
`col_names_to_result_fields`, and produces protocol `ColumnInfo`; explicit
FROM/WITH/TABLE/VALUES/set/non-query/wildcard boundaries remain errors.
`tidb-server::handshake` ports Go's initial-handshake byte order plus safe
HandshakeResponse41 header/body, auth/plugin/database/attribute parsing, and
capability intersection without claiming authentication. `tidb-server::listener`
owns real TCP bind, ephemeral address publication, idempotent initialization,
activation health ordering, shutdown flags, and idempotent close while leaving
Unix sockets, PROXY, accept loops, and bootstrap separate. Added focused
source-shaped tests and connected `Connection::dispatch_framed_auto`. The
full workspace test suite and strict Clippy pass. Generated ledgers are now
2,257 production `UNTRIAGED`, 109 `PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and
16,119 test `UNTRIAGED`, 312 `PARTIAL`, 142 `COVERED`, 12 `BLOCKED`, with 57
production and 174 test evidence fragments. Catalog-backed ResultField
binding, auth/TLS/compression, full session charset conversion, temporal/
JSON/enum/set/vector formatting, TiKV/RPC, accept-loop/bootstrap, and mixed
cluster routing remain open.

Update note (2026-07-16): integrated the next three source-owned lanes and
connected the catalog path. `tidb-protocol::ResultEncoder` now supports the
Go GBK/CP936 result bytes, euro/malformed-input replacement, GBK collation
advertisement, and session/column precedence while keeping the shared
datatype registry narrow. `tidb-distsql::select_iter` ports the serial
`SelectResult` owned-row/close contract and keeps raw/chunk/TiKV/sorted-heap
owners explicit. `tidb-exec::result_schema` binds single-table projections,
aliases, qualified names, and wildcards to stored `ColumnType` metadata;
`Connection::dispatch_framed_auto` now executes that catalog-backed path as
well as table-less SELECTs. The server regression creates a table, inserts a
row, derives metadata, and verifies the returned row. Focused protocol,
DistSQL, executor, and server tests pass. Current generated ledgers are
2,256 production `UNTRIAGED`, 110 `PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and
16,118 test `UNTRIAGED`, 313 `PARTIAL`, 142 `COVERED`, 12 `BLOCKED`, with 58
production and 175 test evidence fragments. Multi-relation schema binding,
full session charset lifecycle, temporal/JSON/enum/set/vector formatting,
response-channel decoding, TiKV/RPC, authentication/TLS/compression, and
deployable accept-loop/bootstrap remain open.

Update note (2026-07-16): added the next parallel source/test wave. The
DistSQL channel iterator ports `newSelRespChannelIter`/`Read` over owned rows,
including empty-chunk skipping, channel tagging, validation, close, and
explicit raw-tipb/chunk/TiKV boundaries; the existing serial iterator remains
separate. The server accept-loop leaf ports injected listener/handler
ownership, nil-listener and shutdown exits, listener/handler error
propagation, and a real localhost connection without pulling protocol/auth
into the loop. The executor statement-status leaf ports source counters,
warning order/cap, last-insert-ID explicit-set state, retry/full reset, and
DML/DDL/SELECT/session publish row-count policy. Focused tests and targeted
strict Clippy pass. Current ledgers are 2,255 production `UNTRIAGED`, 111
`PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and 16,114 test `UNTRIAGED`, 317
`PARTIAL`, 142 `COVERED`, 12 `BLOCKED`, with 59 production and 176 test
evidence fragments. Response-channel decoding, dynamic session/error-context
integration, multi-relation binding, and TiKV/RPC remain the next gates.

Update note (2026-07-16): integrated three bounded response/status protocol
leaves without editing the protected server/session roots. `tidb-distsql::response_channel`
now preserves ordered owned result/warning/error/close events, failed and
explicit-close lifecycle, idempotence, and explicit raw-tipb/TiKV boundaries.
`tidb-protocol::error_packet` ports `clientConn.writeError`'s protocol-41 and
legacy ERR payload ordering byte-for-byte while leaving error conversion,
framing, and flush to callers. `tidb-exec::status_result` converts an already
published statement status losslessly into `OkPacket` and `ResultSetOptions`
without inspecting runtime Datum values. Focused tests, the full workspace
test suite, strict Clippy, parser/inventory checks, ledger checks, formatting,
and diff checks pass. Counts remain 2,255/111/24/0 production and
16,114/317/142/12 test obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED), with
59 production and 176 test evidence fragments. Raw tipb/chunk decoding,
dynamic session/error-context/wire integration, multi-relation binding,
authentication/TLS/compression, TiKV/RPC, and deployable bootstrap remain
explicit next gates.

Update note (2026-07-16): completed the next batched source/schema/status
integration. `tidb-proto` now owns the descriptor-checked SelectResponse and
StreamResponse wire projection; `tidb-exec::result_schema_multi` binds
bounded inner/CROSS/comma/LEFT relation trees and prepared self-join metadata;
and `Session::run` publishes final statement status on success/error and
retry. A source audit corrected ordinary session-command `ROW_COUNT()` to
zero (only the separate SET SESSION STATES contract retains a prior value).
Final workspace tests and strict Clippy pass. Current ledgers are
2,255/111/24/0 production and 16,113/318/142/12 test obligations, with
59 production and 177 test evidence fragments. Raw tipb/chunk decoding,
planner-owned ON/USING semantics, full error-context/wire attachment,
authentication/TLS/compression, TiKV/RPC, and deployable bootstrap remain
open.

Update note (2026-07-16): integrated the next raw-wire/error/planner batch in
one workspace validation cycle. `tidb-distsql::chunk_decode` now decodes the
tipb `SelectResponse` envelope and validates `Chunk.rows_meta` byte ranges,
while leaving default/columnar/CHBlock typed codecs and intermediate-output
routing explicit. `tidb-protocol::error_conversion` owns the source-backed
typed error-kind to MySQL errno/SQLSTATE table and preserves message bytes;
session/executor context adaptation and writes remain outside it.
`tidb-exec::result_schema_join_output` ports planner-visible INNER/CROSS/LEFT
field order, LEFT-side nullability, and USING coalescing metadata for resolved
child schemas, while ON/USING predicate evaluation and row null-extension stay
explicit gaps. The batch caught and corrected the metadata-free opaque chunk
case, plus a missing type annotation and public-field documentation under
strict workspace lints. Final workspace tests and strict Clippy pass; static
ledger/parser/plan checks pass. Current ledgers are 2,255/111/24/0 production
and 16,112/319/142/12 test/support obligations, with 59 production and 177
test evidence fragments. Typed chunk codecs, full error-context/wire
attachment, join execution, authentication/TLS/compression, TiKV/RPC, and
deployable bootstrap remain open.

Update note (2026-07-16): extended the connected response seam in parallel.
`tidb-distsql::stream_decode` now preserves raw StreamResponse payload,
warnings, output counts, warning-count/NDV presence, and malformed-protobuf
errors without decoding nested chunks. `tidb-exec::error_conversion` maps
already-rendered ExecError categories to the protocol ErrorDescriptor without
guessing messages or errno classes. The automatic catalog result path now
proves bare-wildcard INNER/CROSS/LEFT/USING metadata and rows end to end,
including LEFT null extension and USING coalesced order; explicit projections
remain outside the planner-output contract. `tidb-codec::value` validates
default-row tagged boundaries, and `tidb-server::error_response` attaches
caller-rendered errors to sequence-one protocol-41 or legacy ERR frames. The
regenerated ledgers are 2,255/111/24/0 production and 16,110/321/142/12
test/support obligations. The typed columnar/CHBlock codecs, general join
typing/projection names,
session error-context/wire lifecycle, authentication/TLS/compression, TiKV/RPC,
and deployable bootstrap remain open.

Update note (2026-07-16): the final wave for this turn passed one batched
workspace test/Clippy gate. It added raw default-row value framing, automatic
bare-wildcard LEFT/USING output integration, and sequence-one rendered-error
ERR framing. The catalog seed still does not retain CREATE TABLE NOT NULL
declarations, so nullable flags are source-shaped only for planner-provided
fields; typed codecs and full session/error context remain open.

Update note (2026-07-16): the next parallel wave also passed one batched
workspace test/Clippy gate. `tidb-codec::column` and `tidb-distsql` now carry
raw fixed/variable TypeChunk framing, null bitmaps, offsets, default-row values,
and exact remainders without guessing FieldType/Datum or native CHBlock
semantics. `tidb-exec::result_schema_projection` ports direct-column,
wildcard, alias, nullability, and hidden-USING rejection metadata over resolved
join output; the automatic catalog path now routes direct columns, aliases, and
wildcards through the existing `Database::project_row` owner while preserving
explicit typed-expression and FullSchema boundaries. `RenderedExecError` carries caller-rendered bytes
plus an optional exact published status snapshot into `tidb-server` ERR framing
without copying warnings into the wire payload. Current ledgers are
2,252/114/24/0 production and 16,103/328/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED), with the new partial evidence included.
Typed Datum/CHBlock semantics, typed expression/FullSchema projection mappings,
general ON/USING typing, full ErrCtx/session ownership, authentication/TLS/
compression, TiKV/RPC, and deployable bootstrap remain open.

Update note (2026-07-16): integrated the next parallel source-first wave and
closed the workspace gate. `tidb-codec::column` now derives Go `getFixedLen`
physical layouts from `FieldType` (FLOAT=4, scalar/time=8, NEWDECIMAL=40,
otherwise variable); `tidb-distsql::KvRequestBuilder` preserves opaque
`Request.Data` bytes plus ordered TiFlash partition IDs/ranges before any
protobuf, region, or RPC owner exists; and the automatic catalog path routes
direct columns, aliases, and qualified/bare wildcards through the existing
`Database::project_row` owner. Hidden right-side USING provenance is retained
so the missing FullSchema mapping fails explicitly instead of becoming an
unknown qualifier. Full workspace tests and strict Clippy pass, all static
ledger/parser/plan/dependency gates pass, and current ledgers are
2,252/114/24/0 production and 16,103/328/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Typed Datum/CHBlock codecs, typed
expression/FullSchema projection mappings, general ON/USING typing, full
ErrCtx/session ownership, authentication/TLS/compression, TiKV/RPC, and
deployable bootstrap remain open.

Update note (2026-07-16): integrated the next typed execution/context wave in
one batched workspace cycle. `tidb-codec::decode_column_datums` now converts
the source-proven native scalar subset (signed/unsigned 64-bit, float32/64,
and byte-preserving variable strings) while retaining nulls/remainders and
rejecting temporal, decimal, JSON, enum/set, vector, bit, and unknown types
explicitly. `tidb-exec::join_predicate` binds only direct cross-side equality
for ON/USING, shares NULL-as-non-match semantics, and leaves compound or
ambiguous predicates to the general evaluator. Statement status now records
`exec_success`; `Session::render_exec_error` and the server framing path attach
that exact failed status without copying warnings into ERR payloads. Full
workspace tests and strict Clippy pass, all static ledger/parser/plan/
dependency gates pass, and current ledgers are 2,251/115/24/0 production and
16,103/328/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Temporal/decimal/JSON/vector Datum
codecs, native CHBlock, general ON/USING typing and join algorithms, dynamic
warning/error context, authentication/TLS/compression, TiKV/RPC, and
deployable bootstrap remain open.

Update note (2026-07-16): integrated the next three source-backed leaves in
one batched workspace cycle. `tidb-datatype::PackedTime` and
`tidb-codec::temporal` preserve Go's packed temporal integer layout and
big-endian codec boundary without claiming SQL calendar/FSP/timezone or
Duration semantics. `tidb-proto` now owns the exact CoprocessorRequest field
projection, and `tidb-distsql::CoprocessorRequestEnvelope` preserves opaque
request bytes, context, and ordered partition ranges before region splitting
or RPC. `tidb-server::AuthHandshake` makes the initial response, SSLRequest,
TLS-established, and authentication-pending phases explicit, retaining raw
auth bytes and classifying plugin fallback/switch/defer without performing TLS,
password verification, or user lookup. The full workspace test batch passed;
strict Clippy passed after boxing the large pending-auth phase payload, and
formatting/diff plus static ledger/parser/plan/dependency gates pass. Current
ledgers are 2,248/118/24/0 production and 16,098/333/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Temporal SQL/Duration,
decimal/JSON/enum/set/vector Datum and native CHBlock codecs, typed
expression/FullSchema projection, general planner ON/USING typing, full
session/error-context and authentication/TLS/user-store lifecycle, region/RPC,
TiKV, and deployable bootstrap remain open.

Update note (2026-07-16): integrated the next parallel codec/transport/planner
wave and closed one combined workspace gate. `tidb-codec::json` now ports the
source-defined BinaryJSON type/value boundary, including primitive, container,
opaque, and duration payload lengths with exact remainders; `RawValue::json`
now accepts the JSON value tag while malformed/unknown physical payloads stay
explicit errors. `tidb-proto` and `tidb-distsql::RegionTaskEnvelope` preserve
the exact StoreBatchTask region epoch, peer, ordered ranges, task ID, versioned
ranges, and bucket-version fields before lookup/retry/endpoint/RPC ownership.
`tidb-exec::result_schema_join_output` now retains source-ordered FullSchema
fields and maps hidden right-side USING fields to canonical visible output
indices without widening executor rows. Full workspace tests and strict
Clippy pass after fixing borrowed-JSON lifetimes and source-test cardinality;
formatting/diff plus all ledger/parser/plan/dependency gates pass. Current
ledgers are 2,248/118/24/0 production and 16,098/333/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Full JSON semantics, SQL
temporal/Duration, decimal/enum/set/vector Datum and native CHBlock codecs,
typed expressions/nested FullSchema execution, general ON/USING typing, full
session/error-context and authentication/TLS/user-store lifecycle, region/RPC
execution, TiKV, and deployable bootstrap remain open.

Update note (2026-07-16): integrated the next three source-backed leaves in
one static-first parallel wave. `tidb-server::AuthExchange` now preserves
source-shaped `AuthSwitchRequest`/`AuthMoreData` payloads, sequence framing,
and explicit malformed/trailing-byte errors without claiming password,
plugin-registry, TLS, or transport-flush behavior. `tidb-distsql::RawChBlockChunk`
validates the native CHBlock envelope and borrowed payload/row metadata while
leaving typed ClickHouse Datum decoding explicitly unsupported. The planner
owns a narrow `join_condition` classifier for qualified/unqualified cross-side
`=`/`<=>` ON predicates plus USING binding and explicit ambiguity/unsupported
outcomes; it is not the general join executor. The workspace test/Clippy gate
and static ledger/parser/plan/dependency checks pass. Current ledgers are
2,245/121/24/0 production and 16,095/336/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps are plugin authentication
and password/TLS transport, typed CHBlock/temporal/decimal/JSON/enum/set/vector
Datum semantics, full planner residual condition execution and nested
FullSchema propagation, TiKV/MPP RPC, session/bootstrap, and real-cluster
validation.

Update note (2026-07-16): integrated the next three static-first parity
leaves. `tidb-server::AuthPluginRegistry` now mirrors Go's custom-plugin
metadata validation order (empty/duplicate/reserved names and required
callbacks) and selects LDAP or `RequiredClientSidePlugin` client names without
executing callbacks, hashing passwords, or doing TLS/I/O. `RawValue::decode_datum`
and the DistSQL default-row consumer now decode the source-proven scalar tag
subset with exact payload consumption; duration, JSON, vector, enum/set, and
schema-aware temporal conversion remain explicit errors. `tidb-planner::residual_condition`
retains residual AND/OR/NOT shape and syntax-only scalar/function metadata with
deferred typed evaluation rather than guessing a value or hash key. The
workspace tests pass for all crates; strict Clippy, formatting/diff, and the
static ledger/parser/plan/dependency gates pass after correcting evidence
ownership overlaps. Current ledgers are 2,242/124/24/0 production and
16,091/340/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps are callback execution,
password verification/user store/TLS transport, typed temporal/Duration,
JSON/enum/set/vector/CHBlock semantics, general residual predicate typing and
join execution, FullSchema propagation, TiKV/MPP RPC, session/bootstrap, and
real-cluster validation.

Update note (2026-07-16): integrated the next three static-first boundaries.
`tidb-server::SecureTransportPolicy` now mirrors Go's `RequireSecureTransport`
admission decision: plaintext TCP is rejected when enabled, while Unix sockets
and transport-owned direct/gateway-secure assertions are allowed; the leaf does
not perform TLS, certificate, gateway-attribute, or password validation.
`tidb-codec::RawDuration` preserves the signed nanosecond `EncodeInt` payload
and DecodeOne's `MaxFsp=6` result with exact remainders, while SQL range/FSP and
warning policy remain typed-time work. `tidb-planner::condition_binding`
resolves known residual column paths into source-ordered `FullSchema` indices
and marks IN/CASE/subquery and other dedicated shapes opaque for a future typed
executor. The full workspace test batch and strict Clippy pass; static
ledger/parser/plan/dependency checks pass after regenerating the inventories.
Current ledgers are 2,240/126/24/0 production and 16,088/343/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps
are TLS handshake/certificates, password verification/user store, SQL duration
semantics, decimal/JSON/enum/set/vector/CHBlock codecs, typed residual
evaluation and join execution, full session/error context, TiKV/MPP RPC,
bootstrap, and real-cluster validation.

Update note (2026-07-16): integrated the next three source-backed leaves in
one batched workspace cycle. `tidb-server::AuthChallenge` and
`AuthSessionAttempt` now preserve the session-facing identity/plugin/auth-byte/
salt envelope, enforce the Go `auth_socket` Unix-only precondition, and stop at
`PendingVerification` or an explicit pre-verification rejection; privilege
lookup, password/plugin callbacks, account locking, and authenticated-session
publication remain outside the boundary. `tidb-codec::DecimalWireMetadata`
inspects the exact precision/scale header, packed payload length, and remainder
without materializing or rounding the coefficient; short buffers now return a
typed framing error. `tidb-planner::predicate_partition` routes bound residual
predicates conservatively to left/right/join/deferred candidates and requires a
typed effects check for functions or opaque AST shapes, without selecting a
join algorithm or pushing values. Workspace tests and strict Clippy pass after
fixing the owned-auth const transition and decimal test/framing edges; static
ledger/parser/plan/dependency checks pass. Current ledgers are
2,238/128/24/0 production and 16,085/346/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps are password verification,
TLS/certificates, SQL duration/decimal/JSON/enum/set/vector/CHBlock semantics,
typed residual evaluation, join algorithms, full session/error context,
TiKV/MPP RPC, bootstrap, and real-cluster validation.

Update note (2026-07-16): opened the next three static-first rings in parallel.
`AuthSessionAttempt::begin_with_policy` now composes secure-transport admission
before the Unix-only `auth_socket` rule and preserves the same
`PendingVerification` boundary; TLS, user lookup, plugin/password callbacks,
account locking, and authenticated-session publication remain external.
`tidb-codec::RawJsonTemporal` preserves Go BinaryJSON DATE/DATETIME/TIMESTAMP
type codes and little-endian packed calendar bits with exact remainders;
calendar/FSP/timezone conversion and SQL duration semantics remain typed work.
`tidb-planner::typed_condition` now carries an explicit child/join/outer-match
evaluation mode, source `FullSchema` width, and TRUE-only versus UNKNOWN-
tracking truth policy without evaluating a Datum or materializing a row. The
combined workspace and strict Clippy gate passes after ledger regeneration. Current ledgers
are 2,237/129/24/0 production and 16,084/347/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); downstream typed evaluators, join
algorithms, authentication/TLS/user store, session/bootstrap, TiKV/MPP RPC,
and real-cluster validation remain open.

Update note (2026-07-16): the next semantic ring is integrated and validated.
`tidb-server::IdentityLookupRequest`/`IdentityLookupResult` now preserve the
Go `MatchIdentity` pre-auth request, canonical matched row, and explicit
NotFound outcome without wildcard matching, privilege access, or treating a
match as authenticated. `tidb-codec::RawDuration::parts` mirrors Go's
`splitDuration` sign/hour/minute/second/microsecond decomposition, including
sub-microsecond truncation while leaving SQL TIME range/rounding/warning policy
to typed session owners. `tidb-exec::evaluate_typed_condition` is the first
real consumer of planner requests: it validates FullSchema width and returns
TRUE/FALSE/UNKNOWN from existing scalar `Datum` values; vectorized filtering,
outer-row status mutation, row materialization, and join execution remain
open. The workspace/Clippy/static gates pass after regenerating the ledgers.
Current ledgers are 2,235/131/24/0 production and 16,081/350/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

Update note (2026-07-16): the next semantic ring is integrated and validated.
`tidb-server::IdentityCatalog` now ports Go MatchIdentity host ordering,
percent/underscore/escape matching, localhost loopback, IPv4 network patterns,
and caller-injected reverse-DNS fallback while keeping privilege storage, DNS
I/O, password verification, and authenticated-session publication external.
`tidb-datatype::truncate_overflow_mysql_time` clamps to MySQL TIME endpoints and
returns typed positive/negative overflow without constructing warnings or
session errors. `evaluate_typed_condition_batch` now returns disjoint
row-indexed TRUE and UNKNOWN masks over the existing scalar Datum evaluator and
reports indexed width/evaluation failures; selection reuse, vectorized
expression execution, outer-row mutation, null extension, and join materialization
remain open. The workspace, strict Clippy, ledger, parser, plan, dependency,
formatting, and diff gates pass. Current ledgers are 2,233/133/24/0 production
and 16,079/352/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

Update note (2026-07-16): Wave-26 adds the next source-backed semantic layer.
`IdentityLookupPolicy` and `IdentityLookupResult::Bypassed` preserve the Go
`SkipWithGrant` early admission without confusing it with a canonical matched
privilege row or authenticated session. `tidb-datatype::round_duration_fsp`
normalizes FSP and performs half-away-from-zero rounding, including negative
values and second carry, with typed invalid-FSP/overflow errors. The executor
now exposes `OuterRowStatus::{Unmatched, Matched, HasNull}` and a pure
`transition_outer_row_status` over batch masks, matching
`filterAndCheckOuterRowStatus` while leaving cumulative merge, selection/chunk
lifecycle, null extension, and physical joins open. The full workspace,
strict Clippy, static ledgers, parser/plan/dependency, formatting, and diff
gates pass. Current ledgers are 2,232/134/24/0 production and
16,076/355/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

Update note (2026-07-16): Wave-27 extends the three parallel semantic rings.
`PrivilegeRowAdmission` now mirrors `ConnectionVerification`'s exact
canonical username/host row check without wildcard rematching or password
claims. `tidb-datatype::parse_duration` covers signed `HH:MM[:SS]`,
day-prefixed forms, fractions, carry, FSP normalization, and MySQL TIME
endpoint clamping with typed overflow; compact numeric literals,
date/datetime fallback, Unicode trimming, and warning/session policy remain
open. `merge_outer_row_status` preserves cumulative join precedence across
batches (`Matched` > `HasNull` > `Unmatched`) without owning chunks, rows, null
extension, or physical join execution. Static ledger checks pass with
2,230/136/24/0 production and 16,073/358/142/12 test/support obligations.

Update note (2026-07-16): Wave-28 continues the same bounded seams. The
server identity leaf now exposes `AuthPluginHandoff` metadata from an exact
privilege row and the source `SkipWithGrant` native-plugin default without
validating plugins, hashing passwords, or publishing an authenticated
session. `parse_duration` now also mirrors Go compact `HHMMSS` forms,
including short and leading-zero forms, while date/datetime fallback,
Unicode trimming, and SQL warning/session policy remain open.
`select_outer_row_statuses` preserves source-order selected indexes aligned
with TRUE statuses while retaining the full status slice for FALSE/UNKNOWN
outer-row handling; it does not copy rows or own chunks. Static ledger checks
pass with 2,229/137/24/0 production and 16,071/360/142/12 test/support
obligations.

Update note (2026-07-16): Wave-29 closes the next metadata-only decisions.
`AuthPluginRegistry::admit` classifies built-in, validated custom, and
unsupported plugin names without invoking callbacks. The duration owner now
returns a typed `DurationDateTimeFallback` shape for Go's compact-12,
compact-14, and separated date/time routing while leaving calendar conversion
and session warnings to a higher owner. `finalize_outer_row_statuses` emits
source-order events only for `Unmatched`/`HasNull` rows, exposing default-inner
and UNKNOWN signals without looking up rows or materializing null extension.
Static ledger checks pass with 2,228/138/24/0 production and
16,070/361/142/12 test/support obligations.

Update note (2026-07-16): Wave-30 completes another metadata/lifecycle ring.
`AuthPluginRegistry::select_client_plugin` mirrors Go's session-token
passthrough, native fallback (including legacy clients), auth-token clear-
password mapping, custom/LDAP switch selection, and explicit unsupported
outcomes without packet I/O or verification. `DurationParseEvent` classifies
overflow, datetime fallback, and truncation without warning text or context
mutation. `PredicateBatchBuffer` resets/reuses TRUE/UNKNOWN slices with strict
length alignment while never copying rows or running vectorized kernels.
The consolidated workspace tests and strict Clippy pass; ledgers remain
2,227/139/24/0 production and 16,069/362/142/12 test/support obligations.

Update note (2026-07-16): Wave-31 adds `AuthTokenCheck`/
`AuthTokenRetryState`/`AuthTokenJwksState` for the exact three-segment JWT
boundary, initial-attempt plus retry ordering, JWKS reload-after-verification-
failure, and explicit missing/load-error outcomes. Parser `FieldType` now owns
raw flag masks, immutable bit operations, length/decimal defaults (including
CAST), DECIMAL validity, and variable-length predicates. `cardinality::pseudo`
now owns the source rates and bounded equality/less/between,
signed/unsigned/scalar, and prefix-index range arithmetic. RSA/JWK/filesystem/
network/claims authentication, full FieldType formatting/enum/set metadata, and
session/statistics/catalog planner integration remain open. Static evidence and
formatting checks pass; current ledgers are 2,225/141/24/0 production and
16,065/366/142/12 test/support obligations.

Update note (2026-07-16): Wave-32 adds `tidb-codec::RowLayout` for new-row
headers, small/large ID and offset metadata, sorted lookup/null-default
decisions, value ranges, and checksum-trailer framing; pure ErrCtx group
levels/flags with ignore-over-warning precedence and Go statement defaults; and
deterministic DDL affinity-level normalization, stable group IDs, duplicate
partition collapse, missing-partition validation, and pre-commit level
rejection. Typed row encoding/decoding, schema/handles/checksum calculation,
warning sinks/session wiring, TiKV/PD/catalog mutation, and DDL coordination
remain open. Current ledgers are 2,221/145/24/0 production and
16,057/374/142/12 test/support obligations.

Update note (2026-07-16): Wave-33 adds three bounded source-owned leaves in
parallel. `tidb-codec::row_encoder` now frames opaque row payloads with sorted
non-null/null IDs, small/large metadata selection, compact little-endian
integer widths, and append-buffer behavior. `tidb-server::bootstrap` now owns
bootstrap/upgrade/normal mode selection, SYSTEM-keyspace guards, feature-gate
outcomes, and the source phase-order contract, with bootstrap/upgrade finishing
before outer global/session initialization. `tidb-planner::cardinality::row_count_column`
ports normalized-range arithmetic and partial-index selectivity over
caller-owned estimates. Typed Datum/schema/decoder/checksum behavior, KV/domain/
DDL side effects, histogram/TopN/statistics context, and deployable bootstrap
remain open; regenerate ledgers and run the consolidated workspace gate before
claiming this wave validated. The completed static snapshot is
2,218/148/24/0 production and 16,049/382/142/12 test/support obligations.

Update note (2026-07-16): Wave-34 adds three parallel bounded leaves. The
transaction lane ports `tidb-txnkv::TxnScopeVar`'s exact global/local scope
metadata without configuration, PD/oracle, or session propagation. The
session lane adds a borrowed `WarningPublication` view over ordered statement
warnings with protocol-sized total/error counts, leaving mutable warning
handlers, IgnoreWarn/JSON, and session/error rendering open. The DDL lane
ports case-insensitive partition name validation, ADD/REORGANIZE collision
checks, ordered lookup/IDs, staged/published definitions, and ADD phase order
`Initial -> ReplicaOnly -> Public`; expressions, physical IDs, KV/PD/catalog,
and workers remain external. Current ledgers are 2,215/151/24/0 production and
16,045/386/142/12 test/support obligations.

Update note (2026-07-16): Wave-35 adds raw rowcodec decoder metadata over
`RowLayout` (borrowed not-null/null/missing lookup, value boundaries, compact
integer decoding, and typed malformed-layout errors), complete isolation-level
enum/normalization and `tx_isolation_one_shot` state transitions, and safe
unistore MVCC metadata for write/lock records, user timestamps, extra status
keys, and descending timestamp suffixes. Typed Datum/schema/default handling,
live session/sysvar/transaction behavior, storage/RPC/oracle/lock resolution,
and commit protocol remain external. Current ledgers are 2,208/158/24/0
production and 16,040/391/142/12 test/support obligations.

Update note (2026-07-16): Wave-36 adds three source-owned leaves in parallel.
`tidb-stats::cmsketch` ports zero-seed Murmur3, CMSketch bucket/counter/query
geometry, equal-shape merge, and sorted encoded-byte TopN lookup without Datum,
analyze, histogram, persistence, or statistics-handle ownership.
`tidb-exec::nontransactional` ports the session admission gates and admitted
DML-family policy while leaving AST/shard/worker/error aggregation external.
`tidb-planner::range_detacher` ports normalized CNF/DNF access/filter
reconstruction with caller-owned checker decisions; expression typing,
collation/session checks, range endpoints, and the full ranger checker remain
open. The regenerated snapshot is 2,205/161/24/0 production and
16,032/399/142/12 test/support obligations. `cargo test --workspace` and
strict Clippy pass with 12 jobs; the original Go suite and deployable server
remain unverified and incomplete.

Update note (2026-07-16): Wave-37 adds three additional bounded leaves.
`tidb-stats::fmsketch` ports the already-hashed FM sketch mask/hash-set
admission, level transition, NDV, merge, copy, and memory contracts;
Datum/tablecodec hashing and protobuf/persistence remain external.
`tidb-exec::txn_read_ts` ports `tx_read_ts` consume/peek/set and used-plus-
nonzero cleanup semantics without timestamp-oracle or stale-read execution.
`tidb-planner::selectivity_greedy` ports `GetUsableSetsByGreedy` mask
traversal, source type/ID ordering, non-overlap selection, and all six
tie-breaks over caller-owned metadata. The regenerated snapshot is
2,203/163/24/0 production and 16,028/403/142/12 test/support obligations.
Workspace tests, strict Clippy, formatting, ledgers, parser, and plan gates
pass with 12 jobs; original Go parity and deployable server remain open.

Update note (2026-07-16): Wave-38 adds three source-backed leaves in parallel.
`tidb-stats::status::StatsLoadedStatus` ports the zero-value, loaded/evicted
constructors, copy semantics, and exact integer predicate ordering from the
statistics loading metadata. `tidb-planner::cost_factors` ports the source
selection/distinct/tolerance thresholds, small-scan threshold, all sixteen
aggregate-factor entries including `default`, and unknown-name fallback.
`tidb-exec::retry_info` ports deterministic retry metadata queues, replay
offsets, dropped prepared-statement cleanup, and lifecycle fields without
owning retry orchestration. The regenerated snapshot is 2,201/165/24/0
production and 16,025/406/142/12 test/support obligations. Workspace tests,
strict Clippy, formatting, ledgers, parser, plan, and dependency gates pass
with 12 jobs; original Go parity, full session/statistics integration, and a
deployable server remain open.

Update note (2026-07-16): Wave-39 adds three small source-owned leaves in
parallel. `tidb-stats::constants` ports the exported TopN and histogram default
values without applying configuration. `tidb-planner::cardinality::index_range_policy`
ports the full-range including-NULLs predicate and non-partial/non-MV gates over
normalized endpoint metadata, leaving estimation and statistics loading open.
`tidb-exec::reserved_row_id` ports the complete statement-context reservation
counter and exact exhaustion behavior without storage reservation or table
mutation. The regenerated snapshot is 2,199/167/24/0 production and
16,022/409/142/12 test/support obligations. Workspace tests, strict Clippy,
formatting, ledgers, parser, plan, and dependency gates pass with 12 jobs;
original Go parity and deployable server remain incomplete.

Update note (2026-07-16): Wave-40 adds three additional source-backed leaves
in parallel. `tidb-stats::status::StatsLoadedStatus::status_to_string` ports
the exact `unInitialized`/`allLoaded`/`allEvicted`/`unknown` labels and
uninitialized precedence. `tidb-planner::cardinality::cross_estimation` ports
the pure expected-count range conversion over normalized opaque endpoints,
including ascending/descending selection, unbounded/full-scan sentinels, and
endpoint-exclusion inversion. `tidb-exec::sequence_state` ports numeric latest
sequence-value update, lookup, snapshot-copy, and `maps.Copy`-style merge
semantics. Statistics handles, Datum/ranger construction, SQL sequence
execution/allocation, live session ownership, original Go parity, and a
deployable bootstrap remain open. The regenerated ledgers are
2,195/171/24/0 production and 16,020/411/142/12 test/support obligations;
the consolidated workspace/Clippy gate is the next verification step.

Update note (2026-07-16): Wave-41 adds three more disjoint source-backed
leaves. `tidb-stats::AnalyzeTableId` ports table/partition statistics-ID
selection, the non-partition sentinel, source formatting, and optional-value
equality. `tidb-planner::cardinality::out_of_range` ports
`outOfRangeEQSelectivity` and `outOfRangeFullNDV` arithmetic, including
modification/deletion fallback, zero-NDV derivation, smoothing, minimum-row,
and floating-point boundaries. `tidb-exec::session_status` ports the atomic
SessionVars status bitfield, any-bit reads, set/clear operations, protocol
readback, and default autocommit/transaction/cursor masks. Analyze lifecycle,
histogram/TopN/session integration, transaction/cursor ownership, original Go
parity, and deployable bootstrap remain open. The regenerated ledgers are
2,193/173/24/0 production and 16,016/415/142/12 test/support obligations;
the consolidated workspace/Clippy gate is pending.

Update note (2026-07-16): Wave-42 adds three source-backed leaves. The
statistics workstream now owns `RowEstimate` construction, field-wise
arithmetic, clamp ordering, and skew-ratio bounds. The planner's
`cardinality::uniform` leaf ports normalized `estimateRowCountWithUniformDistribution`
arithmetic, including TopN/empty-histogram fallback, modification/deletion
derivation, and `RiskEqSkewRatio` interpolation. The executor's
`removed_sysvar` leaf ports the complete 13-entry removed-system-variable
registry with exact case-sensitive reason lookup. Histogram/TopN/context
integration, sysvar dispatch/error rendering, original Go parity, and a
deployable bootstrap remain open. The regenerated ledgers are
2,192/174/24/0 production and 16,013/418/142/12 test/support obligations;
the consolidated workspace/Clippy gate is the next step.

Update note (2026-07-16): Wave-43 adds three source-backed leaves in parallel.
`tidb-planner::schema_table_key` owns lowercase schema/table identity and
qualified-versus-bare alias keys with map-safe equality/hash. The statistics
workstream adds `tidb-stats::stats_version` for the exact version constants and
analyzed/synthesized metadata predicates. The session workstream adds
`tidb-exec::option_values` for ON/1 and ON/OFF/true/false compatibility text
conversion. Parser/CTE/view scope, statistics persistence/handles, live
SessionVars validation and warning plumbing, original Go parity, and a
deployable bootstrap remain open. The regenerated ledgers are
2,191/175/24/0 production and 16,009/422/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The consolidated workspace/Clippy and
parser/plan/dependency gates are the next verification step.

Update note (2026-07-16): Wave-44 adds three disjoint source-backed leaves.
`tidb-planner::implementation_cost` ports the dependency-closed base
implementation cost arithmetic. `tidb-stats::ColAndIdxExistenceMap` preserves
known/analyzed column and index metadata, clone/equality, and replacement or
deletion semantics. `tidb-exec::statement_pushdown` composes the exact TiKV
push-down flag bits from type flags, error levels, statement kind, LOAD DATA,
and restricted SQL. Physical-plan/memo, stats handle/DDL loading, live
StatementContext, request construction, original Go parity, and a deployable
bootstrap remain open. The regenerated ledgers are
2,189/177/24/0 production and 16,005/426/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The consolidated workspace/Clippy and
parser/plan/dependency gates are the next verification step.

Update note (2026-07-16): Wave-45 adds three source-backed leaves in parallel.
`tidb-stats::scalar_geometry` ports interval fraction, common-prefix, and
left-aligned byte-scalar arithmetic. `tidb-planner::task_type` preserves the
four known task kinds, exact labels, and unknown raw values. `tidb-exec::context_id`
ports atomic non-zero monotonic statement-context ID generation. Datum and
histogram conversion, physical-property scheduling, live StatementContext
construction/reset, original Go parity, and a deployable bootstrap remain open.
The regenerated ledgers are 2,186/180/24/0 production and
16,002/429/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
The consolidated workspace/Clippy and parser/plan/dependency gates are the
next verification step.

Update note (2026-07-16): Wave-47 adds three bounded source-owned leaves.
`tidb-planner::physical_property` ports MPP partition-type classification,
exchange mapping, unknown fallback, and matched-result metadata.
`tidb-stats::overlap_geometry` ports left/right out-of-range overlap geometry
and squared-width normalization. `tidb-exec::used_stats` ports deterministic
slow-log statistics formatting across pseudo/real versions, row counts, and
sorted ID fallback names. Physical-plan matching, histogram/Datum/skew
integration, TableInfo resolution, live session collection, original Go parity,
and a deployable bootstrap remain open. The regenerated ledgers are
2,184/182/24/0 production and 15,995/436/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The consolidated workspace/Clippy and
parser/plan/dependency gates are the next verification step.

Update note (2026-07-16): Wave-46 adds three bounded source-owned leaves.
`tidb-planner::by_item` ports ORDER BY direction/identity/formatting metadata
over opaque expression text. `tidb-stats::memory_usage` ports measured
column/index memory totals and tracking-cost arithmetic, preserving the FMS
exclusion. `tidb-exec::statement_refcount` ports atomic frozen/no-reference
and reference-count transitions; its Go source has no dedicated transition
test, so the Rust contract tests are explicitly supplemental. Expression and
physical-sort integration, table/cache aggregation, cached StatementContext
reuse, original Go parity, and a deployable bootstrap remain open. The
regenerated ledgers are 2,185/181/24/0 production and
15,997/434/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
The consolidated workspace/Clippy and parser/plan/dependency gates are the
next verification step.

Update note (2026-07-16): Wave-48 adds three bounded source-owned leaves.
`tidb-planner::stats_info` ports row-count truncation and caller-owned NDV
capping; `tidb-stats::HistogramCountSummary` ports non-null/total counts,
null addition, realtime-row difference, and the zero-total factor boundary;
and `tidb-exec::plan_cache_params` ports ordered append/reset, indexed/borrowed
access, and the non-prepared-cache privacy bit. Catalog/planner statistics,
histogram mutation/loading, prepared-plan evaluation, live SessionVars, full
Go parity, and a deployable bootstrap remain open. The regenerated ledgers are
2,183/183/24/0 production and 15,992/439/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The consolidated workspace/Clippy gate
passed with 12 jobs; the static parser/plan/dependency gate is the next step.

Update note (2026-07-16): Wave-49 adds three bounded source-owned leaves.
`tidb-planner::index_columns` ports normalized index-column projection and
prefix metadata; `tidb-stats::analysis_policy` ports analyzed/minimum-count/
pseudo/eligibility predicates; and `tidb-exec::stats_load_result` ports
statistics-load item identity and error metadata. The shared `stmtctx.go`
source row was extended rather than duplicated. Planner/catalog integration,
statistics scheduling/loading, retries/channels, full Go parity, and a
deployable bootstrap remain open.

Update note (2026-07-16): Wave-50 adds `tidb-planner::pattern_engine` for
cascades engine flags, predefined sets, overlap membership, raw values, and
stable labels. Pattern matching, logical-plan integration, original Go parity,
and a deployable bootstrap remain open. The combined regenerated ledgers are
2,181/185/24/0 production and 15,986/445/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); the next consolidated workspace/Clippy
and static parser/plan/dependency gate is pending.

Update note (2026-07-16): Wave-51 adds three bounded source-owned leaves.
`tidb-planner::fix_control` ports fix-control parsing and duplicate-warning
semantics; `tidb-stats::analyze_version_matches` ports analyzed-version
matching decisions; and `tidb-exec::alternative_plan_signals` ports the
eight-field mark/reset signal state. The shared `table.go` and `stmtctx.go`
source rows were extended rather than duplicated. Session-variable wiring,
statistics rewrite/scheduling, planner rounds/cost choice, failpoints, full Go
parity, and a deployable bootstrap remain open. The regenerated ledgers are
2,180/186/24/0 production and 15,983/448/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); the next consolidated workspace/Clippy
and static parser/plan/dependency gate is pending.

Update note (2026-07-16): Wave-52 adds three bounded source-owned leaves.
`tidb-planner::memo_group_id` ports one-based cascades memo IDs and uint64
wraparound; `tidb-stats::estimate_ndv_by_gee` ports GEE singleton correction,
scaling, rounding, and clamps; and `tidb-exec::read_consistency` ports strict/
weak validation, exact raw `IsWeak`, and the strict default. The shared
`session.go` source row was extended rather than duplicated. Memo/optimizer
integration, sketch callers, request isolation, full Go parity, and a
deployable bootstrap remain open. The regenerated ledgers are
2,178/188/24/0 production and 15,980/451/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); the next consolidated workspace/Clippy
and static parser/plan/dependency gate is pending.

Update note (2026-07-16): Wave-53 adds three bounded source-owned leaves.
`tidb-planner::task_scheduler` ports serial LIFO execution, first-error stop,
pending-stack retention, cleanup, and default construction;
`tidb-stats::avg_count_per_not_null_value` ports increase-factor/NDV arithmetic
and empty fallback; and `tidb-exec::chunk_alloc_status` ports deterministic
set/clear/readback state. Shared histogram/stmtctx ownership rows were
extended rather than duplicated. Cascades pools/context, histogram/planner
integration, chunk reuse/lifecycle, full Go parity, and a deployable bootstrap
remain open. The regenerated ledgers are 2,177/189/24/0 production and
15,978/453/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED);
the next consolidated workspace/Clippy and static parser/plan/dependency gate
is pending.

Update note (2026-07-17): Wave-54 adds three bounded source-owned leaves.
`tidb-planner::hash_equaler` ports cascades primitive FNV-1a updates,
string/byte framing, nil markers, cache/reset lifecycle, and digest readback;
`tidb-stats::calc_correlation` ports the histogram builder's Pearson
order-correlation arithmetic and one-sample/zero-sample boundaries; and
`tidb-exec::setvar_hint_restore` ports the statement-local first-write-wins
old-value registry for SET_VAR hints. Object dispatch, sampling/histogram
construction, hint parsing/sysvar mutation/restoration, full Go parity, and a
deployable bootstrap remain open. The regenerated ledgers are
2,175/191/24/0 production and 15,973/458/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy, and the
static parser/plan/dependency gate pass with 12 jobs. The next parallel source
queues are ready.

Update note (2026-07-17): Wave-55 adds three bounded source-owned leaves.
`tidb-planner::plan_context` ports the bounded BuildPBContext detach hand-off,
`tidb-stats::index_usage` ports percentage-access buckets and sample merging,
and `tidb-exec::cursor_tracker` ports cursor state, monotonic IDs, lookup,
range, close, and bounded concurrent lifecycle behavior. Full planner context
interfaces, collector workers/persistence, session/result-set execution, full
Go parity, and a deployable bootstrap remain open. The regenerated ledgers are
2,171/195/24/0 production and 15,965/466/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy, and the
static parser/plan/dependency gate pass with 12 jobs. The next parallel source
queues are ready.

Update note (2026-07-17): Wave-56 adds three bounded source-owned leaves.
`tidb-planner::task_stack` ports the cascades reusable LIFO stack contract;
`tidb-stats::analyze_jobs` ports analyze status/job metadata and progress
threshold/dump/reset arithmetic; and `tidb-exec::session_context_key` ports
the source integer context-key labels and unknown display. Stack pools,
analyze persistence/scheduling, live context storage, full Go parity, and a
deployable bootstrap remain open. The regenerated ledgers are
2,168/198/24/0 production and 15,961/470/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy, and the
static parser/plan/dependency gate pass with 12 jobs. The next parallel source
queues are ready.

Update note (2026-07-17): Wave-57 adds three bounded source-owned leaves.
`tidb-planner::pattern` ports cascades operand/matching and child-pattern
metadata; `tidb-stats::async_load` ports the 128-shard pending statistics-load
map and full-load upgrade/delete semantics; and `tidb-exec::status_registry`
ports status scopes/values, provider registration/removal, deterministic
collection, and error propagation. Concrete planner/session consumers,
statistics persistence/scheduling, full Go parity, and a deployable bootstrap
remain open. The regenerated ledgers are 2,165/201/24/0 production and
15,951/480/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy, and the
static parser/plan/dependency gate pass with 12 jobs. The next parallel source
queues are ready.

Update note (2026-07-17): Wave-58 adds three bounded source-owned leaves.
`tidb-planner::string_writer` ports ordered string assembly and delimiter
semantics; `tidb-stats::datum_map_cache` ports normalized datum-key caching and
bounded map lifecycle; and `tidb-exec::process_info` ports shallow process
metadata cloning with optional field preservation. Full planner formatting and
cascades callers, statistics persistence/scheduling, session-manager ownership,
full Go parity, and a deployable bootstrap remain open. The regenerated ledgers
are 2,162/204/24/0 production and 15,946/485/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy, formatting,
and the static parser/plan/dependency gate pass with 12 jobs. The next parallel
source queues are ready.

Update note (2026-07-17): Wave-59 adds three bounded source-owned leaves.
`tidb-planner::expr_iterator` ports source-shaped memo expression matching,
recursive child cartesian enumeration, engine filtering, and reset/advance
state; `tidb-stats::need_analyze_table` ports bounded auto-analyze trigger
policy; and `tidb-exec::nextgen_readonly_vars` ports the six-name,
case-insensitive next-generation read-only-variable predicate. Real memo/list
ownership, statistics scheduling and persistence, variable registration and
SET dispatch, full Go parity, and a deployable bootstrap remain open. The
regenerated ledgers are 2,159/207/24/0 production and
15,940/491/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED);
workspace tests, strict Clippy, formatting, and the static parser/plan/
dependency gate pass with 12 jobs. The next parallel source queues are ready.

Update note (2026-07-17): Wave-60 adds three bounded source-owned leaves.
`tidb-planner::explore_mark` ports memo round-bit set/clear/query state and
fixed-width overflow behavior; `tidb-stats::parse_auto_analyze_ratio` ports
default, fallback, clamp, and non-finite ratio handling; and
`tidb-exec::slow_log_threshold` ports typed slow-log equality and threshold
conversion helpers. Real memo/statistics/session lifecycle, full Go parity, and
a deployable bootstrap remain open. The regenerated ledgers are
2,156/210/24/0 production and 15,935/496/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy, formatting,
and the static parser/plan/dependency gate pass with 12 jobs. The next parallel
source queues are ready.

Update note (2026-07-17): Wave-61 adds three bounded source-owned leaves.
`tidb-planner::group_expr` ports memo child identity, fingerprint framing,
exploration marks, and applied-rule tracking; `tidb-stats::AutoAnalysisTimeWindow`
ports inclusive UTC minute windows including unset endpoints and midnight
crossing; and `tidb-exec::slow_log_rules` ports typed slow-log rule metadata,
session stale-field state, and global connection grouping. Real memo/statistics/
session lifecycle, full Go parity, and a deployable bootstrap remain open. The
regenerated ledgers are 2,153/213/24/0 production and 15,929/502/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests,
strict Clippy, formatting, and the static parser/plan/dependency gate pass with
12 jobs. The next parallel source queues are ready.

Update note (2026-07-17): Wave-62 adds three bounded source-owned leaves.
`tidb-planner::column_length` ports `Col2Len` dominance and comparability;
`tidb-stats::calculate_priority_weight` plus `special_event_weight` ports the
auto-analyze priority formula and event weights; and
`tidb-exec::session_token_timing` ports classic/Starter token, certificate
reload, and old-certificate grace durations. Path extraction, queue/session
lifecycle, crypto/certificate I/O, full Go parity, and a deployable bootstrap
remain open. The regenerated ledgers are 2,150/216/24/0 production and
15,925/506/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED);
workspace tests, strict Clippy, formatting, and the static parser/plan/
dependency gate pass with 12 jobs. The next parallel source queues are ready.

Update note (2026-07-17): Wave-63 adds three bounded source-owned leaves.
`tidb-planner::plan_cache_constants` ports nil-preserving safe sharing and
unsafe deep-copy plan-cache constants; `tidb-stats::get_partition_sql` plus
`flatten_partition_names` ports dynamic-partition SQL/name helpers; and
`tidb-exec::advisory_lock_state` ports owner identity and signed reference
count state. Plan-cache/analysis-job/session lifecycle, SQL lock validation,
full Go parity, and a deployable bootstrap remain open. The regenerated ledgers
are 2,147/219/24/0 production and 15,921/510/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy, formatting,
and the static parser/plan/dependency gate pass with 12 jobs. The next parallel
source queues are ready.

Update note (2026-07-17): Wave-64 adds three bounded source-owned leaves.
`tidb-planner::index_advisor_model` ports column/index normalization and prefix
containment; `tidb-stats::priority_heap` ports bounded max-heap operations over
caller-owned scalar entries; and `tidb-exec::txn_running_state` ports the five
transaction-running states, labels, and counter. Advisor/AnalysisJob/session
lifecycle, live KV locks, full Go parity, and a deployable bootstrap remain
open. The regenerated ledgers are 2,144/222/24/0 production and
15,908/523/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED);
workspace tests, strict Clippy, formatting, and the static parser/plan/
dependency gate pass with 12 jobs. The next parallel source queues are ready.

Update note (2026-07-17): Wave-65 adds three bounded source-owned leaves.
`tidb-planner::rule_type` ports rule discriminants, raw round-tripping,
unknown-value retention, and exact labels; `tidb-stats::analysis_interval`
ports interval sentinels, failure/average-duration arithmetic, and raw query
constants; and `tidb-exec::txn_summary` ports FNV-1a SQL-digest sequences,
distinct-sequence promotion, bounded LRU eviction, resizing, and ordered
snapshots. Rule dispatch, query execution, JSON/duration rendering, global
recorder/session wiring, full Go parity, and a deployable bootstrap remain
open. The regenerated ledgers are 2,141/225/24/0 production and
15,902/529/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED);
workspace tests, strict Clippy, formatting, and the static parser/plan/
dependency gate pass with 12 jobs. Wave 66 is now the next parallel source
queue.

Update note (2026-07-17): Wave-66 adds three bounded source-owned leaves.
`tidb-planner::base_traits` ports `Hash64`, `Equals`, and `HashEquals` over the
existing typed hasher; `tidb-stats::auto_analyze_job` ports bounded indicator
string/JSON formatting and dynamic-partitioned job classification; and
`tidb-exec::session_pool_capacity` ports the system-session pool limit and
invalid-value normalization. Cascades object dispatch, concrete analysis-job
interfaces/queue lifecycle, and session factory/channel/transfer ownership
remain open. The regenerated ledgers are 2,138/228/24/0 production and
15,899/532/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED);
workspace tests, strict Clippy, formatting, and the static parser/plan/
dependency gate pass with 12 jobs. Wave 67 is now the next parallel source
queue.

Update note (2026-07-17): Wave-67 adds three bounded source-owned leaves.
`tidb-planner::scheduler_contract` ports the source Scheduler interface over
the existing task owner; `tidb-stats::non_partitioned_analysis` ports exact
analyze-table/index SQL templates, ordered parameters, and index-kind metadata;
and `tidb-exec::sysvar_scope` ports ScopeFlag bits, rendering order, and
unknown-bit behavior. Concurrent/cascades scheduling, schema/session analysis
execution and validation, and SysVar registry/SET/GET lifecycle remain open.
The regenerated ledgers are 2,135/231/24/0 production and
15,891/540/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED);
workspace tests, strict Clippy, formatting, and the static parser/plan/
dependency gate pass with 12 jobs. Wave 68 is now the next parallel source
queue.

Update note (2026-07-17): Wave-68 adds three bounded source-owned leaves.
`tidb-planner::stack_contract` ports the richer Stack/StackTask boundary over
the existing concrete task-stack owner; `tidb-stats::static_partitioned_analysis`
ports exact static partition/table/index SQL, physical partition keys, and
index-kind metadata; and `tidb-exec::charset_variable_groups` ports ordered SET
NAMES/SET CHARSET groups and membership. Concrete task-stack behavior,
partition/session analysis execution and validation, and SET/collation/
SessionVars lifecycle remain open. The regenerated ledgers are
2,132/234/24/0 production and 15,884/547/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and the static parser/plan/dependency gate pass with 12 jobs. Wave
69 is now the next parallel source queue.

Update note (2026-07-17): Wave-69 adds three bounded source-owned leaves.
`tidb-planner::topn_push_down` ports the source rule wrapper callback/name/
change-flag contract; `tidb-stats::queue_gate` ports the exact uninitialized
priority-queue error and shared gate defaults; and `tidb-exec::sysvar_type`
ports byte-backed TypeFlag discriminants 0..7 while sharing the authoritative
Go source ownership row with the ScopeFlag leaf. Full logical-plan TopN
integration, queue/worker/session/DDL lifecycle, and SysVar registry/validation/
conversion remain open. The regenerated ledgers are 2,130/236/24/0 production
and 15,881/550/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and the static parser/plan/dependency gate pass with 12 jobs. Wave
70 is now the next parallel source queue.

Update note (2026-07-17): Wave-70 adds three bounded source-owned leaves.
`tidb-planner::derive_topn_from_window` ports the source rule wrapper callback,
name, false change flag, and nil-error boundary; `tidb-stats::ddl_queue_gate`
ports initialized/retry/ignore readiness decisions before DDL event dispatch;
and `tidb-exec::sysvar_error` ports exact variable error-code identities.
Window/TopN/MPP semantics, queue event/lifecycle mutation, and dbterror
constructors/messages/SQLSTATE/warning plumbing remain open. The regenerated
ledgers are 2,127/239/24/0 production and 15,877/554/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict
Clippy, formatting, and the static parser/plan/dependency gate pass with 12
jobs. Wave 71 is now the next parallel source queue.

Update note (2026-07-17): Wave-71 adds three bounded source-owned leaves.
`tidb-planner::eliminate_empty_selection` ports the recursive rule-wrapper
callback/name/change-flag/error boundary; `tidb-stats::refresher_state` ports
initialized-only ratio/prune-mode queue-rebuild decisions; and
`tidb-exec::hint_updatable_vars` ports the complete 128-name SET_VAR registry
and exact membership predicate. Logical-plan mutation, queue/session/worker
lifecycle, and SysVar/hint mutation/application remain open. The regenerated
ledgers are 2,124/242/24/0 production and 15,874/557/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict
Clippy, formatting, and the static parser/plan/dependency gate pass with 12
jobs. Wave 72 is now the next parallel source queue.

Update note (2026-07-17): Wave-72 adds three bounded source-owned leaves.
`tidb-planner::push_down_sequence` ports recursive sequence CTE/main merging,
DataSource/CTE push-through, unary descent, and safe child attachment;
`tidb-stats::worker_capacity` ports SubmitJob admission and unchanged
concurrency-update no-op behavior; and `tidb-exec::noop_read_only` ports the
first five no-op/read-only registrations plus pure session/global
OFF/ON/WARN read-only policy. Logical operators, async workers, full SysVar
mutation, warning/error plumbing, and session lifecycle remain open. The
regenerated ledgers are 2,121/245/24/0 production and 15,871/560/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests,
strict Clippy, formatting, and the static parser/plan/dependency gate pass with
12 jobs. Wave 73 is now the next parallel source queue.

Update note (2026-07-17): Wave-73 adds three bounded source-owned leaves.
`tidb-planner::eliminate_unionall_dual_item` ports recursive zero-row
TableDual/projection filtering, schema-preserving empty-union replacement, and
changed aggregation; `tidb-stats::stats_key_set` ports thread-safe key
replacement, lookup/removal costs, enumeration, length, and clear; and
`tidb-exec::session_reuse_state` ports owner-gated avoid-reuse plus idempotent
close state. Logical operator execution, LFU admission/eviction, table
accounting, owner hooks, context close, in-use deferral, transfer, and
operation locking remain open. The regenerated ledgers are 2,118/248/24/0
production and 15,867/564/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and the static parser/plan/dependency gate pass with 12 jobs. Wave
74 is now the next parallel source queue.

Update note (2026-07-17): Wave-74 adds three bounded source-owned leaves.
`tidb-planner::projection_elimination` ports the loose projection-elimination
predicate; `tidb-stats::stats_key_set_shards` ports fixed 256-shard routing and
aggregate key-set operations; and `tidb-exec::system_db_filter` ports the
`SkipLoadDiff=false` plus lower-case `mysql` system-database filter. Full
expression/schema elimination, LFU admission/eviction and accounting, and
domain/schema loading remain open. The regenerated ledgers are 2,114/252/24/0
production and 15,864/567/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and the static parser/plan/dependency gate pass with 12 jobs. Wave
75 is now the next parallel source queue.

Update note (2026-07-17): Wave-75 adds three bounded source-owned leaves.
`tidb-planner::resolve_grouping_expand` ports post-order Expand traversal and
append-style generated-level counts; `tidb-stats::memory_cost` ports LFU
capacity adjustment, 20%-memory fallback, test override, probe errors, and
signed cost wraparound; and `tidb-exec::upgrade_versions` ports the exact
ordered 173-entry upgrade registry, historical gaps, current version 263, and
function naming. Grouping-set construction, host-memory/cache lifecycle,
upgrade SQL/bootstrap mutation, and schema changes remain open. The
regenerated ledgers are 2,111/255/24/0 production and 15,861/570/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests,
strict Clippy, formatting, and the static parser/plan/dependency gate pass with
12 jobs. Wave 76 is now the next parallel source queue.

Update note (2026-07-17): Wave-76 adds three bounded source-owned leaves.
`tidb-planner::join_reorder_projection_inline` ports the recursive supported
expression tree and rejects the source's deferred, unsupported, expanded,
mutable, nondeterministic, correlated, and zero-column cases;
`tidb-stats::BatchUpdate` ports capacity-triggered update/delete flushing and
ordering; and `tidb-exec::session_metrics` ports the exact
delete/insert/update registration labels. Join-group substitution, the stats
queue/cache lifecycle, and session metric collection remain external. The
regenerated ledgers are 2,108/258/24/0 production and 15,858/573/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests,
strict Clippy, formatting, and all static parser/plan/ledger/domain gates pass
with 12 jobs. Wave 77 is now the next parallel source queue.

Update note (2026-07-17): Wave-77 adds three bounded source-owned leaves.
`tidb-planner::max_min_elimination` ports the source eligibility gates and
single-vs-multi aggregate classification; `tidb-stats::MapCache` ports
caller-costed map-cache operations, copy state, and no-op lifecycle hooks; and
`tidb-exec::hash_join_version` ports the legacy/optimized version literals,
legacy default, and case-insensitive optimized predicate. Index-path and plan
construction, LFU admission/eviction, cache ownership, SysVar mutation, and
join selection remain external. The regenerated ledgers are 2,105/261/24/0
production and 15,853/578/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and all static parser/plan/ledger/domain gates pass with 12 jobs.
Wave 78 is now the next parallel source queue.

Update note (2026-07-17): Wave-78 adds three bounded source-owned leaves.
`tidb-planner::logical_table_dual` ports TableDual identity/hash and explain
metadata; `tidb-stats::healthy_metrics` ports the exact ten healthy buckets;
and `tidb-exec::slow_log_match` ports slow-log boolean composition and
session/connection/global precedence. Field-type/runtime details, metrics
registration and traversal, and slow-log accessors/thresholds/session state
remain external. The regenerated ledgers are 2,102/264/24/0 production and
15,850/581/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and all static parser/plan/ledger/domain gates pass with 12 jobs.
Wave 79 is now the next parallel source queue.

Update note (2026-07-17): Wave-79 adds three bounded source-owned leaves.
`tidb-planner::logical_limit` ports Limit identity/hash and bounded explain
metadata; `tidb-stats::json_metadata` ports the global marker and deterministic
predicate-column ordering; and `tidb-exec::privilege_set` ports exact
split/join/add/delete set semantics. Runtime limit behavior, tipb/storage and
stats-handle ownership, and GRANT/REVOKE SQL/persistence remain external. The
regenerated ledgers are 2,099/267/24/0 production and 15,847/584/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests,
strict Clippy, formatting, and all static parser/plan/ledger/domain gates pass
with 12 jobs. Wave 80 is now the next parallel source queue.

Update note (2026-07-17): Wave-80 adds three bounded source-owned leaves.
`tidb-planner::logical_max_one_row` ports the generated MaxOneRow identity/hash
contract; `tidb-stats::locked_tables` ports the locked-table query marker and
requested-ID filter; and `tidb-exec::effective_auth_plugin` ports explicit
plugin precedence and default fallback resolution. Runtime planning, SQL/lock
lifecycle, auth storage, capability checks, and password policy remain
external. The regenerated ledgers are 2,096/270/24/0 production and
15,842/589/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and all static parser/plan/ledger/domain gates pass with 12 jobs.
Wave 81 is now the next parallel source queue.

Update note (2026-07-17): Wave-81 adds three bounded source-owned leaves.
`tidb-planner::logical_sort` ports generated Sort identity/hash framing;
`tidb-stats::lock_messages` ports stable skipped-table/partition formatting;
and `tidb-exec::broadcast_query_error` ports the nil-safe unsupported-broadcast
classifier. Runtime ordering, lock/SQL lifecycle, and broadcast RPC remain
external.

Update note (2026-07-17): Wave-82 adds three bounded source-owned leaves.
`tidb-planner::logical_top_n` ports generated TopN identity/hash framing;
`tidb-stats::usage_collector` ports bounded priority queues and worker
drain/close behavior; and `tidb-exec::insert_rows_col_multiply` ports
zero-aware saturating row/column multiplication. Runtime TopN, session/worker
lifecycle, and RUV2 metric wiring remain external. The regenerated ledgers are
2,090/276/24/0 production and 15,833/598/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); workspace tests, strict Clippy,
formatting, and all static parser/plan/ledger/domain gates pass with 12 jobs.
Wave 83 is now the next parallel source queue.

Update note (2026-07-17): Wave-83 adds three bounded source-owned leaves.
`tidb-planner::logical_show_ddl_jobs` ports generated ShowDDLJobs identity/hash
framing; `tidb-stats::stats_delta` ports the locked-statistics delta query
marker and row/error behavior; and `tidb-exec::readable_size` ports
case-sensitive human-readable size parsing with source-compatible wrapping.
DDL scheduling, statistics-handle ownership, inspection SQL, and caller policy
remain external. The regenerated ledgers are 2,087/279/24/0 production and
15,830/601/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` still tracked separately.
Wave 84 is now the next parallel source queue.

Update note (2026-07-17): Wave-84 adds three bounded source-owned leaves.
`tidb-planner::logical_show` ports generated Show identity/hash framing with
ordered normalized schema metadata; `tidb-stats::bootstrap_sql` ports exact
statistics metadata and histogram bootstrap SQL with ordered IDs and
`[start,end)` paging; and `tidb-exec::placement_labels` ports deterministic
SHOW PLACEMENT label grouping, deduplication, and row ordering. SHOW contents,
stats-handle/session/SQL execution, BinaryJSON/PD/store retrieval, and row
encoding remain external. The regenerated ledgers are 2,084/282/24/0
production and 15,823/608/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 85 is now the next parallel source queue.

Update note (2026-07-17): Wave-85 adds three bounded source-owned leaves.
`tidb-planner::logical_schema_producer` ports generated
LogicalSchemaProducer identity/hash framing with nil/present ordered schemas;
`tidb-stats::special_global_index` ports the virtual-generated/prefix-column
global-index predicate and any-column short circuit; and
`tidb-exec::lazy_txn_state` ports source-faithful `Valid`, `pending`, and
`validOrPending` composition. The lazy-transaction original test anchors were
already owned by the transaction-wave17/18 rings, so the shared test ledger
keeps one authoritative owner per anchor. Schema propagation, full field
metadata, index metadata resolution, KV/session lifecycle, and transaction
execution remain external. The regenerated ledgers are 2,081/285/24/0
production and 15,821/610/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 86 is now the next parallel source queue.

Update note (2026-07-17): Wave-86 adds three bounded source-owned leaves.
`tidb-planner::logical_sequence` ports generated LogicalSequence identity/hash
framing; `tidb-stats::global_topn` ports histogram-free partition TopN
aggregation with wrapping sums, count/encoded-byte ranking, and selected versus
remainder ordering; and `tidb-exec::config_int_json` ports integer SET CONFIG
JSON rendering for boolean flags and ordinary integers. The global TopN
evidence uses the unclaimed `global_stats_test.go:322 TestGlobalStatsData3`
anchor (ranking assertions at 342-347); narrower TopN anchors remain owned by
the earlier datum-map-cache ring. CTE/runtime sequence behavior, histograms,
Datum/config mutation, storage, and session lifecycle remain external. The
regenerated ledgers are 2,078/288/24/0 production and 15,818/613/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 87 is now the next parallel source queue.

Update note (2026-07-17): Wave-87 adds three bounded source-owned leaves.
`tidb-planner::logical_union_all` ports generated LogicalUnionAll identity/hash
framing; the initially selected LogicalSelection owner was already claimed and
was removed before integration. `tidb-stats::pending_delta_ids` ports pending
table-ID filtering, target deduplication, and ascending order; and
`tidb-exec::lack_handles` ports ordered missing-handle reconciliation with the
source cardinality stop boundary. Union execution, stats/session sweeps,
storage, KV encoding, workers, and consistency reporting remain external. The
regenerated ledgers are 2,075/291/24/0 production and 15,815/616/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 88 is now the next parallel source queue.

Update note (2026-07-17): Wave-88 adds three bounded source-owned leaves.
`tidb-planner::logical_mem_table` ports generated LogicalMemTable identity/hash
framing over optional table metadata and normalized names;
`tidb-stats::sync_load_concurrency` ports the source table-count threshold
policy; and `tidb-exec::slow_log_split` ports nested byte-oriented slow-log
field/value splitting with malformed-input and cardinality-boundary behavior.
Memtable planning/execution, statistics scheduling/handle lifecycle, log
ingestion, session policy, and persistence remain external. The regenerated
ledgers are 2,072/294/24/0 production and 15,812/619/142/12 test/support
obligations; the parser ring and full workspace/Clippy gate remain green, with
the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 89 is now the next parallel source queue.

Update note (2026-07-17): Wave-89 adds three bounded source-owned leaves.
`tidb-planner::logical_projection` ports generated LogicalProjection
identity/hash framing over normalized schema and ordered expression columns,
nil/present expression markers, `CalculateNoDelay`, and `Proj4Expand`;
`tidb-stats::partition_table_id_cache` ports schema-versioned
partition-to-parent-table cache rebuild and lookup with duplicate last-write
behavior; and `tidb-exec::analyze_panic_error` ports analyze-worker panic
classification for the memory sentinel, propagated errors, and worker fallback
with the exact samplerate guidance. Expression evaluation, planner rewrites,
InfoSchema traversal/table resolution, locking, recovery, logging, and worker
scheduling remain external. The regenerated ledgers are 2,069/297/24/0
production and 15,809/622/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 90 is now the next parallel source queue.

Update note (2026-07-17): Wave-90 adds three bounded source-owned leaves.
`tidb-planner::logical_expand` ports generated LogicalExpand identity/hash
framing over normalized grouping metadata and nested rollup/level structure;
`tidb-stats::weighted_reservoir` ports bounded weighted sampling with the
source min-heap fill/replace and tie behavior; and
`tidb-exec::delete_rows_col_multiply` ports saturating DELETE row/column metric
accumulation, MAX sentinel handling, and positive overflow clamping. Expression
variants, grouping maps, RNG/Datum/sketch collectors, metric/session/storage
effects, optimizer context, and runtime execution remain external. The
regenerated ledgers are 2,066/300/24/0 production and 15,806/625/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 91 is now the next parallel source queue.

Update note (2026-07-17): Wave-91 adds three bounded source-owned leaves.
`tidb-planner::window_frame` ports FrameBound/WindowFrame Hash64/Equals,
nil-preserving clone behavior, caller compare-function tokens, and the source
start/end hash asymmetry; `tidb-stats::stats_meta` ports exact normal and
`FOR UPDATE` `mysql.stats_meta` selectors, empty-row null sentinels, and
uint64-to-int64 conversion; and `tidb-exec::cte_first_error` ports first-error
precedence while preserving the original value. Expressions, SQL/storage/DDL
execution, worker lifecycle, logging, failpoints, and cleanup ordering remain
external. The regenerated ledgers are 2,063/303/24/0 production and
15,801/630/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 92 is now the next parallel source queue.

Update note (2026-07-17): Wave-92 adds three bounded source-owned leaves.
`tidb-planner::handle_cols` ports CommonHandleCols and IntHandleCols
identity/hash framing with nil/present metadata and ordered column lists;
`tidb-stats::stats_read_writer` ports historical-version and slow-save
predicates, the five-lease threshold, force override, duration wrapping, and
the exact refresh error text; and `tidb-exec::traffic_form` ports Go-compatible
sorted form encoding, escaping, duplicate ordering, UTF-8, and reserved-byte
boundaries. Catalog/handle/storage, SQL/transaction/failpoint lifecycle, and
HTTP/TiProxy traffic remain external. The regenerated ledgers are
2,060/306/24/0 production and 15,795/636/142/12 test/support obligations; the
parser ring and full workspace/Clippy gate remain green, with the pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582` tracked
separately.
Wave 93 is now the next parallel source queue.

Update note (2026-07-17): Wave-93 adds three bounded source-owned leaves.
`tidb-planner::logical_aggregation` ports source-faithful
`LogicalAggregation` Hash64/Equals framing, normalized aggregate metadata,
ordered possible properties, and explicit `HasTiFlash` omission;
`tidb-stats::stats_meta_update` ports locked/unlocked and positive/negative
delta partitioning, exact stats-meta SQL assembly, cache invalidation order,
MinInt64 wrapping, and version-refresh parameters; and
`tidb-exec::ddl_job_comments` ports source-ordered analyze, reorg, DXF/cloud,
worker, batch, write-speed, and placement labels, including next-gen early
return behavior. The evidence audit corrected the Go test anchors to
`show_ddl_jobs_test.go:26` and `:115`. Live planner/statistics/DDL execution
remains external. The regenerated ledgers are 2,057/309/24/0 production and
15,790/641/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 94 is now the next parallel source queue.

Update note (2026-07-17): Wave-94 adds three bounded source-owned leaves.
`tidb-planner::cost_usage` ports CostVer2/CostTrace factor gating, lazy
formula construction, ordered aggregation, fixed-point arithmetic,
nonnegative/NaN handling, and tie-break preservation;
`tidb-stats::sample_bytes` ports the exact 32,767-byte sample limit, inclusive
length filtering, and wrapping total-size accumulation; and
`tidb-exec::global_sysvar_initial` ports environment-adjusted system-variable
defaults across TiKV, test, row-format, assertion, mutation-checker, and
fair-locking branches. Registry lookup, validation, SessionVars mutation, and
next-gen hook errors remain external. The regenerated ledgers are
2,054/312/24/0 production and 15,785/646/142/12 test/support obligations; the
parser ring and full workspace/Clippy gate remain green, with the pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582` tracked
separately.
Wave 95 is now the next parallel source queue.

Update note (2026-07-17): Wave-95 adds three bounded source-owned leaves.
`tidb-planner::wrap_cast` ports the source mode gate across
Complete/Partial1/Dedup and Final/Partial2, including caller-marked delegated
uncastable arguments; `tidb-stats::index_query_bytes` ports TopN-hit,
CMSketch-hit, then histogram fallback precedence over caller-supplied counts;
and `tidb-exec::tagged_ptr` ports 64-bit tagged-pointer width, mask
initialization, tag extraction, clear/roundtrip behavior, and the 24-bit cap.
Expression construction, statistics encoding/lifecycle, and join/hash-table
execution remain external. The regenerated ledgers are 2,051/315/24/0
production and 15,780/651/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 96 is now the next parallel source queue.

Update note (2026-07-17): Wave-96 adds three bounded source-owned leaves.
`tidb-planner::logical_mock` ports `MockDataSource.Init` metadata with
`mockDS`, query-block offset zero, retained plan-context token, and
reinitialization/zero-value behavior; `tidb-stats::historical_stats` ports
table-versus-partition history-version selection; and `tidb-exec::stddevpop`
ports zero-count NULL handling plus `sqrt(variance/count)` with negative
variance preserving NaN. Physical mock planning, JSON/storage/session
lifecycle, and aggregation accumulation remain external. The regenerated
ledgers are 2,048/318/24/0 production and 15,776/655/142/12 test/support
obligations; the parser ring and full workspace/Clippy gate remain green, with
the pinned Go restore failure at `tests/integrationtest/t/expression/json.test:582`
tracked separately.
Wave 97 is now the next parallel source queue.

Update note (2026-07-17): Wave-97 adds three bounded source-owned leaves.
`tidb-planner::logical_property` ports zero-value and optional Stats/Schema/FD
state, MaxOneRow, nil-vs-empty PossibleProps, and HasTiFlash preservation;
`tidb-stats::init_stats_concurrency` ports force CPU-minus-two and normal
CPU-half policies with the `[2,16]` clamp and signed wrapping arithmetic; and
`tidb-exec::stddevsamp` ports counts-at-most-one NULL handling plus
`sqrt(variance/(count-1))` with negative variance preserving NaN. Memo/schema
consumers, runtime/config lifecycle, and aggregation accumulation remain
external. The regenerated ledgers are 2,045/321/24/0 production and
15,772/659/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 98 is now the next parallel source queue.

Update note (2026-07-17): Wave-98 adds three bounded source-owned leaves.
`tidb-planner::outer_to_inner_join` ports the rule identity, exactly-once
delegated LogicalPlan conversion, and intentionally unchanged flag;
`tidb-stats::predicate_column_queries` ports exact load-all, load-table,
predicate, cleanup SQL markers and ordered decimal column-ID formatting; and
`tidb-exec::varsamp` ports counts-at-most-one NULL handling plus
`variance/(count-1)` while preserving signed float results. Join semantics,
schema/session/storage execution, and aggregation accumulation remain external.
The regenerated ledgers are 2,042/324/24/0 production and
15,767/664/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 99 is now the next parallel source queue.

Update note (2026-07-17): Wave-99 adds three bounded source-owned leaves.
`tidb-planner::columnar_index_extra` ports the vector columnar-index metadata
constructor with fixed vector type, retained index identity/derived ID, ANN
query type/metric/top-k, column name, copied reference-vector bytes, and source
column identity; its direct Go test anchor is
`pkg/planner/core/task_heavy_function_optimize_test.go:36
TestGetPushedDownTopNHeavyFunctionNotFirstByItem`.
`tidb-stats::ddl_stats_delta` ports the locked, missing-row, and existing-row
`stats_meta` SQL branches with ordered arguments, GREATEST clamps, and Go
wrapping additions; its direct anchors are
`pkg/statistics/handle/ddl/ddl_test.go:1106 TestExchangeAPartition` and
`:1256 TestExchangeAPartitionAndDropTableImmediately`.
`tidb-exec::cume_dist` ports the source `curIdx`/`lastRank` tied-peer loop as
an Iterator plus partial state-size metadata; its direct anchors are
`pkg/executor/aggfuncs/func_cume_dist_test.go:25 TestMemCumeDist` and
`pkg/executor/aggfuncs/window_func_test.go:172 TestWindowFunctions`. TiFlash/vector planning,
DDL/storage/session lifecycle, row comparison, window scheduling, and chunk
execution remain external. The regenerated ledgers are 2,039/327/24/0
production and 15,762/669/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 100 is now the next parallel source queue.

Update note (2026-07-17): Wave-100 adds three bounded source-owned leaves.
`tidb-planner::physical_cte_table` ports signed CTE storage identity,
`Scan on CTE_<id>` explain text, and the index-join/sort task rejection gates;
its direct anchor is `pkg/planner/core/tests/redact/redact_test.go:23
TestRedactExplain`. `tidb-stats::gc_batch_count` ports Go `forCount` integer
division, positive-remainder rounding, and signed overflow behavior; its
direct anchors are `pkg/statistics/handle/storage/gc_test.go:30 TestGCStats`
and `:63 TestGCPartition`. `tidb-exec::ntile` ports the five-field partial
state, quotient/remainder updates, reset, group advancement, and zero-divisor
NULL behavior; its unowned direct anchor is
`pkg/executor/aggfuncs/func_ntile_test.go:25 TestMemNtile`. Schema/statistics/
task wiring, storage/session lifecycle, typed chunks, argument coercion, and
window scheduling remain external. The regenerated ledgers are 2,036/330/24/0
production and 15,758/673/142/12 test/support obligations.

Update note (2026-07-17): Wave-101 adds `tidb-exec::lead_lag`, porting the
buffered row cursor, physical lead/lag offsets, current-row/default fallback,
reset, and partial-state size. Typed Datum serialization, chunk/window
construction, and scheduling remain external. Direct anchors are
`pkg/executor/aggfuncs/func_lead_lag_test.go:27 TestLeadLag` and
`:119 TestMemLeadLag`. The Wave-101 combined regenerated ledgers are
2,035/331/24/0 production and 15,756/675/142/12 test/support obligations; the
same fail-fast 12-job workspace/Clippy/static gate is green, with the pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582` tracked
separately. Wave 102 is now the next parallel source queue.

Update note (2026-07-17): Wave-102 adds three bounded source-owned leaves.
`tidb-planner::physical_max_one_row` ports the pure
`ExhaustPhysicalPlans4LogicalMaxOneRow` support gates, fixed `ExpectedCnt: 2`,
and CTE/no-cop metadata forwarding; `pkg/executor/test/executor/executor_test.go:2157`
(`TestMaxOneRow`) is the direct anchor. `tidb-stats::StatsLease` ports atomic
signed-nanosecond get/set semantics with direct anchors
`pkg/statistics/integration_test.go:220` (`TestShowHistogramsLoadStatus`) and
`:266` (`TestColumnStatsLazyLoad`). `tidb-exec::json_arrayagg` ports ordered
accumulation, partial merge/reset, empty-input NULL, JSON framing, scalar
escaping, finite-real guards, and explicit spill boundaries; direct anchors are
`pkg/executor/aggfuncs/func_json_arrayagg_test.go:27`, `:65`, `:131`, and
`pkg/executor/aggfuncs/spill_helper_test.go:842`. Typed Datum/BinaryJSON
conversion, chunk evaluation, physical task/runtime wiring, and statistics
lifecycle remain external. The regenerated ledgers are 2,032/334/24/0
production and 15,749/682/142/12 test/support obligations; parser, workspace,
Clippy, and static domain gates pass with 12 jobs, with the pinned Go restore
failure tracked separately. Wave 103 was integrated into the next verified
workspace cycle.

Update note (2026-07-17): Wave-103 adds three bounded source-owned leaves.
`tidb-planner::logical_cte_table` ports the exact `DeriveStats` reload-vector
state transition from `pkg/planner/core/operator/logicalop/logical_cte_table.go`
with direct anchor `pkg/planner/core/casetest/planstats/plan_stats_test.go:281`
(`TestPlanStatsLoadForCTE`). `tidb-stats::global_stats_layout` ports
`newGlobalStats` with four equal-length nil slot arrays, zero counts, and nil
missing-partition metadata; its direct anchor is
`pkg/statistics/handle/globalstats/global_stats_test.go:137`
(`TestBuildGlobalLevelStats`). `tidb-exec::json_objectagg` ports ordered
key/value state, source-after-destination merge, duplicate-key last-wins,
lexicographic JSON framing, empty-input NULL, and NULL/binary-key rejection;
direct anchors are `pkg/executor/aggfuncs/func_json_objectagg_test.go:48`,
`:110`, `:163`, and `pkg/executor/aggfuncs/spill_helper_test.go:889`. Typed
evaluation/coercion, concrete stats/schema context, BinaryJSON/memory/spill
integration, chunk execution, and storage/session lifecycle remain external.
The regenerated ledgers are 2,029/337/24/0 production and 15,743/688/142/12
test/support obligations; parser, workspace, Clippy, and static domain gates
pass with 12 jobs, with the pinned Go restore failure tracked separately. Wave
104 was integrated into the next verified workspace cycle.

Update note (2026-07-17): Wave-104 adds three bounded source-owned leaves.
`tidb-planner::telemetry` ports `IsTiFlashContained` with one-level Explain
unwrapping, physical filtering, TiFlash TableReader detection, ExchangeSender
classification, ordered child traversal, and early stop; its direct anchor is
`pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:568`
(`TestMPPSharedCTEScan`). `tidb-stats::table_id_filter` ports exact
source-ordered signed decimal `table_id in (...)` formatting, including empty
input, with direct anchors `pkg/executor/test/infoschema/infoschema_test.go:171`
(`TestDataForTableStatsField`) and `:224` (`TestPartitionsTable`).
`tidb-exec::first_row` ports first-physical-row-wins state, NULL preservation,
later-batch short-circuit, unset-destination merge, and reset; direct anchors
are `pkg/executor/aggfuncs/func_first_row_test.go:27`/`:52` plus the ten
type-specific spill anchors from `pkg/executor/aggfuncs/spill_helper_test.go:941`
through `:1349`. Concrete plans/session telemetry, cache/InfoSchema lifecycle,
typed values/chunk output, memory and spill encoding remain external. The
regenerated ledgers are 2,026/340/24/0 production and 15,728/703/142/12
test/support obligations; parser, workspace, Clippy, and static domain gates
pass with 12 jobs, with the pinned Go restore failure tracked separately. Wave
105 was integrated into the next verified workspace cycle.

Update note (2026-07-17): Wave-105 adds three bounded source-owned leaves.
`tidb-planner::condition_to_dual` ports `IsConstFalse`/`Conds2TableDual`
control flow with NULL/false classification, NULL precedence,
empty/multi-condition cardinality, and plan-cache suppression; its direct
anchor is `pkg/planner/core/logical_plans_test.go:241`
(`TestAntiSemiJoinConstFalse`). `tidb-stats::auto_analyze_process_set` ports
the RWMutex-backed tracker/untracker/all/contains global process set over uint64
IDs with direct anchors `pkg/statistics/handle/autoanalyze/exec/exec_test.go:35`
(`TestExecAutoAnalyzes`) and `:154` (`TestKillInWindows`). `tidb-exec::bit_agg`
ports u64 AND/OR/XOR identities, NULL-skipping updates,
operation-preserving merges, and reset; direct anchors are
`pkg/executor/aggfuncs/func_bitfuncs_test.go:25`/`:36` and
`pkg/executor/aggfuncs/spill_helper_test.go:801`. Expression/coercion and
statement context, concrete stats/process execution, typed Eval/chunk/sliding/
memory/spill integration, and optimizer/runtime wiring remain external. The
regenerated ledgers are 2,023/343/24/0 production and 15,722/709/142/12
test/support obligations; parser, workspace, Clippy, and static domain gates
pass with 12 jobs, with the pinned Go restore failure tracked separately. Wave
106 is now the next parallel source queue.

Update note (2026-07-17): Wave-106 adds three bounded source-owned leaves.
`tidb-planner::physical_table_sample` ports exact `PhysicalTableSample.Init`
metadata—TableSample type, pseudo row count one, query-block offset, physical
table ID, and Desc—with direct anchor `pkg/executor/sample_test.go:111`
(`TestTableSamplePlan`). `tidb-stats::stats_meta_save_sql` ports source-ordered
`stats_meta` INSERT/upsert tuple assembly, optional
`last_stats_histograms_version`, and empty-spacing behavior; its direct anchor
is `pkg/statistics/integration_test.go:442` (`TestSaveMetaToStorage`).
`tidb-exec::varpop` ports non-DISTINCT float64 population variance state,
NULL-skipping updates, source intermediate/merge formulas, zero-count branches,
population output, and reset; direct anchors are
`pkg/executor/aggfuncs/func_varpop_test.go:28`/`:37`/`:46`/`:54`. Schema/table/
sampler objects, SQL/startTS/session/storage, DISTINCT sets, typed EvalReal,
chunk/sliding/memory, spill, and runtime execution remain external. The
regenerated ledgers are 2,020/346/24/0 production and 15,716/715/142/12
test/support obligations; parser, workspace, Clippy, and static domain gates
pass with 12 jobs, with the pinned Go restore failure tracked separately. Waves
107-112 were integrated below; Wave 113 is the next parallel source queue.

Wave 107 adds `tidb-planner::rule_set`, `tidb-stats::init_stats_progress`, and
`tidb-exec::sum_float64`. These preserve source-shaped rule-ID mask filtering
and intermediate Apply rule-set selection, init-stats uint64-to-float64
progress arithmetic including IEEE zero-denominator behavior, and non-DISTINCT
float64 SUM state with NULL skipping, empty-result NULL, source-empty merge
short-circuit, and reset. Exact anchors are
`pkg/planner/cascades/rule/ruleset/rule_set.go` plus
`pkg/planner/cascades/old/optimize_test.go:212`,
`pkg/statistics/handle/initstats/load_stats_page.go:104-107` plus
`pkg/statistics/handle/handletest/initstats/init_stats_test.go:231`, and
`pkg/executor/aggfuncs/func_sum_test.go:33`/`:50`/`:66` plus
`pkg/executor/aggfuncs/spill_helper_test.go:658`/`:703`. Concrete rules,
memo/optimizer execution, init-stats workers/channels/atomics, typed SUM
coercion and variants, DISTINCT/sliding/chunk/memory/spill, and runtime wiring
remain external. The regenerated ledgers are 2,017/349/24/0 production and
15,704/727/142/12 test/support obligations; the consolidated 12-job static
gate is green, with the pinned Go restore failure tracked separately. Waves
108-112 were integrated below; Wave 113 is the next parallel source queue.

Wave 108 adds `tidb-planner::column_pruning`,
`tidb-stats::global_stats_sql_index`, and `tidb-exec::group_concat`. These
preserve the recursive zero-column schema invariant and its schema-reuse/
TableDual exceptions, exact `toSQLIndex` false/true to `is_index` 0/1 mapping,
and non-DISTINCT GROUP_CONCAT partial buffering with NULL skipping, separator
order, merge/reset/final-NULL behavior, byte-based max length, and lifetime
truncation sentinel. Exact anchors are
`pkg/planner/core/logical_plans_test.go:652`,
`pkg/statistics/handle/globalstats/global_stats_test.go:260`, and
`pkg/executor/aggfuncs/func_group_concat_test.go:37`/`:42`/`:66`/`:81`, with
source implementations at `rule_column_pruning.go`, `global_stats_async.go:50-57`,
and `func_group_concat.go:222-275`/`:285-292`. Logical-plan/optimizer
execution, stats workers/storage/SQL lifecycle, typed GROUP_CONCAT evaluation,
warning publication, DISTINCT/ORDER BY, chunk/memory, spill, and runtime wiring
remain external. The regenerated ledgers are 2,014/352/24/0 production and
15,698/733/142/12 test/support obligations; the consolidated 12-job static
gate is green, with the pinned Go restore failure tracked separately. Waves
109-112 were integrated below; Wave 113 is the next parallel source queue.

Wave 109 adds `tidb-planner::physical_union_scan`,
`tidb-stats::ddl_physical_ids`, and `tidb-exec::sum_int`. These preserve
UnionScan TiFlash rejection/index-join admission and initialization metadata,
DDL stats physical-ID selection including nil-versus-empty partition metadata
and dynamic global-ID append, and signed/unsigned non-DISTINCT SUM state with
checked Add/Sub overflow, NULL/empty, merge/reset, and outgoing-before-incoming
sliding order. Exact anchors are
`pkg/planner/core/casetest/dag/dag_test.go:274`,
`pkg/statistics/handle/ddl/ddl_test.go:203`, and the shared SUM anchors
`pkg/executor/aggfuncs/func_sum_test.go:33`/`:50`/`:66`/`:89`/`:133`.
The top-level SUM test rows stay under the existing aggregate test domain to
avoid duplicate ownership; focused Rust SUM_INT tests are recorded in the
source evidence note. Property/optimizer and executor wiring, DDL/session/
storage lifecycle, EvalInt/dispatch, chunk/memory/spill, and DISTINCT remain
external. The regenerated ledgers are 2,011/355/24/0 production and
15,696/735/142/12 test/support obligations; the consolidated 12-job static
gate is green, with the pinned Go restore failure tracked separately. Waves
110-112 were integrated below; Wave 113 is the next parallel source queue.

Wave 110 adds `tidb-planner::physical_show`, `tidb-stats::stats_cache_version`,
and `tidb-exec::percentile`. These preserve PhysicalShow/PhysicalShowDDLJobs
plan metadata and shared rejection gates, monotonic stats-cache version updates
with `skip_move_forward`, and bounded integer/real APPROX_PERCENTILE state with
NULL skipping, source-clearing merge, reset, exact ordinal rank selection, and
P=100 behavior. SHOW catalog/extractor/task/runtime wiring, cache atomics and
Handle lifecycle, typed percentile coercion/dispatch, introselect, chunk/memory,
and unsupported temporal/decimal/enum/set/bit variants remain external. Exact
anchors are `pkg/planner/core/planbuilder_test.go:63` (`TestShow`),
`pkg/statistics/handle/handletest/handle_test.go:111` (`TestVersion`), and
`pkg/executor/aggfuncs/func_percentile_test.go:35`/`:51`/`:63`.
The regenerated ledgers are 2,008/358/24/0 production and
15,690/741/142/12 test/support obligations; the consolidated 12-job static
gate is green, with the pinned Go restore failure tracked separately. Wave 111
is the next parallel source queue.

Wave 111 adds `tidb-planner::physical_lock`, `tidb-stats::topn_merge_task`,
and `tidb-exec::avg_float64`. These preserve PhysicalLock TiFlash rejection,
`Lock` plan metadata, query-block offset zero, opaque lock type, lossless wait
seconds, and exact ExplainInfo; the TopN merge-task range descriptor without
validation; and non-DISTINCT float64 AVG sum/count, NULL/empty behavior,
merge/reset, and incoming-before-outgoing sliding order. AST/catalog/task/lock
execution, TopN worker/concurrency/merge arithmetic, typed AVG coercion,
decimal/DISTINCT, rounding/context, chunk/memory, and spill remain external.
Exact anchors are `pkg/planner/core/tests/pointget/point_get_plan_test.go:407`,
`pkg/statistics/handle/globalstats/topn_bench_test.go:94`, and
`pkg/executor/aggfuncs/func_avg_test.go:27`/`:37`/`:48`. The regenerated
ledgers are 2,005/361/24/0 production and 15,685/746/142/12 test/support
obligations; the consolidated 12-job static gate is green, with the pinned Go
restore failure tracked separately. Wave 112 was integrated below; Wave 113 is
the next parallel source queue.

Wave 112 adds `tidb-planner::physical_table_dual`,
`tidb-stats::json_stats_version`, and `tidb-exec::minmax_deque`. These preserve
PhysicalTableDual `Dual` metadata, query-block offset, `rows:<RowCount>`
explain text, IndexJoin rejection, and row-count-dependent sort admission; the
old JSON StatsVer fallback where explicit versions win and missing positive
NDV/null-count infers version 1; and MinMaxDeque pair storage, deque
operations, reset, expiry dequeue, and monotonic max/min enqueue with equal
value eviction. Schema/catalog/task wiring, JSON/storage/session lifecycle,
typed MAX/MIN evaluation, window callbacks, chunk/memory, and spill remain
external. Exact anchors are
`pkg/planner/core/casetest/cbotest/cbo_test.go:367`,
`pkg/statistics/handle/storage/dump_test.go:582`, and
`pkg/executor/aggfuncs/func_max_min_test.go:335`/`:345`. The regenerated
ledgers are 2,002/364/24/0 production and 15,681/750/142/12 test/support
obligations; the consolidated 12-job static gate is green, with the pinned Go
restore failure tracked separately. Wave 113 is the next parallel source queue.

Wave 113 adds `tidb-planner::logical_lock`, `tidb-stats::stats_lock_table`, and
`tidb-exec::count_distinct_int`. These preserve raw lock discriminants and the
supported FOR UPDATE/FOR SHARE sets, fully qualified table-lock payloads with
nil-versus-explicit-empty partition-map semantics, and typed-int DISTINCT
NULL skipping, deduplication, cardinality, source-preserving partial merge, and
reset. Exact anchors are `pkg/planner/core/integration_test.go:1466`,
`pkg/statistics/handle/lockstats/lock_stats_test.go:186`/`:260`, and
`pkg/executor/aggfuncs/func_distinct_agg_test.go:26` plus
`pkg/executor/aggfuncs/func_count_test.go:115`. SQL/session/lock execution,
other DISTINCT types, typed Eval/chunk/memory/spill integration, and runtime
scheduling remain external. The regenerated ledgers are 1,999/367/24/0
production and 15,676/755/142/12 test/support obligations; the consolidated
12-job workspace, Clippy, formatting, parser, plan, ledger, and domain gates
are green. Wave 114 is now the next parallel source queue.

Wave 114 adds `tidb-planner::physical_exchange_receiver`,
`tidb-stats::pseudo_cache_policy`, and `tidb-exec::window_value_int`. These
preserve `ExchangeReceiver` plan identity, root offset zero, lossless uint64
stream-count metadata, and exact explain rendering; pseudo-statistics cache
admission below the partitioned threshold of 64 with temporary-table rejection;
and already-evaluated integer FIRST_VALUE/LAST_VALUE/NTH_VALUE transitions,
including NULL capture, batch-spanning selection, reset, and unreached output.
Exact anchors are `pkg/planner/core/integration_test.go:904`,
`pkg/statistics/handle/handletest/handle_test.go:1100`, and
`pkg/executor/aggfuncs/func_value_test.go:63`. MPP task/runtime wiring,
pseudo-table/cache/session lifecycle, typed evaluators, all value domains,
chunk/memory/window dispatch, and scheduling remain external. The regenerated
ledgers are 1,996/370/24/0 production and 15,673/758/142/12 test/support
obligations; the consolidated 12-job workspace, Clippy, formatting, parser,
plan, ledger, and domain gates are green. Wave 115 was integrated below; Wave
116 is now the next parallel source queue.

Wave 115 adds `tidb-planner::physical_selection`, `tidb-exec::spill_count`, and
`tidb-stats::cache_metrics_labels`. These preserve Selection plan identity,
caller-owned query-block offsets and exact condition/stream explain text;
native-endian int64 count-spill serialization, strict decoding, reusable
buffers, and sequential row consumption; and the six source-ordered cache
counter labels plus two gauge labels. Exact anchors are
`pkg/planner/core/casetest/mpp/mpp_test.go:673`,
`pkg/executor/aggfuncs/spill_helper_test.go:73`, and
`pkg/statistics/handle/cache/bench_test.go:99`. MPP/runtime wiring, typed
expression and aggregate domains, chunk/spill lifecycle, Prometheus handles,
cache concurrency, and session/storage integration remain external. The
regenerated ledgers are 1,993/373/24/0 production and 15,670/761/142/12
test/support obligations; the consolidated 12-job workspace, Clippy,
formatting, parser, plan, ledger, and domain gates are green. The evidence
fragment loader now rejects escaped `\t` headers. Wave 116 is now the next
parallel source queue.

Wave 116 adds `tidb-planner::physical_limit`, `tidb-exec::pd_approximate_count`,
and `tidb-stats::ddl_event_match`. These preserve Limit plan identity,
query-block offset, lossless offset/count metadata, and ExplainInfo redaction
branches over caller-owned partition/prefix text; the direct underscore-joined
approximate-count key plus bounded TTL/LRU hit/miss/eviction behavior with a
caller-supplied clock; and first-match DDL event selection with no-match
timeout behavior. Exact anchors are
`pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1600`,
`pkg/executor/internal/pdhelper/pd.go:69-85` plus
`pkg/executor/internal/pdhelper/pd_test.go:42`, and
`pkg/statistics/handle/autoanalyze/priorityqueue/queue_ddl_handler_test.go:885`.
Typed planner properties, PD/storage and restricted-SQL access, channel/ticker
timing, notifier decoding, and full planner/executor/statistics/session/SQL
lifecycle remain external. The regenerated ledgers are 1,990/376/24/0
production and 15,667/764/142/12 test/support obligations; the consolidated
12-job workspace, Clippy, formatting, parser, plan, ledger, and domain gates
are green. Wave 117 is now the next parallel source queue.

Wave 117 adds `tidb-planner::physical_union_all`, `tidb-exec::apply_cache`,
and `tidb-stats::mock_statistics_shape`. These preserve Union plan identity,
query-block offset, MPP flag, and source Exhaust gates/candidate ordering;
byte-key/value memory charge, over-quota rejection, oldest-entry LRU eviction,
and get-touch/accounting behavior; and fixture column/index counts with
CMSketch/TopN/histogram switches plus total item count. Exact anchors are
`pkg/planner/core/casetest/mpp/mpp_test.go:446`,
`pkg/executor/internal/applycache/apply_cache.go:35-43,76-101` plus
`pkg/executor/internal/applycache/apply_cache_test.go:30`, and
`pkg/statistics/handle/cache/bench_test.go:125`. Child planner properties,
typed chunk/memory/session quota, statistics allocation/cache concurrency, and
runtime/benchmark integration remain external. The regenerated ledgers are
1,987/379/24/0 production and 15,664/767/142/12 test/support obligations; the
consolidated 12-job workspace, Clippy, formatting, parser, plan, ledger, and
domain gates are green. Wave 118 is now the next parallel source queue.

Wave 118 adds `tidb-planner::physical_apply`, `tidb-exec::next_io_acc`, and
`tidb-stats::stats_request_matcher`. These preserve Apply plan identity and
offset plus the exact non-PhysicalJoin boundary; positive row/cell guards,
reset/reuse, wrapping accumulation, and child/parent/tracking admission; and
the exact `internal_StatsForegroundPriority` predicate and matcher description.
Exact anchors are
`pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1537`,
`pkg/executor/internal/exec/executor.go:42-89` plus
`pkg/executor/internal/exec/executor_test.go:35`, and
`pkg/statistics/handle/util/test/ctx_matcher.go:24-36` plus
`pkg/statistics/handle/autoanalyze/autoanalyze_test.go:407`. Hash-join/subquery
runtime, executor atomics/provider/pool/RUV2, context/request propagation,
gomock/SQL/session lifecycle, and full integration remain external. The
regenerated ledgers are 1,984/382/24/0 production and 15,661/770/142/12
test/support obligations; the consolidated 12-job workspace, Clippy,
formatting, parser, plan, ledger, and domain gates are green. Wave 119 is now
the next parallel source queue.

Wave 119 adds `tidb-planner::physical_projection`, `tidb-exec::cluster_index_id`,
and `tidb-stats::predicate_column_query_mode`. These preserve Projection plan
identity/offset, opaque expression-list rendering, and the uint64 stream-count
suffix; clustered-index identity selection for PK-as-handle, common-handle
primary indexes, and rowid/non-clustered tables; and the exact predicate-column
transaction boundary (`LoadColumnStatsUsage` without `FlagWrapTxn`,
`GetPredicateColumns` with it). Exact anchors are
`pkg/planner/core/casetest/mpp/mpp_test.go:710`,
`pkg/executor/internal/exec/indexusage.go:130-148` plus
`pkg/executor/internal/exec/indexusage_test.go:447`, and
`pkg/statistics/handle/usage/predicate_column.go:47-62` plus
`pkg/statistics/handle/usage/predicate_column_test.go:103`. Typed projection,
table/index collector, session-pool/SQL, and full planner/executor/statistics
integration remain external. The regenerated ledgers are 1,981/385/24/0
production and 15,658/773/142/12 test/support obligations; the consolidated
12-job workspace, Clippy, formatting, parser, plan, ledger, and domain gates
are green. Wave 121 is now the next parallel source queue.

Wave 120 adds `tidb-planner::physical_shuffle`, `tidb-stats::index_usage_key`,
and `tidb-exec::mock_global_accessor`. These preserve `Shuffle` plan identity
and query-block offset, hash/range splitter discriminants, and source-shaped
concurrency/data-source ExplainInfo; the exact table-ID/index-ID lookup pair
used by index-usage GC; and ordinary/test-suite variable maps, unknown-variable
errors, default authentication plugin validation plus its bypass setter, and
`tikv_gc_life_time` readback. Exact anchors are
`pkg/planner/core/operator/physicalop/physical_shuffle.go:155` plus
`pkg/planner/core/casetest/integration_test.go:245`,
`pkg/statistics/handle/usage/index_usage.go:59-62` plus
`pkg/statistics/handle/usage/index_usage_integration_test.go:29`, and
`pkg/sessionctx/variable/mock_globalaccessor.go:23-130` plus
`pkg/sessionctx/variable/mock_globalaccessor_test.go:26`. Live planner
partitioning/receivers, index-usage collection/GC/workers, SessionVars hooks,
context cancellation, SQL error/OpenCensus cleanup, and full integration remain
external. The regenerated ledgers are 1,978/388/24/0 production and
15,655/776/142/12 test/support obligations; parser, workspace, Clippy,
formatting, and static ledger/parser/plan/domain gates pass with 12 jobs. Wave
121 is now the next parallel source queue.

Wave 121 adds `tidb-planner::physical_exchange_sender`,
`tidb-stats::stats_table_snapshot`, and `tidb-exec::vec_group_checker_int`.
These preserve `ExchangeSender` identity/root offset zero and ExplainInfo
exchange labels, compression names/fallback, hash-column text, ordered task
IDs, and uint64 `stream_count`; the `AssertTableEqual` realtime/modify counts,
column/index cardinality, per-ID item/payload/nil shape, and existence bytes;
and integer/NULL group boundaries, cross-chunk first-group continuity,
offsets/count, cursor ranges, exhaustion/reset, and the non-empty-chunk error.
Exact anchors are `pkg/planner/core/operator/physicalop/physical_exchange_sender.go:222`
plus `pkg/planner/core/casetest/mpp/mpp_test.go:78`,
`pkg/statistics/handle/internal/testutil.go:25-55` plus
`pkg/statistics/handle/handletest/statstest/stats_test.go:307`, and
`pkg/executor/internal/vecgroupchecker/vec_group_checker.go:80-151,524-564`
plus `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:141`.
MPP runtime, statistics table/payload/storage lifecycle, expression/chunk and
codec evaluation, collations, non-integer/vector groups, and stream aggregation
remain external. The regenerated ledgers are 1,975/391/24/0 production and
15,652/779/142/12 test/support obligations; parser, workspace, Clippy,
formatting, and static ledger/parser/plan/domain gates pass with 12 jobs. Wave
122 is now the next parallel source queue.

Wave 122 adds `tidb-planner::physical_window`, `tidb-exec::concurrent_entry_map`,
and `tidb-stats::stats_cache_inner`. These preserve Window plan identity,
initialization offset, inherited uint64 fine-grained-shuffle stream-count clone
state, and the optional ExplainInfo suffix; 320-shard routing,
lock-protected prepend chains, lookup/snapshot iteration, length/empty, row
identity, and portable accounting; and the eleven-method cache interface
(`Get`, `Put`, `Del`, `Cost`, `Values`, `Len`, `Copy`, `SetCapacity`, `Close`,
`TriggerEvict`, and `WaitForAsyncUpdates`) over opaque values. Exact anchors
are `pkg/planner/core/operator/physicalop/physical_window.go:480` plus
`pkg/planner/core/plan_test.go:681`, `pkg/executor/join/concurrent_map.go:20-79`
plus `pkg/executor/join/concurrent_map_test.go:27,70`, and
`pkg/statistics/handle/cache/internal/inner.go:18-50` plus
`pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go:49`. PhysicalSort
sharing, MPP runtime, the Go memory-map ABI/constants and hash-join trackers,
LFU admission/eviction/async/metrics, and full statistics storage lifecycle
remain external. The regenerated ledgers are 1,972/394/24/0 production and
15,648/783/142/12 test/support obligations; parser, workspace, Clippy,
formatting, and static ledger/parser/plan/domain gates pass with 12 jobs. Wave
123 is now the next parallel source queue.

Wave 123 adds `tidb-planner::physical_sort`, `tidb-exec::join_table_meta`, and
`tidb-stats::StatsPool`. These preserve Sort identity/offset, source ByItems
text with `:desc`, partial-sort/stream-count metadata, deep clone and monotonic
memory accounting; hash-join key mode, inlining/fixed-length, mixed-sign and
variable serialization modes, row-column ordering, null-map alignment, and
thread-safe null-map reads; and the opaque goroutine/session pool access plus
close boundary. Exact new planner anchors are
`pkg/planner/core/physical_plan_test.go:582` and
`pkg/planner/core/planbuilder_test.go:277`; the shared
`pkg/planner/core/plan_test.go:681` remains singly owned by Wave 122. Executor
anchors are all six top-level tests in
`pkg/executor/join/join_table_meta_test.go:27-274`; the statistics anchor is
`pkg/statistics/handle/util/util_test.go:75`. Typed planner/expression/runtime
wiring, join row encoding/execution and live FieldType/collation/chunk/codec
behavior, concrete pool construction/session cleanup, and complete statistics
lifecycle remain external. The regenerated ledgers are 1,969/397/24/0
production and 15,639/792/142/12 test/support obligations. The batched
workspace tests passed; after strict Clippy requested source-equivalent
`div_ceil`, the focused six-test join-metadata suite and full workspace Clippy
passed, along with formatting and all static ledger/parser/plan/domain gates.
Wave 124 is now the next parallel source queue.

Wave 124 adds `tidb-planner::physical_topn`,
`tidb-exec::OrderedApplyBuffer`, and `tidb-stats::BoundedMinHeap`. These
preserve TopN identity/offset, ordinary and independent normalized
ByItems/PartitionBy formatting, redaction/prefix/clone/memory metadata; ordered
parallel Apply sequence buffering, consecutive drain, empty advancement, full
and idle partial flush, EOF/error/cancellation termination, and nested
composition; and the complete generic bounded-heap comparator/capacity/
replacement/tie/sorting/constructor contract. Exact owners are
`pkg/planner/core/planbuilder_test.go:340`,
`pkg/planner/core/integration_test.go:1897`, all seven ordered tests in
`pkg/executor/parallel_apply_test.go:560-969`, and all seven heap tests in
`pkg/util/generic/bounded_min_heap_test.go:44-186`. Optimizer/task/PB/storage
TopN wiring, live Apply executors/chunks/channels/correlation/joiners/SQLKiller
timing, and statistics histogram/TopN consumers remain external. The
regenerated ledgers are 1,966/400/24/0 production and
15,623/808/142/12 test/support obligations. The reused-target workspace test
batch passed; after strict Clippy exposed the missing Rust `is_empty`
companion, the focused nine-test heap suite and full workspace Clippy passed,
along with formatting and all static ledger/parser/plan/domain gates. Wave 125
is now the next parallel source queue.

Wave 125 adds three complete source-file owners:
`tidb-planner::physical_table_reader` for request/store types, clone shape,
scan metadata, explain text and memory; `tidb-exec::statement_rows_reader` for
bounded buffer/pull/EOF/error/close lifecycle; and
`tidb-distsql::distsql_runtime` for DAG/MPP/ANALYZE/CHECKSUM metadata, native
Go-`int`-width runtime plan IDs, memory/paging/chunk policy, TiFlash outgoing
settings, endian/alignment selection, and KV counter binding. Their 12 exact
original tests are the two physical-table-reader anchors, three statement
summary anchors, and seven `pkg/distsql/distsql_test.go` anchors at lines 42,
61, 73, 82, 106, 154, and 179. Source/test ledgers now report
1,963/403/24/0 and 15,611/820/142/12
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). DistSQL transport/RPC, protobuf and
response streaming, concrete tracker lifecycle, and full runtime-stat
aggregation remain external.

The post-Wave-125 throughput refactor replaces hand-paired micro-wave dispatch
with `scripts/work-unit-queue.py`: checked-ledger candidates are instant,
active source/test claims are atomic and overlap-checked, and stale anchors
fail before code is edited. `scripts/rewrite-gate.sh` now separates zero-build
status, focused leaf, static evidence, and steward-only integration scopes with
one reused 12-job target. The first test-harness consolidation reduces
`tidb-exec` from 103 test binaries to 10 and the workspace from 361 to 268
test targets while preserving all 103 source test files and the normalized
620-test union. Full workspace tests, strict workspace Clippy, the focused
11-test DistSQL runtime suite, formatting, Python claim/queue tests, parser
dependency isolation, and all static source/test/parser/plan/domain gates pass.

The next throughput increment replaces single-file claims with schema-2 atomic
multi-source claims and checked records under `workstreams/slices/`. Each
record names a real consumer, exact original tests, a focused target, and
prerequisites; `ready` hides claimed or dependency-blocked work. The first
adversarial batch rejected a proposed ranger leaf because it introduced a
second scalar authority and bypassed encoded-key/collation ordering. The
accepted EXPLAIN increment now contains only executor-owned lifecycle behavior,
and CMSketch corrected zero-limit TopN spill, source-default tie ordering,
packed protobuf counters, embedded TopN decoding, and strengthened coding/GEE
tests. Focused EXPLAIN tests passed 1 external plus 7 internal; CMSketch passed
19. The complete workspace test phase, strict workspace Clippy, Python queue
tests, parser dependency isolation, and every static evidence gate pass. The
honest post-batch ledgers are 1,962/404/24/0 production and
15,604/827/142/12 test/support obligations.

The first dependency-slice batch is integrated and its claims are released.
`tidb-datatype::field_type` is now one deep, non-`Copy` parser/runtime
authority with the exact 29x29 merge table, unknown-byte zero-map-index
behavior, aggregation/value inference, source formatting, and independently
owned ENUM/SET binary-literal markers. Six complete FieldType test anchors are
`COVERED`; parser-to-FieldType consumers, real Datum conversion, restore/wire
metadata, and broad type-modification paths keep both source files and the
remaining tests honestly `PARTIAL`. `tidb-stats::estimate` consolidates all
five global-singleton/GEE functions behind the existing FMSketch authority;
valid estimator/global-merge cases are covered, while exact Go `intest`
runtime assertions and the original Datum-to-codec-to-Murmur negative sample
remain partial. The proposed parsed SQL EXPLAIN consumer was rejected and
deleted because it fabricated optimizer plans inside `tidb-exec`; only the
dependency-closed planner format/schema/tree-renderer and LineFields leaf
remains, with SQL tests partial until PlanBuilder, physical optimization,
FlattenPhysicalPlan, and ExplainExec form one real route.

The queue now validates exact source/test capability prerequisites and minimum
evidence status instead of inferring readiness from a broad owner. Schema-2
claims named after a checked slice must exactly match its source/test anchors,
and stale claims can always be released for repair. The accepted batch passes
focused FieldType 14/14, statistics 10/10, planner EXPLAIN 3/3, executor
EXPLAIN 1/1, and codec 3/3 tests, followed by the full workspace test, strict
all-target Clippy, formatting, 12 Python queue tests, parser dependency
isolation, and every static source/test/parser/plan/domain gate with 12 jobs.
Current ledgers are 1,958/408/24/0 production and
15,580/839/154/12 test/support obligations; active claims are zero. Use
`scripts/work-unit-queue.py ready` and `claim-slice`; never fill an unmet
dependency with an invented local abstraction.

The next reviewed vertical batch is integrated. `tidb-error` is now the shared
MySQL/TiDB errno, message, SQLSTATE, SQLError, and redaction authority consumed
by protocol, transaction, and executor error paths; Enabled redaction preserves
Go's replacement-before-format behavior for `%s`, `%d`, `%v`, and `%#v`.
`tidb-datatype::Datum` now carries MinNotNull/MaxValue sentinels, key encoding
and the separate DecodeRange boundary are source-shaped, and expression/exec
consumers explicitly reject range bounds instead of silently producing SQL
NULL. Planner ranger consolidation remains open. `tidb-txnkv::KeyRanges` ports
all split/storage shapes, exact Go byte quoting, logical equality, safe
protobuf conversion, and immutable Rust ownership; the unsafe Go alias and
104-byte layout assertion remain partial. Source-only and test-only ownership
transfers are now first-class checked queue records. The complete 12-job
integration gate passes workspace tests, strict all-target Clippy, formatting,
16 queue regressions, every source/test/parser/plan/domain inventory, parser
dependency isolation, and diff checks. Current ledgers are
1,951/410/29/0 production and 15,568/843/163/11 test/support obligations.

The next dependency batch is integrated and its claims are released.
`tidb-distsql::paging` directly ports wrapping growth, the configured cap and
floor, and every seek-count boundary; `PagingConfig::default()` now consumes
that same source-default authority. `tidb-datatype::format` ports the shared
indent/flat formatter state machine, cross-call indentation and dangling-`%`
`NOVERB` behavior, and the valid-UTF-8 `OutputFormat` replacement table; wider
Go `fmt` verbs/flags, diagnostic text, partial-write accounting, and invalid
UTF-8 remain explicit gaps. `tidb-txnkv::UnionIter` ports forward/reverse merge
order, dirty precedence, tombstones, error order, idempotent close, and an
owned constructor-error path that returns both still-open input iterators; the
real transaction-driver consumer remains external. Active claims now also
serialize exact declared Rust output paths, preventing two agents from editing
the same integration seam even when their Go anchors do not overlap. The
complete 12-job integration gate passes workspace tests, strict all-target
Clippy, formatting, 17 queue regressions, all source/test/parser/plan/domain
inventories, parser dependency isolation, and diff checks. Current ledgers are
1,948/412/30/0 production and 15,560/845/169/11 test/support obligations.

The throughput rule is now explicit: agents inspect and translate complete
source/test ownership envelopes without running Cargo; one steward batches all
accepted envelopes into a reused 12-job target and pays the workspace build,
Clippy, formatting, and static-gate cost once. Parallelism is bounded by real
Go ownership, dependency prerequisites, and declared Rust output paths rather
than package names. The next wave is open concurrently on expression field-name
resolution, coprocessor-cache key/admission behavior, and an independent
transaction dependency leaf.

That three-lane wave is integrated and released. Shared datatype field-name
metadata replaces the executor duplicate; `tidb-expr::find_field_name` ports
qualified matching, non-explicit filtering, redundant preference, ambiguity,
and the original benchmark shapes while consuming the central MySQL error
catalog. Executor projection uses the ordinary qualified/ambiguous path; it
does not yet carry redundant/non-explicit flags. `tidb-distsql::copr_cache`
ports exact cache-key bytes, paging-marker equivalence, 64-bit value cost, and
the four source-tested admission/configuration contracts; Ristretto lifecycle
and non-finite/out-of-range Go float conversion remain open.
`tidb-txnkv::BufferBatchGetter` ports buffer/middle/snapshot precedence,
tombstones, snapshot staging, commit timestamps, and the complete original
test, while the distinct TiDB-to-client-go error adapter and shared complete
ValueEntry/options authorities remain external. The batched gate caught and
fixed signed-`int` drift, a false not-found identity, and the benchmark's
intentional case-insensitive duplicate outcome. Full workspace tests, strict
all-target Clippy, formatting, 17 queue regressions, and every static gate pass
with 12 jobs. Current ledgers are 1,944/416/30/0 production and
15,550/849/175/11 test/support obligations; active claims are zero.

The next dependency wave is integrated through the guarded release path.
`tidb-datatype::truncate` ports the exact ten-code truncation policy and wires
BinaryLiteral `ToInt` through strict/warn/ignore behavior while preserving the
Go value-plus-error result; generic cause traversal, dbterror class/RFC
identity, full conversion flags/location, and the warning-publication owner
remain explicit prerequisites. `tidb-distsql::ReadBytesEma` ports the
mutex-protected one-second decay, source-zero timestamp semantics, seed/first
sample behavior, nonmonotonic clamp, and concurrent prediction; its future
clock adapter must supply source-zero-compatible timestamps, and protobuf
read-byte extraction/RPC feedback plus out-of-range float conversion remain
open. `tidb-txnkv::driver_error` ports recognized root-cause conversion,
exact oversize messages, and chain-preserving unknown passthrough; the shared
terror ClassGlobal result-undetermined identity and remaining client/PD/error
families remain partial. The complete integration gate passes workspace tests,
strict all-target Clippy, formatting, 18 queue regressions, parser isolation,
and every static inventory with 12 jobs. Current ledgers are
1,941/419/30/0 production and 15,538/852/184/11 test/support obligations.

Queue lifecycle now distinguishes abandonment from successful integration.
`release --owner <slice> --integrated` requires the checked slice to be
`partial` or `covered`; recovery now requires explicit `--abandon`, and a
release with neither mode is rejected. This closes the bug
that left the already integrated output-format slice marked `ready` and
silently offered it again. The conversion-context slice remains correctly
blocked on the warning handler and complete truncation-context authority rather
than bypassing those missing prerequisites.

The authority-first follow-up wave is integrated and all claims are released.
`tidb-error::terror` now owns the fixed 27-class catalog, four shared codes,
class/code RFC identities, root-cause equality, generated registered messages,
and SQL conversion for registered instances plus the fixed Global prototypes.
The review caught and fixed an important source mismatch: registration is an
identity property in Go, so a synthesized `global:2` still converts as code 2;
it cannot be modeled only as provenance on one Rust value. Transaction-driver
`ResultUndetermined` now consumes that shared identity, making the complete
original wrapper-shape conversion test covered. JSON wire compatibility,
logging/fatal helpers, mutable registration/freeze, and Go stack capture remain
explicit terror gaps.

`tidb-exec::warning_publication` is now the sole warning-handler authority.
It ports capped single append, source batch-cap behavior, copy-capacity reuse
without handler aliasing, independent truncation/copy, reset/set/snapshot,
wrapping error counts, IgnoreWarn, and the original JSON level/message rows;
all four original warning tests are covered, while typed terror JSON identity,
arbitrary custom levels, zero-copy slices, func appenders, and live session
attachment remain source-level gaps. A proposed DistSQL replica-read adapter
was deleted after review because it invented a storage-client enum before the
canonical TiKV client exists. Keep that source untriaged until the real client
boundary can own the mapping. The complete 12-job integration gate passes
workspace tests, strict all-target Clippy, formatting, 18 queue regressions,
parser isolation, and every static inventory. Current ledgers are
1,940/420/30/0 production and 15,527/851/193/14 test/support obligations;
active claims are zero.

The next three-lane dependency wave is also integrated and released.
`tidb-datatype::conversion_context` is now the single ten-bit conversion flag,
strict/default context, opaque location, and warning-input authority.
BinaryLiteral `ToInt`, the truncation policy, executor error policy, statement
pushdown, and the existing `StaticWarningHandler` consume it; the duplicate
statement type-flag carrier is gone. Both original context tests and their file
obligation are covered. The source remains partial only where Rust accepts
typed terror warnings rather than arbitrary Go errors and carries location
identity without timezone transition rules.

`tidb-distsql::tiflash_replica_read` completely ports the three policies,
predicates, exact strings, raw/string fallbacks, and closest-replica remote-read
limit, and the existing `DistSqlContext -> ReadRequestMetadata ->
KvRequestMetadata` path consumes it. This is covered and has no original Go
test file. `tidb-txnkv::row_key_prefix_filter` ports the inverted prefix
predicate over the existing owned `Key` and `next_until`, including the exact
embedded-NUL test; ScanMetaWithPrefix/DelKeyWithPrefix remain open until a real
mutable transaction/retriever exists. Integration moved the conversion sink
implementation into `warning_publication.rs`, its actual owner, so library and
source-shard compilation share one module graph. The complete 12-job gate
passes workspace tests, strict all-target Clippy, formatting, 18 queue
regressions, parser isolation, and every static inventory. Current ledgers are
1,937/422/31/0 production and 15,523/851/197/14 test/support obligations;
active claims are zero.

Wave 132 is integrated and all three receipted claims are released. The
existing executor aggregate path now has one encoded-tuple DISTINCT authority
for scalar folds, multi-argument COUNT, and GROUP_CONCAT. `TestDistinct` and
the complete original test file are covered. GROUP_CONCAT checks the full
evaluated tuple before rendering and calls the existing byte-preserving Datum
CONCAT authority directly, so tuple boundaries survive without a lossy AST
round trip. This is connected through `Database::run`; aggregate SELECT is
still deliberately outside the narrower shared Session/COM_QUERY capability.

The transaction fault-injection leaf no longer carries a duplicate `KvRead`
value/batch model: it consumes `Getter`, `BatchGetter`, `ValueEntry`, and `Key`,
and keeps the source read lock across delegation. Treat it only as a
future-client consolidation. There is no production `KvStorage` implementation
or real TiKV path, and option/context, pair-result, nil-map, root-cause, and
Begin-wrapper differences remain partial. The live Session result path now
uses one published status snapshot for COM_QUERY OK/EOF packets: affected rows
reach DML OK, caller warnings cannot override the published count, and
connection status/capability bits remain connection-owned. Warning producers,
GetWarnings iteration, nonempty info, and live shared-cluster auto-ID remain
open.

The integration process is now structurally safer and faster. `integrate`
freezes all checked claims and gate inputs before strict all-target Clippy and
the workspace tests, verifies byte identity afterward, then issues one-time
per-claim receipts. Mid-gate edits, a second begin, undeclared Rust edits,
post-gate code/test/script/domain-manifest edits, and implicit plain release are
rejected; only checked evidence/generated-ledger/slice-status/handoff promotion
is allowed after the receipt. Clippy runs before the test sweep to fail compile
and lint errors earlier. The queue tool has 24 regressions covering these
invariants. The final reused-target 12-job gate passes Clippy, all workspace and
differential tests, formatting, parser isolation, every inventory, and diff
checks. Current ledgers are 1,935/424/31/0 production and
15,520/852/199/14 test/support obligations; active claims and ready slices are
zero.

Wave 133 is integrated and both receipted integration claims are released.
`tidb-codec` now owns canonical table-record prefixes and row keys, while
`tidb-distsql::table_handles_to_kv_ranges` consumes the existing typed
Int/Common/Partition handles to coalesce sorted integer runs, preserve physical
partition boundaries, create common-handle point ranges, and publish exact
row-count hints into the existing pre-transport `KvRequestBuilder`. Both
original request-builder tests are covered. This remains metadata-builder
reachability only: session, RPC, region splitting, and TiKV execution are open.

The shared Session now admits only dependency-closed table-less top-level
COUNT. Execution uses the existing canonical aggregate/group owner over one
synthetic input row rather than a second evaluator, and `WHERE false` preserves
the empty scalar-group count. Automatic COM_QUERY metadata is source-exact:
LONG_LONG, binary plus not-null flags, length 21, decimal 0, and binary
collation. The first live nonzero warning producer is also connected:
`tidb_enable_noop_functions=WARN` and the two read-only aliases append the
existing diagnostic directly into canonical `StatementStatus`, whose count is
published through COM_QUERY OK without caller spoofing and resets at the next
statement boundary. Table-backed aggregates, warning errno/SHOW WARNINGS,
global no-op variables, other warning producers, and planner-dependent shapes
remain explicit gaps.

The parallel process now follows mutable Rust write sets before semantic Go
domains. Two individually valid session lanes both touched `cluster.rs`; their
evidence remains separate, but they were consolidated into one Wave-133
integration claim rather than hiding the overlap. The queue also supports
atomic `amend --source` plus `--test`, so a newly discovered authoritative Go
anchor no longer requires abandon/reclaim churn. Its 25 regressions cover the
new source amendment and prior receipted-gate invariants. Cross-review and the
gate caught wrong COUNT wire metadata, warning admission that rejected its own
live SET statements, missing request-builder source ownership, false runtime
connectivity wording, invalid CommonHandle fixtures, and aggregate execution
misrouting. The final reused-target 12-job gate passes strict Clippy, the full
workspace and differential test sweep, formatting, parser isolation, and all
inventories. Current ledgers are 1,933/426/31/0 production and
15,515/855/201/14 test/support obligations; active claims and ready slices are
zero.

Wave 134 is integrated and all three receipted claims are released. The
canonical DistSQL context now owns `EnableChunkRPC`, detach preserves it, and
the existing encoding policy consumes it with the explicit alignment gate;
protobuf layout and production RPC wiring remain open. Region-task envelopes
now expose the exact seven-helper location-coverage translation, with all 32
generated Go cases plus nil-location coverage; this is a callable test boundary
until a real task builder invokes it. Planner and executor aggregate consumers
now share one `AggFuncDesc`/`AggFunctionMode`/`AggregateKind` authority, the
complete `TestAggFuncDesc` hash mutation vector, and the preserved exact COUNT
metadata contract.

Independent review caught runtime-connectivity and aggregate-dispatch wording
overclaims plus missing evidence write paths. Static generation then caught a
duplicate `base_func.go` owner during the COUNT-to-descriptor transfer, and the
first strict-Clippy gate caught one test-only import at module scope. After
focused repairs, the single reused-target 12-job gate passes strict all-target
Clippy, full workspace/differential tests, formatting, parser isolation, plan
inventory, all ledgers, and 25 queue regressions. Current ledgers are
1,929/430/31/0 production and 15,512/857/202/14 test/support obligations;
active claims and ready slices are zero.

The next throughput step is campaign mode, not more tiny waves. Root must keep
six disjoint ready slices (two three-agent batches) scoped ahead, prefer closing
or consolidating existing PARTIAL families, pre-register leaf directories so
agents avoid shared routing files, and normally batch at least nine production
files or fifty original obligations into one expensive gate. The exact policy
is in `PARALLEL.md`. The next dispatcher work should create that ready backlog
before assigning more implementation.

Campaign `2026-07-read-path-01` proves that execution model end to end. Six
disjoint vertical slices were frozen across txn read/error handling, DistSQL
region construction/response consumption, aggregate runtime, and typed join
runtime. Together they transition 24 authoritative Go source files and 71
exact original test/support obligations. Feature agents translated and
cross-reviewed source-shaped leaves without running private workspace builds;
root integrated both three-agent batches through one persistent 12-job Cargo
target. The gate caught and repaired semantic boundary defects in interceptor
lifecycle, region coverage, lazy response ordering, aggregate numeric types,
USING-column visibility, predicate pushdown, metadata preservation, and exact
write-conflict diagnostics. The final shared gate passes strict Clippy, the
full workspace/differential test sweep, formatting, parser isolation, plan
inventory, ledgers, and 34 queue regressions. Its receipt was used to release
all six claims; the campaign is `integrated`, with zero active claims.

The Rust workspace has a tracked Campaign 07 baseline. Create one branch and
one Git worktree per claimed slice, retain one shared Cargo cache policy, and
merge only frozen evidence-backed slices. Claims are still acquired by the
primary dispatcher before worktree creation; ignored local claim leases are
not copied or committed as ownership evidence. Continue replacing append-only
wave prose with the generated current-status page plus short durable decision
records; this handoff and the design are too large for the dispatcher hot path.

Campaign `2026-07-read-path-02` is integrated and its six receipted claims are
released. It closes one batch across paging continuation, streaming result-set
lifecycle, RIGHT/NATURAL join runtime, prefix transaction operations, MAX/MIN,
and live LEAD/LAG, covering 13 Go sources and tracking 52 exact original
obligations. Campaign `2026-07-runtime-closure-03` is also integrated and all
six claims are released. It transitions 13 Go sources and tracks 56 exact
obligations across window ranking, variance, bit aggregates, internal
FIRST_ROW, coprocessor cache, and driver-error conversion. The shared 12-job
gate passed after its focused repair loop exposed public API documentation,
window metadata result-shape, typed DOUBLE fixture, strict-Clippy, and incorrect
Cluster-vs-Database consumer-boundary defects. The generated dispatcher page,
not this prose, remains the current-state authority.

Campaign `2026-07-runtime-closure-04` is integrated and all six receipted
claims are released. It transitions nine Go production sources and tracks 122
exact original obligations across the complete DistSQL request envelope,
compressed PacketIO, CMSketch/TopN estimation, live GROUP_CONCAT, the
auto-analyze priority heap/job/queue, and parser arena/slab behavior. Independent
review caught empty compressed-envelope EOF handling, RequestBuilder setter
order and zero-concurrency semantics, nil CMS protobuf encoding, missing live
estimator consumers, bounded ordered GROUP_CONCAT eviction/truncation, static
partition identity, DML version filtering, and retry loss. The single reused
12-job gate then caught large-array test construction, strict lints, an
over-strengthened million-unique CMS assertion, compressor-specific byte-length
overclaims, and an inconsistent priority test weight. Exact Go zlib/zstd writer
byte lengths remain explicitly `PARTIAL`: Rust emits valid streams with
different encoder bytes, while exact Go-produced reader fixtures, protocol
headers, sequence behavior, decoded lengths, and round trips pass. Current
generated ledgers are 1,922/434/34/0 production and
15,359/910/302/14 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED), with zero active claims.

Campaign 05 is integrated: its six receipt-backed connected slices cover mutable
transactions over an injected MemBuffer backend, the `distsql.go`
request-to-response bridge, complete auto-analyze jobs, compressed command I/O,
one canonical physical-partition window runtime, and a bounded
LogicalDataSource-to-index-task planner seam. Together they transition 32 Go
production sources and track 130 exact original obligations. The final 12-job
gate passed workspace Clippy and tests, queue/dashboard/ledger checks, parser
isolation, plan inventory, and diff validation. The planner seam constructs an
unordered TiKV Cop single-read `PhysicalIndexScanPlan` only with explicit
source-owned cardinality, exact upstream ExpectedCnt, and point-get admission;
represented empty index ranges produce `TableDual`, while all unsupported paths
reject explicitly. The previous ranking and LEAD/LAG manifests are `retired`
transfer predecessors and no longer represent active ownership.

Post-Campaign-05 ownership consolidation is also receipted. The
`datatype-value-context-and-format` manifest atomically replaces five
overlapping datatype predecessors and owns exactly six Go sources plus 18
original test anchors. This is deliberately an ownership bundle, not a new
runtime pipeline: FieldType metadata and OutputFormat, conversion-produced
truncation and Context policy, and Datum rendering/sentinel ordering remain on
their independent Go call paths. Independent review found that a proposed
cross-source formatter invented compatibility and warning semantics and was
wrong for BIT/signedness admission, invalid UTF-8, and float rendering; the
production adapter was deleted rather than patched. The source-shaped smoke
target now proves only those three independent paths. A separate
`error-catalog-and-terror-identity` manifest consolidates seven Go sources and
15 anchors while preserving its catalog and terror gaps as `PARTIAL`.

The official 12-job integration gate passed formatting, strict workspace
Clippy, the full workspace tests, 38 queue/dashboard regressions, every checked
source/test/parser/plan inventory, parser dependency isolation, and diff
validation. Receipt claim SHA-256 is
`1f884fba2fddbab06a0fd59feaddeef1d2a409d1e46f4535aa82768da6b125ee`; the
datatype claim is released and the queue has zero active claims. Current
generated ledgers remain 1,914/440/36/0 production and
15,307/953/311/14 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED).

The exact five-source/five-anchor expression successor is implemented,
independently approved, and released from `integration_receipt 1`. Its
bounded live closure accepts only aliased `COUNT(t.a)` over one unqualified
catalog table: it validates the qualified field, preserves NULL exclusion and
empty-input zero, publishes exact fixed COUNT metadata, and reaches framed
COM_QUERY. Aliasless, schema-qualified, multi-field, join/filter/group/window,
ordering, and limit forms reject. Do not describe this as a shared bound
`AggFuncDesc` pipeline: descriptor identity, execution, and full FieldName
ambiguity/redundancy rules remain independent `PARTIAL` authorities.

Campaign `2026-07-runtime-closure-06` is integrated and all three
receipt-backed claims are released. It owns exactly 12 Go production sources
and 74 original obligations across raw DistSQL response ownership (2/15), the
connected warning/status publication path (5/11), and the bounded coprocessor
read-task coordinator (5/48). `query_runtime` and `cop_paging` are now public
module registries, while new runtime files are nested below their owning
module so parallel agents avoid a shared `lib.rs` edit.

Session now routes live warnings through one `StaticWarningHandler` owned by
`StatementStatus`, snapshots once at finish, and preserves Go's single-append
cap, uncapped batch/set retention, and wrapping OK/EOF `uint16` counts.
DistSQL owns raw response subsets until one-way conversion to
`SelectResponseIter`; Analyze and Checksum remain raw. Cop-read composes
checked tasks, per-attempt cache preparation/restoration, iterator-wide EMA
and wrapping task indexes, paging, and bounded response publication. Embedded
errors and backpressure reject before cache, EMA, paging, channel, or
in-flight mutation.

All three remain `PARTIAL`. Open gaps include real RegionCache/PD,
locks/backoff/endpoints/RPC/cancellation, shared unordered dispatch, Ristretto
and unused-topology parity, production table-reader and concrete memory
wiring, response Close-error and subset-plus-error representation, SHOW
WARNINGS/errno identity, broad SessionVars/SysVar plumbing, and other warning
producers.

Integrated campaign membership is archived in
`workstreams/campaigns/integrated-members.tsv`. Planned/active campaigns must
still meet the 9-source/50-obligation admission floor; later ownership
transfers may shrink live historical manifests without changing what an
integrated receipt contained. All 35 queue regressions enforce this boundary.

The expression gate initially caught `COUNT(t.missing)` falling through to the
legacy generic evaluator. A first repair bound the snapshot too early and
independent review caught that it skipped failed-SELECT session effects. The
final root fix binds inside `Database::run_select` on each cloned catalog
attempt, before generic evaluation but after normal statement reset/promotion.
The regression now asserts `UnknownColumn("missing")` and published
`ROW_COUNT() = -1`. The final gate passed strict all-target Clippy, the full
workspace tests, 39 governance tests, every source/test/parser/plan inventory,
parser dependency isolation, formatting, and diff validation. Campaign 06's
gate then rejected an eight-argument query API under strict Clippy; the root
fix added `QueryResultContext` instead of a lint allow. After workspace
formatting normalization, the official reused-target 12-job gate passed strict
all-target Clippy, the full workspace tests, 39 governance tests, every
checked inventory, parser isolation, formatting, and diff validation, issuing
`integration_receipt 3`. The queue has zero active claims; generated totals
at the Campaign 06 boundary were 1,914/440/36/0 production and
15,300/960/311/14 test/support obligations.

Campaign `2026-07-read-path-07` is integrated and its three receipt-backed
claims are released. It owns a 14-source/65-obligation direct-Go read-path
batch: one validated TiKV TableScan/IndexScan to exact tipb DAG lowering;
ordered table/index reader dispatch with one-way response ownership, caller
`RequiredRows`, dummy temporary-table zero-send, and exact-once cleanup; and an
RPC-ready TiKV unary envelope with exact context/source/replica/timeout/scope
metadata plus decoded response precedence. Cross-review found and closed three
real composition defects before the shared gate: the planner could cost index
A while serializing index B, the reader truncated after fetching instead of
propagating `RequiredRows`, and the request marked TiDB traffic as unknown
origin.

**Historical Campaign 07 boundary:** at that checkpoint, do not call that
transport live. There was no socket/gRPC TiKV client, PD/RegionCache lookup,
lock resolver, backoff/retry scheduler, cancellation, TLS/rate limiting,
RealTiKV execution, general DAG tree, complete range lowering,
signed/unsigned range split, sorted response merge, virtual-column runtime,
or production BaseExecutor/chunk integration. All three Campaign 07 slices
remain `PARTIAL`; Campaign 09 later closed only the socket and bounded live
unary-execution parts of this historical list. The official 12-job gate passed
formatting, strict all-target Clippy, the full Rust workspace tests, 39
governance tests, all checked source/test/parser/plan inventories, dependency
isolation, and diff validation, then issued and fully consumed
`integration_receipt 3`. Exact membership is
archived, active claims are zero, and current generated ledgers are
1,907/447/36/0 production plus 15,284/976/311/14 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). This is measurable ownership movement,
not 30% product parity; the runtime is still far below that threshold.

## 6. Per-increment checklist (what "done" means)

1. Pick a queue gap or explicit Go test obligation; never start from a fixture-derived rule.
2. **Read the owning Go code**, including grammar/AST restore or builtin evaluation, and port its normal structure faithfully.
3. Add or extend a narrow exact selector plus a regression test before taking the global snapshot.
4. Confirm the family against `godump`/`gorun` where that ring applies, and record the controlled differential delta across every outcome category.
5. Run the relevant focused Rust tests and Clippy. During this unfinished rewrite, report this as `WIP`; do not call the workspace Ready until the repository Ready profile is actually run.
6. Update this handoff only for durable queue/workflow facts. Do not claim parser, test, or execution parity from fixture coverage or one differential slice.

Note: fmt sometimes reformats long lines — run it, then re-check. Clippy nits seen repeatedly: `len() >= 1` → `!is_empty()`, `is_multiple_of`, `c.is_digit(base)` over `to_digit().is_some()`, redundant masks, large enum-variant size (box the big variant).

## 7. Longer-term / bigger features (not yet done)

- Multi-table DELETE/UPDATE: **done** (#147–#150, execution too).
- `STR_TO_DATE` (inverse of DATE_FORMAT, parse-by-format), TIME_FORMAT, WEEK/YEARWEEK.
- The `Datum::UInt(u64)` foundation, stateful `LAST_INSERT_ID`, `sql_select_limit`, initial unsigned DML storage, and bounded `AUTO_INCREMENT` allocation are ported: `LAST_INSERT_ID(expr)` records raw UInt64 bits (including `-1 -> u64::MAX`) for promotion at the next statement boundary, while the reader and `@@last_insert_id`/`@@identity` see only the promoted value. `sql_select_limit` keeps an unsigned session cap (default `u64::MAX`), survives rollback, and adds an outer implicit limit only when a SELECT/set operation lacks an explicit LIMIT. DML now coerces `INT`/`BIGINT` columns to real signed/UInt storage with source bounds across INSERT, defaults, and UPDATE. `AUTO_INCREMENT` has immutable one-column `INT`/`INTEGER`/`BIGINT` schema metadata, a separate nontransactional allocator, pre-conflict consumption, explicit-ID/UPDATE rebasing, first-success/failing-row `LAST_INSERT_ID` status, and CREATE/TRUNCATE/RENAME/DROP lifecycle coverage. CREATE's table-option seed follows Go's legacy signed `AutoIncID` carrier (zero or a raw option above `i64::MAX` starts at 1); it is intentionally bounded to default `auto_increment_increment`/`auto_increment_offset` (1/1). Other integer widths plus non-strict `sql_mode` saturation/warnings remain unported.
- Scalar user variables are source-backed through the shared `Rc<RefCell<BTreeMap<lowercase, Datum>>>` session seam: `SET @x` writes ordered session state; inline `@x := rhs` returns its RHS and makes non-NULL writes visible to later select-list items and later scan rows. Do not merge their NULL cases: Go scalar `SETVAR` returns NULL without overwriting the old variable, while top-level `SET @x = NULL` clears it. Timestamp user variables remain out of scope until the datum domain carries temporal type metadata.
- Decimal `/` has two source-visible layers: Go `MyDecimal` retains whole base-1e9 fractional words (`digitsFrac`) for later arithmetic while its SQL result renders only `resultFrac`. `tidb-datatype::Decimal` therefore keeps a separate internal `storage_scale` and display `scale`; do not collapse them or `AVG(a/b)` loses hidden precision (`pkg/types/mydecimal.go:2178-81,2253-84`; `pkg/executor/test/executor/executor_test.go:2488`).
- DB-level DDL (DROP/CREATE DATABASE actually affecting a namespace), views, users/roles — the seed executor models none.
- The design's plan and transaction differential rings.

## 8. Durable context

This tracked handoff, generated `STATUS.md`, checked manifests, and living
ExecPlans are the complete shared context. Per-slice worktrees must not depend
on private agent memory or machine-local paths.

## 9. Git / environment

- The tracked Rust baseline starts on `hparser-integration` (base `master`).
  Feature slices use `codex/<slice>` branches in separate worktrees; do not
  push unless asked.
- `godump`/`gorun` binaries at repo root are untracked build artifacts.
- Don't run `realtikvtest` or heavy Go builds for this work; it's all in the Rust workspace + the two Go differential binaries.
