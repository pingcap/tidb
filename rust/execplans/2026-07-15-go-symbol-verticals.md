# Go-symbol verticals for parallel SQL translation

## Purpose

Remove the remaining parallel-rewrite bottleneck in the table-DDL path.  The
workspace already has the right package boundaries; the problem is that three
large shared Rust files still make unrelated direct ports contend:

| Shared root | Current size | Why it blocks independent ports |
| --- | ---: | --- |
| `crates/tidb-ast/src/ddl.rs` | 1,196 lines | ALTER/table routing and remaining DDL payloads still share one owner |
| `crates/tidb-parser/src/ddl.rs` | 1,370 lines | ALTER grammar and the remaining column subfamilies still share one owner |
| `crates/tidb-exec/src/ddl.rs` | 389 lines | dispatch still shares commit ordering with unrelated DDL actions |

The source of truth remains Go.  The translation unit changes from an entire
Go file to a **non-overlapping named Go-symbol family**, backed by the exact
Go source and original tests.  This is semantic transit: port Go's parsing,
AST, restore, validation order, and execution boundary directly.  It is not a
token-for-token transpilation, which would import Go's representation and
runtime mechanics without preserving Rust ownership or making independently
verifiable seams.

## Invariants

1. A symbol family has one durable owner, one Rust leaf in every layer it
   needs, direct Go-source evidence, and one source-derived selector.
2. A Go source file may be split only into explicitly named, non-overlapping
   symbol families.  A full-file domain conflicts with every family inside it.
3. `ddl.rs` is a module declaration/re-export and table-DDL router only after
   the move.  It contains no domain grammar, AST restore, or capability rule.
4. The Rust AST preserves Go's model; a feature that the seed executor cannot
   model returns `Unsupported` before implicit commit or catalog mutation.
5. No forwarding compatibility aliases remain after a move.  Internal code
   imports the physical owner.
6. Mechanical extraction preserves all existing parser snapshots.  Behavioral
   ports update a selector and the global static oracle only after every
   changed outcome is reviewed.

## Target layout

```text
crates/tidb-ast/src/
  ddl.rs                         # DDL envelope + re-exports only
  ddl/
    create.rs                    # CreateTableStmt, CTAS, creation-side split
    column.rs                    # shared ColumnDef/default restore
    table_elements.rs            # create-only element envelope
    field_type.rs                # ColumnType, aliases and modifier canonicalization
    table_option.rs              # TableOption including affinity and auto-random

crates/tidb-parser/src/
  ddl.rs                         # shared cursor helpers and DDL router only
  ddl/
    create.rs                    # CREATE header + ordered tail composition only
    create/elements.rs           # list framing; delegates constraints to index
    column.rs                    # shared parseColumnDef (CREATE and ALTER)
    column/options.rs            # option dispatcher and remaining options
    column/generated.rs          # generated expression body and validation
    column/inline_key.rs         # inline PRIMARY/UNIQUE locality
    column/time.rs               # DEFAULT/ON UPDATE time grammar
    check.rs                     # shared CREATE/ALTER CHECK body
    field_type.rs                # parseFieldType-derived grammar and normalization
    table_option.rs              # parseCreateTableOptions / parseTableOption
    create_split.rs              # CREATE-side split tail only
    index.rs                     # existing index/FK owner
    alter/
      index_visibility.rs        # `parseAlterAlter` VISIBLE/INVISIBLE branch
      repartition.rs             # terminal `parseAlterPartition` PARTITION BY

crates/tidb-exec/src/ddl/
  create_table.rs                # catalog build/publication
  table_capability.rs            # explicit pre-mutation unsupported classification
  index.rs                       # existing index/FK owner
  table.rs                       # existing ALTER-table execution

crates/tidb-{ast,parser,exec}/src/tests/ddl/
  table.rs column.rs field_type.rs table_option.rs create_split.rs
```

`ddl_partition.rs` stays separate because it is already a real source-owned
vertical.  Index/FK stay in their existing `ddl_index` vertical.  This avoids
turning the target layout into an artificial generic DDL framework.

## Ownership records

Replace the whole-file-only domain record schema with explicit source-owner
selectors, for example:

```toml
go_owners = [
  "method:pkg/parser/ddl_table_parser.go#HandParser.parseColumnDef",
  "method:pkg/parser/ddl_table_parser.go#HandParser.parseColumnOptions",
  "method:pkg/parser/ddl_table_parser.go#HandParser.parseGeneratedColumnBody",
]
```

The schema admits only `file:`, `func:`, and `method:` selectors. A small Go
lexical scanner resolves declarations while excluding comments and literals;
the checker rejects an unknown selector, duplicate selector, file/symbol
overlap, and a partial symbol set for one Go file. Evidence names the exact
selector, not merely the file. This makes a rename fail loudly while allowing
harmless line movement, and keeps the source-domain queue truthful while
allowing independent families in a large upstream parser source file.

## Migration sequence

### 1. Make ownership granular before splitting code

- Atomically migrate `domain_queue` to schema v2 `go_owners`, including a Go
  lexical scanner and negative tests for comments, literals, duplicate
  selectors, missing symbols, partial file coverage, and file/symbol overlap.
- Replace the broad `ddl_table` record with these non-overlapping records:
  - `ddl_table_create`: `parseCreateTableStmt`;
  - `ddl_table_elements`: `parseTableElementList`;
  - `ddl_column_definition`: `parseColumnDef`;
  - `ddl_column_options`: `parseColumnOptions`;
  - `ddl_column_generated`: `parseGeneratedColumnBody`;
  - `ddl_column_inline_key`: `parseGlobalLocalOption`;
  - `ddl_column_time`: `normalizeDDLFuncName` and
    `parseNowSymOptionFraction`;
  - `ddl_field_type`: `parseFieldType` source family and its Go aliases;
  - `ddl_table_option`: `parseCreateTableOptions`, `parseTableOption`, and
    table-option helpers;
  - `ddl_create_split`: the CREATE-only split suffix.
- Keep `ddl_partition` and `ddl_index` records unchanged.

### 2. Do one behavior-preserving physical extraction

- Add the new leaf modules and move complete definitions/functions with their
  focused tests in one symbol-family at a time.
- Leave `ddl.rs` as temporary declarations/re-exports until all five leaves
  compile, then delete the moved bodies and private root helpers.
- Do not mix new syntax behavior into this move.  Each extraction must retain
  the global parser-oracle counts byte-for-byte.

### 3. Run independent direct-port lanes

After the structural move, agents may work concurrently on separate leaves:

| Lane | Immediate source-backed work | Independent proof |
| --- | --- | --- |
| field type | `SIGNED`/`UNSIGNED`/`ZEROFILL` final-state model and aliases | `TestSimple`, `TestCompatTypes`, field-type selector |
| table option | `AFFINITY`, then `AUTO_RANDOM` | `TestTableAffinityOption`, table-option selector |
| create split | typed `CreateTableSplit`, never reuse ALTER target | `TestSplitPartition`, split selector |
| column | remaining inline key/global-local constraints | direct parser test plus column selector |

Each lane changes only its leaf, its source test, selector, evidence fragment,
and its checked domain record.  A routing or catalog-interface requirement is
a narrow request to the appropriate steward; it is not permission to edit a
neighboring lane.

## Validation

Run from `rust/` with 12 build jobs:

```sh
CARGO_BUILD_JOBS=12 cargo test --locked -j12 -p difftest --bin domain_queue
CARGO_BUILD_JOBS=12 cargo run --locked -j12 -p difftest --bin domain_queue -- --check
CARGO_BUILD_JOBS=12 cargo test --locked -j12 -p tidb-ast -p tidb-parser -p tidb-exec --no-fail-fast
CARGO_BUILD_JOBS=12 cargo test --locked -j12 -p difftest-parser-tests --test selector_create
CARGO_BUILD_JOBS=12 cargo test --locked -j12 -p difftest-parser-tests --test integration_parser_diff -- --nocapture
CARGO_BUILD_JOBS=12 cargo clippy --locked -j12 -p tidb-ast -p tidb-parser -p tidb-exec --all-targets -- -D warnings
cargo fmt --all -- --check
```

For a purely mechanical move, the reviewed parser-oracle snapshot must stay
at 51,354 exact accepted restores, 197 total Rust parse failures, 9 restore
mismatches, and 37 Rust false accepts. Any behavioral lane owns and explains
its delta before it updates `HANDOFF.md` and this plan.

## Progress

- [x] Identified the real contention seam and current measured baseline.
- [x] Added schema-v2 `file:`/`func:`/`method:` source owners, a Go lexical
  declaration scanner, exact selector evidence, and full-file/symbol-family
  conflict validation. Parser-manifest source fragments remain separate so
  their one-source global ledger is not repurposed as a local ownership file.
- [x] Split the first shared table-DDL leaf: `ColumnType` now lives in
  `tidb-ast/src/ddl/field_type.rs`; field-type parsing and its focused source
  test live in matching parser leaves. The generated parser manifest now
  points to that physical parser owner.
- [x] Split the shared table-option AST/parser vocabulary into matching
  `ddl/table_option.rs` leaves, then replace its broad file claim with the
  complete, non-overlapping `ddl_table_option` Go method family. The Go
  package variable `rowFormatNames` is documented beside its consuming
  `parseTableOptionRowFormat` selector rather than left ownerless.
- [x] Extracted static CREATE/ALTER capability classification into
  `tidb-exec/src/ddl/table_capability.rs`; commit, catalog lookup, and
  publication stay in the coordinator so this structural move cannot change
  TiDB-visible DDL error ordering.
- [x] Ported the complete `parseTableOption` method family into its own
  AST/parser leaf and added typed `CREATE TABLE ... AFFINITY` parsing,
  restoration, source selectors, and a pre-commit executor rejection. The
  creation-partition selector moved eight reviewed rows from parse failure to
  exact restore without changing its existing mismatch.
- [x] Added the creation-only `SPLIT` model and direct Go parse/restore order;
  it does not reuse ALTER's `SplitTarget`, restores bare `SPLIT` before CTAS
  and `ON COMMIT`, accepts Go's optional `REGION`, and rejects seed-executor
  execution before the implicit DDL commit.
- [x] Extracted the CREATE statement envelope, CTAS source, and AST restore
  into `ddl/create.rs`; extracted comma-framing table elements into
  `ddl/create/elements.rs`; and retained shared column parsing rather than
  copying it into CREATE.
- [x] Extracted shared ColumnDef/ColumnOption AST, shared parseColumnDef,
  cross-route CHECK parsing, inline-key parsing, and the column-options
  dispatcher into physical leaves. Direct Go ports now cover bare inline KEY,
  GLOBAL locality, SERIAL, AUTO_RANDOM, column format/storage, secondary
  engine attributes, and MariaDB row markers.
- [x] Split generated/default/time column subfamilies into physical leaves:
  `column/generated.rs` owns generated-body and MariaDB row markers, while
  `column/time.rs` owns DEFAULT alias normalization and ON UPDATE time
  functions. These behavior-preserving moves retained the reviewed parser
  oracle at 50,843 exact accepted restores and 700 Rust parse failures.
- [x] Replace broad table-domain ownership with seven non-overlapping exact
  source-symbol records. Together they account for all eight declarations in
  `ddl_table_parser.go` (the time family owns two declarations).
- [x] Port Go `parseStringOptions` binary-string normalization: the typed AST
  retains the binary flag, normalizes `CHARACTER SET binary` to binary storage
  families, and shares the behavior across CREATE and ALTER. The reviewed
  global parser oracle is now 50,912 exact accepted restores, 640 parse
  failures, and 8 restore mismatches.
- [x] Dispatched and integrated the first four independent direct-port lanes:
  field type, table option, creation split, and column-option/key behavior.
- [x] Extracted the first two ALTER symbol families into physical leaves:
  `ddl/alter/index_visibility.rs` owns the Go `parseAlterAlter` index
  visibility branch and its typed AST payload, while
  `ddl/alter/repartition.rs` owns `parseAlterPartition`'s terminal
  `PARTITION BY` branch and delegates only the already-shared typed partition
  grammar. Both leaves have direct Go TestDDL rows, checked static selectors,
  and exact domain records; no generic ALTER wrapper was added.
- [x] Continued the source-domain pattern with the narrow ALTER CHECK
  enforcement leaf and ordinary SHOW TABLE STATUS leaf. Their direct Go
  source rows and static selectors add 16 and 3 exact restores respectively;
  the AST/parser/executor boundaries preserve canonical restore and reject
  unsupported execution before mutation.
- [x] Completed the last direct `parseAlterAlter` leaf, ALTER COLUMN
  SET/DROP DEFAULT, and the separate SHOW STATUS source family. Their
  selectors execute 25 and 3 exact oracle rows respectively, including Go's
  no-space `DEFAULT(` spelling; unsupported execution stays pre-mutation.
- [x] Added two independent direct source leaves without reopening broad
  dispatch: `parseAlterRename` owns typed `RENAME {KEY|INDEX} old TO new`
  canonicalization (21 exact static rows), while `parseAdminShow` owns typed
  `ADMIN SHOW DDL JOBS [number] [WHERE expression]` state (3 exact rows and
  Go's negative-number rejection). Both have pre-mutation executor
  boundaries; bare ADMIN SHOW DDL, JOB QUERIES, RENAME TABLE, and RENAME
  COLUMN remain explicitly separate leaves.
- [x] Ported the next exact branches without widening either family: Go
  `parseAlterDrop` now has a typed `DROP {CHECK|CONSTRAINT}` leaf (21 static
  single-action rows), and `parseAdminShow` has typed ID-list versus LIMIT
  `JOB QUERIES` alternatives (all six direct TestAdminStmt rows plus one
  static row). The Go-accepted `LOCK = DEFAULT, DROP CHECK ...` composition
  remains explicitly blocked on an independent LOCK leaf.
- [x] Closed two source-only leaves without creating a generic ALTER or ADMIN
  adapter: `parseAlterTableOptions` owns typed `LOCK [=]
  {DEFAULT|NONE|SHARED|EXCLUSIVE}` and composes with the prior DROP CHECK leaf
  for the exact TestDDL regression, while bare `ADMIN SHOW DDL` is a unit
  `parseAdminShow` leaf ordered after its longer JOBS and JOB QUERIES prefixes.
  All direct source rows and pre-mutation executor boundaries pass. Their
  static selectors intentionally assert zero records because this checked
  fixture corpus contains neither standalone form; the reviewed global parser
  snapshot therefore remains unchanged.
- [x] Proved the next parallel leaf batch against the static oracle: typed
  `DROP FOREIGN KEY` moved 16 rows, SHOW VARIABLES WHERE moved 9, SHOW
  STATS_TOPN moved 4, and ADMIN CANCEL/PAUSE/RESUME DDL job control moved 3,
  all from parse failure to exact restore with no other outcome movement.
  Job control is the first physical nested `admin/` leaf; its parser preserves
  Go's deliberately discarded noun token instead of adding a broad alias.
- [x] Ported the next three generic ALTER TABLE option leaves in parallel:
  `AUTO_INCREMENT` moved four selected rows, table-level `COMMENT` moved nine,
  and `SHARD_ROW_ID_BITS` moved five. Each reuses Go's existing
  `AlterTableOption.Options` shape through `SetTableOptions` and has a focused
  source test, static selector, and pre-mutation executor boundary. The
  aggregate oracle now reports 51,128 exact restores and 424 Rust parse
  failures; two additional composite option records close through the same
  shared envelope.
- [x] Ported the next independent queue families in parallel: table-level
  placement policy adds ten exact static records, SHOW STATS_LOCKED adds three,
  and the no-ON GRANT/REVOKE ROLE branches add 25 exact accepted records
  globally (the focused R1 selectors retain five GRANT and three REVOKE
  anchors). Role membership has its own typed AST leaves and executor boundary;
  ordinary privileges, PROXY, and special REVOKE ALL remain separate.
- [x] Ported the next source-owned parser wave in parallel: partition and
  table TTL/REMOVE TTL, DROP PRIMARY KEY, GRANT `REQUIRE` TLS,
  AUTO_ID_CACHE/AUTO_RANDOM_BASE (including FORCE), dynamic
  BACKUP_ADMIN/SYSTEM_VARIABLES_ADMIN REVOKE, and explicit CREATE
  DEFINER/SQL SECURITY view forms. These leaves add typed
  physical AST/parser ownership, source tests, selectors, evidence fragments,
  and unsupported-before-mutation executor boundaries. The measured oracle is
  now 51,294 exact accepted restores with 258 total Rust parse failures,
  unchanged eight restore mismatches, and 37 false accepts; 242 actionable
  nonmatches remained before the next column-default lane.
- [x] Ported grouped `ALTER TABLE ADD COLUMN` literal-default rows through
  the source-owned column/options path. Numeric and DATETIME string defaults
  retain Go's typed string expression and `_UTF8MB4` restore prefix; the
  selector covers 13 accepted integration rows and the executor remains
  unsupported before mutation. The measured oracle is now 51,307 exact
  restores with 245 total Rust parse failures and 229 actionable nonmatches.
- [x] Ported the independent `ALTER TABLE RENAME COLUMN old TO new` branch,
  preserving its qualified-name rejection boundary and canonical backquoted
  restore. The direct source tests and selector cover 16 accepted integration
  rows, with an explicit pre-mutation executor rejection. The oracle now has
  51,323 exact restores, 229 total Rust parse failures, and 213 actionable
  nonmatches.
- [x] Ported ENUM/SET binary members, bare `LOCALTIME`/`LOCALTIMESTAMP` in
  CHECK expressions, and `SET TRANSACTION ... AS OF` snapshot syntax through
  source-owned parser leaves, direct source tests, selectors, coverage
  evidence, and unsupported-before-mutation executor boundaries. The latest
  oracle is 51,345 exact restores, 207 total Rust parse failures, and 191
  actionable nonmatches.
- [x] Ported joined/derived UPDATE bare `DEFAULT` assignments through the
  shared Go `parseExprOrDefault` branch, added per-target executor default
  resolution plus derived-target pre-mutation rejection, and covered the two
  accepted integration rows with source tests, selector evidence, and focused
  executor regressions. Removed the stale lexer token debug/cap that aborted
  long-corpus queue replay. The latest oracle is 51,347 exact restores, 205
  total Rust parse failures, and 189 actionable nonmatches.
- [x] Ported SHOW CHARACTER SET/CHARSET and dynamic
  `RESOURCE_GROUP_ADMIN`/`RESOURCE_GROUP_USER` REVOKE names through their
  physical AST/parser leaves, direct source tests, selectors, evidence, and
  unsupported-before-mutation boundaries. The latest oracle is 51,354 exact
  restores, 197 total Rust parse failures, and 182 actionable nonmatches.
- [x] Ported all 21 `pkg/parser/lexer_test.go` anchors into the Rust lexer,
  keeping raw-token boundaries in the neutral scanner and marking only the
  parser-owned escape/value decoding as partial. Corrected executable-feature
  comment gating, version/feature helper rewind semantics, invalid-byte
  handling, malformed variables, and removed the stale long-token panic. The
  source evidence records 18 covered and 3 partial anchors.
- [x] Ported the actual `rust/difftests/gorun/main_test.go` byte-safe result-cell
  vectors into `tidb-exec::result`, including marker escaping and ordered vs
  unordered row labels. This is support-source evidence, intentionally kept
  outside the original `pkg/` test ledger.
- [x] Added the first real planner owner: `tidb-planner` contains the pure
  exponential-backoff API and `difftest-planner-tests` consumes it directly.
  The external test preserves all 12 original vectors plus a focused NaN/
  infinity contract check for Go `math.Min`/`math.Max` behavior.
- [x] Added the bounded `pkg/kv/utils.go` keyspace predicates to `tidb-txnkv`
  and translated both original keyspace tests in `difftest-transaction-tests`.
  The rest of that mixed Go source remains explicitly partial.
- [x] Reserved `pkg/expression/builtin_control.go` as a checked expression
  domain and closed the scalar IFNULL source rows through the existing
  `tidb-expr` dispatch. Typed temporal/JSON/SET/error rows remain partial until
  the evaluator has their real FieldType/session and error contracts.
- [x] Ported the one accepted `SHOW ENGINES` oracle row through a dedicated
  typed SHOW leaf, source restore tests, a static selector, and an explicit
  executor unsupported-before-mutation regression. Shared LIKE/WHERE filters
  are retained without claiming an engine registry.
- [x] Ported seven qualified CREATE TABLE column-name rows plus the adjacent
  ALTER TABLE ORDER BY/qualified-MODIFY and EXPLAIN/LEADING queue slices
  through source-owned AST/parser leaves, direct source tests, exact selectors,
  and pre-mutation executor boundaries. The aggregate oracle is now 51,369
  exact restores, 182 total Rust parse failures, and 167 actionable
  nonmatches.
- [x] Ported seven MariaDB/table-option compatibility rows, two CHECK/IMPORT
  PARTITION actions, and three SET restore-mismatch rows through the existing
  source-owned option, partition, and SET leaves. The aggregate oracle is now
  51,382 exact restores, 172 total Rust parse failures, six restore mismatches,
  and 154 actionable nonmatches.
- [x] Closed the bare `ADD PARTITION` action and the `CREATE TABLE ... ENGINE =
  MERGE UNION = (...)` option through typed AST/parser leaves, direct
  source-backed tests, exact selectors, evidence fragments, and
  unsupported-before-mutation executor boundaries. The aggregate oracle is now
  51,384 exact restores, 170 total Rust parse failures, six restore mismatches,
  and 152 actionable nonmatches.
- [x] Closed `DISCARD PARTITION ... TABLESPACE` and the full 25-row accepted
  `FIRST/LAST PARTITION LESS THAN` integration family through typed partition
  actions, source-backed tests, exact selectors, evidence fragments, and
  unsupported-before-mutation executor boundaries. The aggregate oracle is now
  51,409 exact restores, 145 total Rust parse failures, six restore mismatches,
  and 127 actionable nonmatches.
- [x] Closed `MERGE FIRST PARTITION LESS THAN`, `SPLIT MAXVALUE PARTITION LESS
  THAN`, and the three-record/four-input parenthesized set-operation family.
  The shared ALTER router guards `SPLIT MAXVALUE` so existing `SPLIT
  PRIMARY|INDEX` parsing cannot be stolen by the partition leaf. The aggregate
  oracle is now 51,417 exact restores, 137 total Rust parse failures, six
  restore mismatches, and 119 actionable nonmatches.
- [x] Closed standalone ALTER TABLE `WITH/WITHOUT VALIDATION`, root
  `ENGINE_ATTRIBUTE` (including its 18-row storage-class family), and SHOW
  MASTER STATUS/PRIVILEGES through typed AST/parser leaves, source-backed
  tests, exact selectors, evidence fragments, and unsupported-before-mutation
  executor boundaries. The aggregate oracle is now 51,439 exact restores, 115
  total Rust parse failures (53 Go-accepted), six restore mismatches, and 97
  actionable nonmatches.
- [x] Closed `ADMIN CLEANUP TABLE LOCK`, `SHOW BUILTINS`, `SHOW FULL TABLES`
  with `FROM/IN` plus filters, and reserved-word `DROP DATABASE` names through
  source-owned AST/parser leaves, exact selectors/evidence, and
  unsupported-before-mutation executor boundaries. The aggregate oracle is now
  51,443 exact restores, 111 total Rust parse failures (49 Go-accepted), six
  restore mismatches, and 93 actionable nonmatches.
- [x] Closed grouped `ALTER TABLE ADD COLUMN` definitions (including ordered
  constraints), reserved-name `USE`, and `ADMIN ALTER DDL JOBS` options through
  source-owned AST/parser leaves, exact selectors/evidence, and the required
  executor boundary. The aggregate oracle is now 51,451 exact restores, 103
  total Rust parse failures (41 Go-accepted), six restore mismatches, and 85
  actionable nonmatches.
- [x] Closed the multi-spec ADD/DROP IF EXISTS metadata family, parenthesized
  `EXPLAIN FORMAT=TRADITIONAL` VALUES, and REVOKE dynamic/ALL-GRANT-OPTION
  edges through source-owned parser/AST leaves, exact selectors/evidence, and
  executor boundaries. The aggregate oracle is now 51,458 exact restores, 96
  total Rust parse failures (34 Go-accepted), six restore mismatches, and 78
  actionable nonmatches.
- [x] Closed standalone ALTER TABLE `UNION`/`INSERT_METHOD`/
  `PRE_SPLIT_REGIONS`, `CREATE GLOBAL BINDING FOR ... WITH ... DML`, binary
  charset literals inside `EXPLAIN`, and recursive `WITH ... LATERAL` CTE
  names through source-owned parser/AST leaves, exact selectors/evidence, and
  executor boundaries. The aggregate oracle is now 51,466 exact restores, 88
  total Rust parse failures (26 Go-accepted), six restore mismatches, and 70
  actionable nonmatches.
- [x] Closed ALTER TABLE ANALYZE PARTITION, INSERT ... WITH ... TABLE,
  EXISTS set-operation subqueries, and comma-separated ENGINE/ROW_FORMAT
  options through source-owned parser/AST leaves, exact selectors/evidence,
  and executor boundaries. The aggregate oracle is now 51,471 exact restores,
  83 total Rust parse failures (21 Go-accepted), six restore mismatches, and
  65 actionable nonmatches.
- [x] Widened scalar subquery ownership to the typed `QueryStmt` envelope so
  top-level `UNION` bodies are preserved in parser and executor paths. Added
  the exact CTE source selector/evidence and execution boundary. The aggregate
  oracle is now 51,473 exact restores, 81 total Rust parse failures
  (19 Go-accepted), six restore mismatches, and 63 actionable nonmatches.
- [x] Added a byte-preserving AST restore sink for binary ENUM/SET members.
  `ColumnTypeArg::Bytes(Vec<u8>)` carries invalid GBK octets without an invalid
  Rust `String`, `Stmt::restore_bytes()` is used by the parser ring, and the
  queue now closes seven accepted binary-member rows. The reviewed aggregate
  is 51,480 exact restores, 74 total Rust parse failures (12 Go-accepted),
  six restore mismatches, and 56 actionable nonmatches.
- [x] Added the source-owned transaction counter leaf for `IncInt64` and
  `GetInt64`, with a narrow `CounterStorage` contract and exact missing-key,
  parse-error, and overflow semantics. Added the complete `TestSign` source
  table against the production expression path. Both verticals have direct
  Rust tests and independent evidence fragments; no fake storage client or
  evaluator/session compatibility layer was added.
- [x] Ported the decimal literal rows from `pkg/parser/ast/format_test.go`:
  the AST now removes leading integer zeros while preserving fractional scale
  and the leading-dot form. The reviewed parser oracle is 51,481 exact
  restores, 74 total Rust parse failures (12 Go-accepted), five restore
  mismatches, 37 false accepts, and 55 actionable nonmatches at that point in
  the sequence.
- [x] Closed the next parser queue wave through source-owned leaves: NATIONAL
  CHAR/CHARACTER/VARCHAR/VARCHARACTER/NCHAR/NVARCHAR field types, quoted
  column `COLLATE`, EXPLAIN hint query-block name decoding, shared table
  charset validation, CHAR/CONVERT USING validation, legacy charset
  introducers, strict DOUBLE arity, CREATE TABLE builtin-name token
  boundaries, unsigned LIMIT overflow, datetime precision, full collation
  validation through the shared lexer registry, the source-shaped multi-
  statement parse envelope, and Go's binary INSERT escape decoding. The
  current reviewed
  oracle is 51,488 exact single-statement restores plus 10 complete
  multi-statement restores, 99 Rust parse failures (all dual rejections), zero
  restore mismatches, zero false accepts, and one actionable nonmatch; sixteen
  invalid table-charsets and the remaining invalid collation rows now reject at
  the Go-compatible parser boundary. The no-ID executable T! comment and
  parenthesized WITH restore rings are source-owned with direct tests,
  selectors, and evidence. The pinned `json_memberof()` Go restore-failure
  row now has an explicit parse-only selector and source test rather than
  being mistaken for an unowned parity gap.
- [x] Added two non-parser source-owned leaves in parallel: planner row-size
  formulas now live in `tidb-planner::cardinality::row_size` with a separate
  `difftest-planner-tests` target covering all 28 `TestAvgColLen` value
  assertions, and the expression ring adds representable `TestCoalesce` rows
  beside the existing `TestIfNull` source table. Both keep missing real
  planner/context adapters explicit as `PARTIAL` evidence rather than adding
  compatibility shims.
- [x] Added the bounded datatype overflow vertical from `pkg/types/overflow.go`.
  Checked signed, unsigned, mixed-integer, duration, and division arithmetic
  now preserve Go's MinInt64 boundaries, with all four original overflow test
  tables in a dedicated Rust module and the broader `dbterror` hierarchy kept
  as an explicit partial seam.
- [x] Added the next source-owned leaves without reopening shared routing:
  byte-first ASCII encoding from `encoding_ascii.go` (including Go's
  lead-byte grouping and dual transform result), a source audit/regression
  ring for LIKE escape and coercion behavior, and explicit transaction
  iteration traits for `NextUntil`/`WalkMemBuffer` with close-on-error tests.
  The ASCII registry/vectorized LIKE state and the real KV storage protocol
  remain honest `PARTIAL` boundaries; the transaction integration target is
  registered separately so it builds in parallel with parser/result rings.
- [x] Added the bounded ILIKE scalar leaf without reopening expression routing:
  `like::ilike_match` ports Go's ASCII-only lowercasing and
  `LowerOneStringExcludeEscapeChar` escape state before reusing the existing
  wildcard matcher. The complete `TestIlike` scalar table and both source
  vectorized test anchors now have independent evidence; Go's function-class,
  session-collation, chunk/vectorized, cache, and warning/error seams remain
  explicit `PARTIAL` boundaries.
- [x] Closed the collation-aware LIKE scalar ring from
  `pkg/expression/builtin_like_test.go::TestCILike`: `like_match_with_collation`
  compares literal runes through the registered general-CI and Unicode-CI
  weight tables while preserving `%`/`_` wildcard cardinality. All 25 source
  rows, including accent/eszett folding and the supplementary-rune identity
  boundary, are direct Rust evidence; the source's 0900-ai-ci column and
  session/function-class/vectorized lifecycle remain explicit `PARTIAL`
  boundaries.
- [x] Added byte-first UTF-8 and strict utf8mb3 encoding leaves from
  `encoding_utf8.go`, preserving Go's decoder-width grouping, malformed-byte
  advancement, three-byte validation, and dual transform result. The shared
  encoding base/registry and non-UTF8 families remain partial; the source
  `TestEncodingValidate` anchor stays uniquely owned by its existing evidence
  fragment while the UTF-8 subset is attached as a file-level obligation.
- [x] Added the portable `pkg/kv/key.go` helper family through the existing
  `tidb-txnkv::Key`/`KeyRange` owners: byte/order/prefix/clone/string semantics
  and safe point-boundary checks now have a separate transaction test target.
  The original unsafe kvproto-layout `TestKeyRangeDefinition` assertion stays
  explicitly blocked until `tidb-proto` exposes a typed conversion boundary.
- [x] Closed the ranking-window peer identity edge in `tidb-exec::window`.
  `RANK`, `DENSE_RANK`, and `PERCENT_RANK` now reuse the typed ORDER BY
  comparator for peer detection, preserving Go's INT/UINT equality, rank gaps,
  dense increments, no-order ties, and single-row zero rule. The direct
  `TestMemRank`, `TestMemPercentRank`, and `TestMemRowNumber` anchors remain
  partial only for Go allocator/memory lifecycle hooks.
- [x] Factored the shared `encoding_base.go` transform policy into
  `tidb-datatype::encoding_base`, then made the ASCII and UTF-8 leaves reuse
  its typed operation bits, generic bytes-plus-error result, and first-error /
  replacement / truncation state machine. Decoder-specific registry wiring,
  GBK, and GB18030 remain explicit partial seams.
- [x] Added the pure planner full-join cardinality leaf from
  `pkg/planner/cardinality/join.go`. The typed adapter preserves Cartesian and
  equi/NA-key paths, max-NDV division, threshold gating, Go's exact 0.9
  exponent including the negative fallback, and NaN/signed-zero max behavior;
  real planner context/statistics/operator adapters remain partial.
- [x] Added the bounded REGEXP_LIKE scalar leaf from
  `pkg/expression/builtin_regexp.go`. A single RegexBuilder preserves Go's
  empty/malformed-pattern errors, i/c rightmost precedence, and m/s flags;
  session collations, cache lifecycle, vectorized execution, warning channels,
  and REGEXP_SUBSTR/INSTR/REPLACE remain explicit partial seams.
- [x] Audited the original two-argument `pkg/expression/builtin_like_test.go`
  `TestRegexp` separately from `TestRegexpLike`: all nine successful source
  rows and both `[NOT] REGEXP` negations now run through parser `Expr::Regexp`
  dispatch, while `regexp_match` asserts Go's four malformed-pattern rows,
  including the lone backslash. The source-owned test evidence intentionally
  reuses the existing `builtin_regexp.go` production fragment instead of
  creating a duplicate source claim.
- [x] Removed a real mixed signed/unsigned DIV regression in the compact
  expression evaluator. `pkg/expression/builtin_arithmetic.go`'s four integer
  signatures now map to the checked datatype helpers, while MOD preserves the
  dividend sign and left-operand unsigned result flag. Decimal/real and
  session warning/type paths remain partial.
- [x] Added the pure inner-transaction timestamp box from
  `pkg/kv/txn.go::innerTxnStartTsBox`. The typed mutex-protected set preserves
  store/delete and strict lower/upper minimum selection, including the
  `current_min <= lower_limit` boundary; oracle clock logging, global registry,
  RunInNewTxn, retry, and storage/session behavior remain partial.
- [x] Added the bounded `GROUPING` scalar leaf from
  `pkg/expression/builtin_grouping.go`. Typed metadata preserves all three
  source algorithms (bit-and, numeric-compare, numeric-set), mark-cardinality
  validation, uninitialized evaluation, and all 19 original `TestGrouping`
  rows; tipb/planner rewrite, function-class, session, unsigned FieldType, and
  vectorized paths remain explicit partial seams.
- [x] Fixed the byte-preservation boundary in `CAST(... AS BINARY(N))` from
  `pkg/expression/builtin_cast.go`. Multibyte source strings now truncate and
  pad raw bytes into `Datum::Bytes`, matching Go's `str[:N]` behavior instead
  of attempting invalid UTF-8 reconstruction; warning, FieldType, charset,
  temporal/JSON, and vectorized cast paths remain partial.
- [x] Ported the MariaDB-only `UUID` field-type branch from
  `pkg/parser/ddl_fieldtype_parser.go`: enabled mode normalizes `UUID` to
  `CHAR(36)`, disabled mode rejects it, and `UUID(...)` is rejected rather
  than treated as a length-bearing type. The complete `TestUUIDTypeMariaDB*`
  rows are now claimed in the exact shared test-domain manifest.
- [x] Fixed the foreign-key reference parser's bare `MATCH` boundary from
  `pkg/parser/ddl_index_parser.go`. Go consumes an unqualified MATCH token and
  continues parsing ON actions; Rust now does the same, with direct regressions
  for bare MATCH and MATCH followed by ON UPDATE.
- [x] Fixed `ABS(MININT)` in the math family. `checked_abs` now reports the
  source overflow error instead of wrapping, with a direct source-derived
  regression; warning/session/vectorized behavior remains partial.
- [x] Added the dependency-closed retry arithmetic leaf from `pkg/kv/txn.go`.
  `retry_backoff_upper_bound_ms` preserves `BackOff`'s capped
  `min(100, 1*2^attempts)` bound, including large-attempt saturation, while
  random jitter, sleeping, `MaxRetryCnt`, and `RunInNewTxn` stay explicitly
  outside `tidb-txnkv`.
- [x] Audited `INTERVAL` against `intervalFunctionClass` and both Go integer/
  real signatures. The scalar leaf now covers every `TestIntervalFunc` value
  row, exact signed/unsigned boundary ordering, nullable scans, and Go's
  finite saturation for overflowing real prefixes (`'1e999'`); FieldType,
  warning/session, nullable-column metadata, and vectorized paths remain
  explicit partial seams.
- [x] Preserved integer `TRUNCATE`'s unsigned-scale short-circuit from
  `builtinTruncateIntSig`/`builtinTruncateUintSig`. A scale whose unsigned
  bits would narrow to signed `-1` now leaves the input unchanged, matching
  the source FieldType check; the direct scalar regression and source/test
  evidence remain partial for function-class, warning, and vectorized seams.
- [x] Added the bounded partition-key algorithm parser leaf from
  `pkg/parser/parser_test.go::TestPartitionKeyAlgorithm`. The four original
  rows now execute in an isolated Rust test: algorithm `1` restores exactly,
  while `-1`, `0`, and `3` reject through the typed range validator; partition
  execution and the remaining partition parser source stay explicit partial
  boundaries.
- [x] Added the bounded interval-partition parser leaf from
  `pkg/parser/parser_test.go::TestIntervalPartition`. All 13 original rows
  now execute in an isolated Rust test: RANGE INTERVAL expression/COLUMNS
  forms, optional FIRST/LAST bounds, NULL/MAXVALUE markers, and the
  interval-specific ALTER split/merge/boundary rows preserve Go's exact
  restore or rejection behavior; broader partition AST/semantic/executor
  obligations remain explicit partial boundaries.
- [x] Closed the omitted mixed string/numeric LEAST/GREATEST row from
  `pkg/expression/builtin_compare_test.go::TestGreatestLeastFunc`. When Go's
  aggregate type selects the string signature, the scalar `compare2` leaf now
  renders numeric arguments to text before byte-order comparison, preserving
  `GREATEST('123a','b','c',12) = 'c'` and `LEAST(...) = '12'`; temporal,
  duration, collation/session, and vectorized construction remain partial.
- [x] Closed the quoted index-name boundary in
  `pkg/parser/join_parser.go::HandParser.parseIndexHint`. Go consumes a
  quoted token directly (`USE INDEX ('idx')`) and canonicalizes it as a
  backquoted index name; the Rust table-hint parser now decodes that string
  locally while retaining the narrower charset/name contracts elsewhere.
  Optimizer hint application, catalog validation, and broader `TestIndexHint`
  coverage remain explicit partial boundaries.
- [x] Completed the bounded `JSON_LENGTH` scalar table from
  `pkg/expression/builtin_json.go::builtinJSONLengthSig` and
  `pkg/expression/builtin_json_test.go::TestJSONLength`. The Rust leaf now has
  direct assertions for all 38 representable rows: scalar length-one rules,
  direct-child object/array counts, exact single-selection paths, NULL and
  missing-path propagation, and multiple-selection wildcard/range/recursive
  errors. Binary JSON value conversion, FieldType/function-class construction,
  warning/session state, and vectorized execution remain explicit partial
  seams.
- [x] Added the omitted pure scalar `TestCompare` row for a Go `float64`
  compared with `MyDecimal` (`1.1 < 123.123`). The Rust direct test keeps
  `eval_binary`'s ETReal promotion boundary and integer boolean result
  visible; temporal/JSON signatures, FieldType construction, warning/session
  state, and vectorized execution remain explicit partial seams.
- [x] Fixed the omitted `FIELD` signature-selection boundary from
  `pkg/expression/builtin_string.go::fieldFunctionClass`. The Rust leaf now
  chooses one ETString/ETInt/ETReal mode for the full argument list, so a mixed
  string/integer call compares every candidate numerically (`FIELD('1','01',1)`
  returns position 1) instead of falling back to pairwise text comparison;
  FieldType/function-class, collation/session, warning, and vectorized paths
  remain explicit partial seams.
- [x] Closed the deterministic hash leaves from
  `pkg/expression/builtin_encryption.go` (`builtinMD5Sig`,
  `builtinSHA1Sig`, and `builtinSHA2Sig`). The Rust crypto owner now hashes
  the exact Go `EvalString` bytes (including raw binary/GBK payloads), keeps
  numeric ETString rendering, preserves `SHA`/`SHA1` aliasing, and retains
  SHA2's ETInt hash-length coercion. AES/COMPRESS/RANDOM_BYTES/PASSWORD/
  VALIDATE_PASSWORD_STRENGTH/ENCODE/DECODE/SM3 remain explicit session,
  nondeterminism, or dependency-boundary work.
- [x] Added the bounded `LENGTH`/`OCTET_LENGTH` scalar table from
  `pkg/expression/builtin_string_test.go::TestLengthAndOctetLength`. Both
  aliases now execute the complete representable scalar rows through the
  source byte-counting path, including UTF-8, numeric/decimal coercion,
  binary-literal, NULL, and an incomplete-UTF-8 binary-cast regression. Go's
  typed DATETIME/SET/DURATION rows, connection-charset loop, injected-error
  datum, and function-class/vectorized/session state remain explicit partial
  seams.
- [x] Added the bounded IPv6 scalar family from
  `pkg/expression/builtin_miscellaneous.go::builtinInet6AtonSig` and
  `builtinInet6NtoaSig`. `INET6_ATON` preserves four-byte plain IPv4 versus
  sixteen-byte colon-containing (including mapped IPv4) binary results, while
  `INET6_NTOA` preserves canonical four/sixteen-byte formatting and NULL for
  other lengths; all original `TestInet6AtoN`/`TestInet6NtoA` rows now run in
  direct Rust tests. Function-class metadata, error/warning/session charset,
  and vectorized execution remain explicit partial seams.
- [x] Added the raw-byte IPv4 predicate leaf from
  `pkg/expression/builtin_miscellaneous.go::builtinIsIPv4MappedSig` and
  `builtinIsIPv4CompatSig`. `IS_IPV4_MAPPED` now recognizes exactly the
  sixteen-byte `::ffff:` prefix and `IS_IPV4_COMPAT` exactly the sixteen-byte
  `::/96` prefix, preserving Go's direct ETString byte semantics even for
  invalid UTF-8. Every original `TestIsIPv4Mapped`/`TestIsIPv4Compat` row plus
  NULL and raw-byte boundaries executes in direct Rust tests; function-class,
  warning/session, and vectorized execution remain explicit partial seams.
- [x] Added the bounded `NAME_CONST` scalar leaf from
  `pkg/expression/builtin_miscellaneous.go::nameConstFunctionClass` and its
  typed `builtinNameConst*Sig` evaluators. The Rust dispatcher returns the
  second argument unchanged for every representable Datum, preserving NULL,
  signed/unsigned integer, real, string, binary, and decimal payloads; typed
  temporal/duration/JSON/vector signatures plus FieldType and column-label
  metadata remain explicit partial seams rather than fabricated Datum cases.
- [x] Added the bounded `TIDB_SHARD` scalar from
  `pkg/expression/builtin_miscellaneous.go::builtinTidbShardSig` and
  `pkg/util/vitess/vitess_hash.go::HashUint64`. The Rust leaf applies the
  source ETInt coercion, runs DES-ECB with Vitess' all-zero key, and preserves
  the 256-bucket modulo; every original `TestTidbShard` integer/string row
  plus NULL, numeric coercion, and wrong-arity boundaries execute directly.
  Go function-class metadata, warning/session state, and vectorized execution
  remain explicit partial seams.
- [x] Closed the scalar `TestIf` rows from
  `pkg/expression/builtin_control_test.go`. The Rust control dispatcher now
  performs the source condition coercion (so `1abc` and `0.1` are true while
  `0.0` is false for string conditions) and evaluates only the selected
  result branch, preserving Go's division-by-zero guard behavior. The direct
  table covers all representable integer/NULL/decimal/real/string rows plus
  both lazy-branch regressions; typed temporal/duration/JSON and injected
  error rows, FieldType result promotion, and session warning/function-class
  state remain explicit partial boundaries.
- [x] Closed the scalar `TestCaseWhen` rows from
  `pkg/expression/builtin_control_test.go`. Direct Rust vectors now cover
  every representable scalar/NULL/real truthiness row, written-order first
  match, lazy dead-branch behavior, and the unreachable JSON result row. A
  selected JSON result, the injected Go `error` condition, FieldType result
  promotion, and function-class/session state remain explicit partial seams;
  the value-only evaluator does not fabricate those typed domains.
- [x] Closed the scalar operator tables from
  `pkg/expression/builtin_op_test.go` (`TestLeftShift`, `TestRightShift`,
  `TestBitXor`, `TestBitOr`, `TestBitAnd`, `TestBitNeg`, `TestUnaryNot`, and
  `TestIsTrueOrFalse`). The Rust operator leaf now shares one numeric-prefix
  truthiness helper for unary `NOT`, `IS TRUE`/`IS FALSE`, and logical
  operators, so string values such as `'0.3'` and malformed text follow the
  source ETReal/ETInt coercion instead of becoming unsupported. Direct tables
  cover every representable integer/string/decimal/real/NULL row plus parser
  arity guards; Go error datums and typed duration/time/JSON signatures,
  FieldType construction, vectorized execution, and session warning state
  remain explicit partial seams.
- [x] Closed the representable scalar and binary coercion rows from
  `pkg/expression/builtin_string_test.go` (`TestConcat` and `TestConcatWS`).
  The Rust string leaf now preserves Go's byte-oriented `EvalString` boundary,
  propagates a NULL CONCAT argument or separator, skips only later NULL
  CONCAT_WS fields, retains empty fields, and rejects both functions' source
  arity violations before evaluation. Typed datetime/duration values,
  injected errors, max-allowed-packet warning state, and vectorized/function
  class construction remain explicit partial seams.
- [x] Added the UUID binary scalar vertical from
  `pkg/expression/builtin_miscellaneous.go` (`builtinUUIDToBinSig` and
  `builtinBinToUUIDSig`) and the complete source tables at
  `builtin_miscellaneous_test.go:563`/`:684`. `UUID_TO_BIN` now performs
  strict whitespace rejection, Google UUID spelling/NULL handling, raw
  sixteen-byte output, and Go's byte swap; `BIN_TO_UUID` consumes arbitrary
  sixteen-byte payloads without UTF-8 conversion, applies the distinct
  `swapStringUUID` field permutation, and renders lower-case canonical text.
  Warning/session channels and function-class/vectorized execution remain
  explicit partial seams. The result corpus and unique test evidence fragment
  are `uuid_binary_source.{txt,golden.txt}` and
  `expression-uuid-binary-source-wave.tsv`.
- [x] Added the dependency-closed `ResourceGroupTagBuilder.EncodeTagWithKey`
  vertical from `pkg/kv/kv.go:861-921`, its `TestResourceGroupTagEncoding`
  vectors, and the owned `DecodeTableID`/label helpers from
  `pkg/tablecodec/tablecodec.go:309-324` and
  `pkg/util/resourcegrouptag/resource_group_tag.go:39-48`. Generated
  `tidb-proto`/prost preserves the nullable=false `table_id` wire presence;
  `tidb-codec` decodes legacy table IDs and classifies row/index/unknown keys.
  Request-envelope extraction, API-V2 keyspace prefixes, next-gen kernel
  ownership, and the standalone resourcegrouptag decode utility remain
  explicit partial seams rather than hand-rolled compatibility paths. The
  source/test evidence is `txnkv-resource-group-tag-source-wave.tsv`.
- [x] Added the dependency-closed `ScaleNDV` cardinality leaf from
  `pkg/planner/cardinality/ndv.go`. `tidb_planner::cardinality::ndv::scale_ndv`
  preserves the source uniform probability calculation, skewed linear path,
  lower/upper NDV clamps, and caller-provided risk-ratio blend; all nine
  `TestScaleNDV` vectors execute through the existing planner cardinality
  target. SessionVars/property registration, histogram/statistics ownership,
  and testkit planner integration remain explicit partial seams.
