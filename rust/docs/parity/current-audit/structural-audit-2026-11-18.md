# Structural audit, 2026-11-18

Evidence for the rows the [register](structural-findings.md) gained on
2026-11-18. Materialized views (E29) were out of scope. The first pass
(method steps 1-3) found the behavior rows; the second pass (step 4) looked
for Go modules that have more than one Rust implementation (X08-X13, and the
last part of N09).

## Method

1. Package coverage. Every non-test Go package linked into `cmd/tidb-server`
   (`go list -deps ./cmd/tidb-server`, 451 `pkg/` packages) was matched to a
   Rust crate by name, by the Go path a crate's `lib.rs` names, or by Go
   symbols in the Rust sources. Unmatched packages were read to decide whether
   their behavior lives elsewhere (X05, N07, N08, A06) or is dead or Go-runtime
   only in Go itself (`pkg/planner/cascades/old`, `pkg/planner/memo`,
   `pkg/planner/implementation`, `pkg/util/gctuner`, `pkg/plugin`'s `.so`
   loading), which is not a finding.
2. Refusal scan. Every `Unsupported` / `unsupported(...)` site outside tests
   whose text is not a Go string was listed; the reachable ones were probed.
3. Live differential. A Go server built from this tree's `pkg/` and the Rust
   `tidb-server` (`cargo build --locked -p tidb-server`, debug) ran side by
   side on unistore; the same scripts went through `mysql -vvv --force` and the
   answers were compared after masking timestamps.
4. Duplicate implementations. Every Rust `fn` / type whose doc comment starts
   with Go `X` was grouped by `X`; a Go package-level function or method that
   is defined once in `pkg/` but claimed by definitions in more than one Rust
   file was read (448 such symbols, most of them layers of one call chain, a
   trait and its implementation, or a test helper, which are not findings).
   Separately: crates that name the same Go package, Rust trait
   implementations of one storage, allocator or snapshot seam, and the
   `mysql.*` access sites. Kept were the cases where two Rust code paths do
   the same Go job independently.

```sh
go build -o /tmp/go-tidb-server ./cmd/tidb-server
/tmp/go-tidb-server -store unistore -path /tmp/g -P 14300 -status 20300
rust/target/debug/tidb-server --store unistore --path /tmp/r --load-privileges -P 14200 -status 20200
mysql -h127.0.0.1 -P<port> -uroot -vvv --force --table < probes.sql
```

## Observed answers

| Row | Statement | Go | Rust |
| --- | --- | --- | --- |
| A08 | `SHOW SESSION_STATES` on a new session | `sys-vars` has `character_set_server` | `sys-vars` has `tidb_opt_tiflash_concurrency_factor` instead |
| B03 | `tidb-server -store unistore -path ...` | starts | `unsupported option -store` |
| E33 | `BINLOG 'abc'` | OK | 1105 `this statement kind (ADMIN Binlog) is not supported yet` |
| E33 | `REFRESH STATS t3` | OK | 1105 `... (ADMIN RefreshStats) ...` |
| E33 | ``ALTER RANGE global PLACEMENT POLICY = `default` `` | OK | 1105 `... (ADMIN AlterRange) ...` |
| E33 | `ADMIN SHOW t3 NEXT_ROW_ID` | empty set | 1105 `... (ADMIN ShowNextRowId) ...` |
| E33 | ``SET CONFIG tidb `log.level` = 'info'`` | 1105 `TiDB doesn't support to change configs online, please use SQL variables` | 1105 `... (ADMIN SetConfig) ...` |
| E33 | `SHOW DISTRIBUTION JOBS` | 1105 `get leader failed, pd http cli is nil` (no PD) | 1105 `... (ADMIN Show) ...` |
| E33 | `CANCEL DISTRIBUTION JOB 1` | 1105 `get leader failed, balance-range-scheduler` | 1105 `... (ADMIN CancelDistributionJob) ...` |
| E33 | `CREATE STATISTICS s1 (correlation) ON t3(a, b)`, `DROP STATISTICS s1`, `GRANT PROXY ...` | 8108 `Unsupported type *ast.CreateStatisticsStmt` (and so on) | 1105 `... is not supported yet` |
| E35 | `EXPLAIN 'abcdef'` | 1105 `can't find any plans for 'abcdef'` | 1105 `EXPLAIN of a plan digest is not supported yet` |
| E35 | `EXPLAIN FORMAT = 'cost_trace' SELECT * FROM t WHERE a = 1` | `costFormula` `N/A` | `costFormula` empty |
| N09 | `SHOW CONFIG WHERE name = 'log.level'` (macOS, system proxy 127.0.0.1:7897) | 1 row | empty, warning `request http://10.0.0.8:20200/config failed: 502 Bad Gateway` |
| X06 | `EXPLAIN LOAD DATA LOCAL INFILE '/nonexistent' INTO TABLE t` | 1064 at `column 12 near "LOAD DATA ..."` | 1105 `EXPLAIN of LoadData is not supported yet` |
| X06 | `EXPLAIN CALL p()` | 1064 `column 12 near "CALL p()"` | 1105 `EXPLAIN of Call is not supported yet` |
| X06 | `EXPLAIN SHOW TABLES` | 1064 `column 12 near "SHOW TABLES"` | 1064 `column 19 near ""` |
| X06 | `EXPLAIN SET @a = 1` | 1064 `column 11 near "SET @a = 1"` | 1064 `column 18 near ""` |
| X06 | `ADMIN SHOW TELEMETRY` | 1064 `column 5 near "ADMIN SHOW TELEMETRY"` | 1064 `column 20 near ""` |
| X07 | `SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1+0 PRECEDING AND CURRENT ROW) s FROM t` | 1064 near `+0 PRECEDING ...` | 4 rows |

Probed with equal answers (not findings): `CAST ... AS UNSIGNED ARRAY`
outside an index, expression indexes, generated and non-literal column
defaults, the partition expressions tried (`a DIV 2`, `TO_DAYS`,
`UNIX_TIMESTAMP`, `a MOD 3 + b`, `ABS`), subpartitions, `ALTER TABLE ...
COMPACT TIKV REPLICA`, `LOAD STATS`, stale reads (`AS OF TIMESTAMP`,
`tidb_read_staleness`), `CALIBRATE RESOURCE`, `QUERY WATCH`, `TRACE`,
`PLAN REPLAYER DUMP`, sequences, temporary and cached tables, placement
policies, resource groups, TTL tables, FLASHBACK / RECOVER TABLE, SPLIT
TABLE, hypothetical indexes, savepoints, prepared statements, non-transactional
DML, roles, the ADMIN DDL job and binding commands, `ADMIN CHECK` /
`RECOVER` / `CLEANUP INDEX`, the pushed-down predicates tried (`BIT_COUNT`,
`PERIOD_ADD`, `PERIOD_DIFF`, `CEIL`, `FLOOR`, `IS NULL` on JSON, `REGEXP`),
and the information_schema, performance_schema and sys tables listed in the
scripts.

## Not registered

- The Go parser of this tree spins (one core at 100%, no answer) on
  `CREATE PROCEDURE p1() BEGIN SELECT 1`, which the mysql client sends when it
  splits `CREATE PROCEDURE p1() BEGIN SELECT 1; END;` at its first `;`. The
  Rust parser answers 1064. This is a Go bug, not a Rust mismatch.
- `ADMIN SHOW SLOW` returned rows only on the Rust node because the debug build
  ran the probes slower than `tidb_slow_log_threshold`.

## Duplicate implementations (second pass)

| Row | Go module | Rust implementations |
| --- | --- | --- |
| X09 | system tables through SQL on an internal session | restricted SQL, and `tidb-syssession`'s `SystemTableView` / `system_row_write` key/value reader-writer; `ddl_reorg_table.rs` and `ddl_delete_range.rs` use both |
| X11 | one `kv.Transaction` path, tests on `mockstore` | `ClusterTableStorage` in production, and `MemTableStorage` + `LocalTableAutoIds` + `run_create_table_on` under 50 `tidb-executor` test files |
| X13 | each `SysVar`'s closures | `SysVarDef.validation` + about 30 name branches in `set_hooks.rs` + session hooks + `StaticSessionVars` |
| N09 | `util.InternalHTTPClient`, `pdhttp.Client` | about a dozen separately built `reqwest` clients and ad hoc `/pd/api/v1/...` calls in five crates |

Checked and not kept: `tidb-format` reuses `tidb-parser-format`;
`tidb_expr::rewriter` is shared by the planner's `expression_rewriter.rs`
(Go's one `expressionRewriter` split by layer, not restated); the
coprocessor `handle_cop_plan` / `handle_pb_plan` chain and the DDL
`ClusterDdl` trait over `executor.rs` are layers of one implementation;
`tidb-lightning-backend-kv`'s `MemBuf` mirrors Go's lightning `kv` session.

Correction made in review: X06 first said `EXPLAIN BATCH ...` also parses
here; it does not (both nodes answer 1064 at the same column), so only
`EXPLAIN CALL` is listed beside `EXPLAIN LOAD DATA`. B03 first listed
`--affinity-cpus` as Rust-only; Go has `-affinity-cpus`, so it was removed.
