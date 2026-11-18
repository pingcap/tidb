# Structural audit, 2026-11-18

Evidence for the rows the [register](structural-findings.md) gained on
2026-11-18. Materialized views (E29) were out of scope.

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

```sh
go build -o /tmp/go-tidb-server ./cmd/tidb-server
/tmp/go-tidb-server -store unistore -path /tmp/g -P 14300 -status 20300
rust/target/debug/tidb-server --store unistore --path /tmp/r --load-privileges -P 14200 -status 20200
mysql -h127.0.0.1 -P<port> -uroot -vvv --force --table < probes.sql
```

## Observed answers

| Row | Statement | Go | Rust |
| --- | --- | --- | --- |
| A05 | `GRANT SELECT ON TABLE sa.t TO sa_u1` | OK | 1105 `GRANT ... ON FUNCTION/PROCEDURE is not supported yet` |
| A05 | `GRANT EXECUTE ON FUNCTION sa.f TO sa_u1` | 1144 `Illegal GRANT/REVOKE command; ...` | 1105 (same text as above) |
| A05 | `GRANT SELECT (b) ON sa.* TO sa_u1` | 1146 `Table 'sa.' doesn't exist` | 1105 `GRANT/REVOKE with a column list is not supported yet` |
| A07 | `-- comment only` after a 3-row INSERT | OK, 3 rows affected, `Records: 3 Duplicates: 0` | OK, 0 rows affected |
| A08 | `SHOW SESSION_STATES` on a new session | `sys-vars` has `character_set_server` | `sys-vars` has `tidb_opt_tiflash_concurrency_factor` instead |
| B03 | `tidb-server -store unistore -path ...` | starts | `unsupported option -store` |
| B04 | `SHOW GLOBAL STATUS LIKE 'Uptime'` 25 minutes after start | seconds since start | 0 on the first read, then seconds since that read |
| D13 | `CREATE TABLE ch (a INT, FOREIGN KEY (a) REFERENCES t (a) MATCH PARTIAL)` | OK, warning 1064 `The MATCH clause is parsed but ignored ...` | 1105 `MATCH PARTIAL is not supported yet` |
| E33 | `BINLOG 'abc'` | OK | 1105 `this statement kind (ADMIN Binlog) is not supported yet` |
| E33 | `REFRESH STATS t3` | OK | 1105 `... (ADMIN RefreshStats) ...` |
| E33 | ``ALTER RANGE global PLACEMENT POLICY = `default` `` | OK | 1105 `... (ADMIN AlterRange) ...` |
| E33 | `ADMIN SHOW t3 NEXT_ROW_ID` | empty set | 1105 `... (ADMIN ShowNextRowId) ...` |
| E33 | ``SET CONFIG tidb `log.level` = 'info'`` | 1105 `TiDB doesn't support to change configs online, please use SQL variables` | 1105 `... (ADMIN SetConfig) ...` |
| E33 | `SHOW DISTRIBUTION JOBS` | 1105 `get leader failed, pd http cli is nil` (no PD) | 1105 `... (ADMIN Show) ...` |
| E33 | `CANCEL DISTRIBUTION JOB 1` | 1105 `get leader failed, balance-range-scheduler` | 1105 `... (ADMIN CancelDistributionJob) ...` |
| E33 | `CREATE STATISTICS s1 (correlation) ON t3(a, b)`, `DROP STATISTICS s1`, `GRANT PROXY ...` | 8108 `Unsupported type *ast.CreateStatisticsStmt` (and so on) | 1105 `... is not supported yet` |
| E34 | `ANALYZE TABLE t UPDATE HISTOGRAM ON b WITH 2 BUCKETS`, `... DROP HISTOGRAM ON b` | OK, sample-rate note | 1105 `this node does not run UPDATE/DROP HISTOGRAM ON` |
| E34 | `ANALYZE TABLE t WITH 0.5 NDVRATE` | 1105 `Value of analyze option NDVRATE should be positive and not larger than 0` | 1105 `NDVRATE is not a knob this node reads` |
| E35 | `EXPLAIN 'abcdef'` | 1105 `can't find any plans for 'abcdef'` | 1105 `EXPLAIN of a plan digest is not supported yet` |
| E35 | `EXPLAIN FORMAT = 'cost_trace' SELECT * FROM t WHERE a = 1` | `costFormula` `N/A` | `costFormula` empty |
| E36 | `SHOW SESSION BINDINGS` | `2026-11-18 13:43:06.564` | `2026-11-18 13:43:06.590000` |
| N09 | `SHOW CONFIG WHERE name = 'log.level'` (macOS, system proxy 127.0.0.1:7897) | 1 row | empty, warning `request http://10.0.0.8:20200/config failed: 502 Bad Gateway` |
| X06 | `EXPLAIN LOAD DATA LOCAL INFILE '/nonexistent' INTO TABLE t` | 1064 at `column 12 near "LOAD DATA ..."` | 1105 `EXPLAIN of LoadData is not supported yet` |
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
