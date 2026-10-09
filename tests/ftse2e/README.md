# Local TiFlash Boolean MATCH E2E

This suite derives fixed result sets from `endless/testcase/fts/function` and `endless/testcase/fts/mysql-test2`. It verifies the real TiDB → TiFlash Boolean-filter execution path, not the TiCI inverted-index path. It creates and drops only a uniquely named `fts_e2e_` schema. It does not start a cluster, modify the QA repository, or change global system variables. The DSN must connect over local TCP or a Unix socket.

Start a dedicated TiUP playground using the current TiDB and TiFlash builds:

```bash
make server
tiup playground v8.5.8 --without-monitor --db 1 --kv 1 --pd 1 --tiflash 1 --db.binpath "$PWD/bin/tidb-server" --tiflash.binpath /Users/solotzg/Work/tiflash/cmake-build-fts-apple/dbms/src/Server/tiflash
```

In another terminal, run:

```bash
TIDB_FTS_E2E_DSN='root@tcp(127.0.0.1:4000)/?charset=utf8mb4' go test ./tests/ftse2e -run TestBooleanMatchTiFlashE2E -count=1 -v
```

Without `TIDB_FTS_E2E_DSN`, the test skips. It prints `tidb_version()`, creates matching tables with and without a TiFlash replica, waits for `information_schema.tiflash_replica.AVAILABLE=1`, and checks both plans and fixed expected row IDs. Supported native queries must use `mpp[tiflash] TableFullScan` and evaluate MATCH in an MPP TiFlash `Selection` with no TiDB-side residual. Queries on the no-replica tables must retain local `match_against` and must not use TiFlash or TiCI. Result IDs are sorted by the test runner rather than by SQL, because SQL `ORDER BY` can change the chosen TiFlash plan. The default matrix covers required/excluded terms, phrase, prefix, NULL, ordered composite-column MATCH, five multi-MATCH combinations (OR, AND, AND NOT, OR over composite MATCH columns, and NGRAM AND), incorrect composite-index order, committed INSERT/UPDATE/DELETE, Chinese NGRAM and prefix matching, five collations, and a compound MATCH across binary and case/accent-insensitive collations. The NGRAM cases require `ngram_token_size=2` and skip with an explicit message otherwise.

The review regressions reuse binary-protocol prepared statements across integer, string, and NULL search parameters. Statement summary checks the sampled executed non-NULL MATCH plans; these prepared statements may use either TiFlash MPP or TiFlash coprocessor Selection, but must not leave MATCH in a root Selection. This requires the test cluster's `tidb_enable_stmt_summary` to be ON (the test checks it without changing GLOBAL variables). It avoids the stale plan returned by this branch's `EXPLAIN FOR CONNECTION` after binary EXECUTE and the independent argument rewriting performed by a prepared EXPLAIN. STANDARD split prefixes such as `+foo.bar*`, `foo.bar*`, and `baz -foo.bar*` are tested separately: MATCH must remain in a root Selection, even if the replica supplies the table scan, and results must match the no-replica TiDB evaluator. These split-prefix terms are not currently eligible for TiFlash scalar pushdown.

To test legacy collation semantics, bootstrap a separate playground with a TiDB configuration file containing `new_collations_enabled_on_first_bootstrap = false`, then run:

```bash
TIDB_FTS_LEGACY_E2E_DSN='root@tcp(127.0.0.1:4401)/?charset=utf8mb4' go test ./tests/ftse2e -run '^TestLegacyCollationLocalMatchTiFlashE2E$' -count=1 -v
```

This test verifies the persisted bootstrap mode, case-sensitive matching and built-in stopword lookup, and root Local MATCH evaluation both with and without a replica. Old-collation mode is not currently eligible for TiFlash scalar pushdown; TiFlash may still supply the scan. The test cannot change the mode of an already bootstrapped cluster.

Index-bound analyzer configuration, custom stopword tables, TiCI `IndexRangeScan`, and FTS selectivity statistics are outside this brute-force pushdown suite.

On 2026-10-09, all 30 default result-and-plan scenarios and all four legacy-collation scenarios passed using TiDB commit `4582a706bb` with the review fixes in the working tree and TiFlash commit `6281c5659f`. Integer prepared parameters produced the correct serialized query and matched the TiDB evaluator; sampled executed plans confirmed TiFlash coprocessor Selection without a root MATCH residual. Split STANDARD prefixes and old-collation mode retained a TiDB root MATCH Selection, including when TiFlash supplied the scan. The binaries were local builds, not official release binaries.

On 2026-09-30, all 22 result-and-plan subtests plus the incorrect-index-order error assertions passed on a local TiUP v8.5.8 cluster using TiDB commit `4fad9b2f76` with the multi-MATCH changes in this worktree and `/Users/solotzg/Work/tiflash/cmake-build-fts-apple/dbms/src/Server/tiflash`. This includes TiDB fallback result parity, a compound NGRAM predicate, a compound predicate across binary and case/accent-insensitive collations, and confirmation that compound Boolean predicates execute in a TiFlash MPP `Selection` rather than a TiDB-side residual.
