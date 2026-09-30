# Local TiFlash Boolean MATCH E2E

This suite derives fixed result sets from `endless/testcase/fts/function` and `endless/testcase/fts/mysql-test2`. It verifies the real TiDB → TiFlash Boolean-filter execution path, not the TiCI inverted-index path. It creates and drops only a uniquely named `fts_e2e_` schema. It does not start a cluster, modify the QA repository, or change global system variables. The DSN must connect over local TCP or a Unix socket.

Start a dedicated TiUP playground using the current TiDB and TiFlash builds:

```bash
GOCACHE=/tmp/tidb-fts-go-cache make server
tiup playground v8.5.8 --without-monitor --db 1 --kv 1 --pd 1 --tiflash 1 --db.binpath "$PWD/bin/tidb-server" --tiflash.binpath /Users/solotzg/Work/tiflash/cmake-build-fts-apple/dbms/src/Server/tiflash
```

In another terminal, run:

```bash
TIDB_FTS_E2E_DSN='root@tcp(127.0.0.1:4000)/?charset=utf8mb4' go test ./tests/ftse2e -run TestBooleanMatchTiFlashE2E -count=1 -v
```

Without `TIDB_FTS_E2E_DSN`, the test skips. It prints `tidb_version()`, creates matching tables with and without a TiFlash replica, waits for `information_schema.tiflash_replica.AVAILABLE=1`, and checks both plans and fixed expected row IDs. Native queries must use `mpp[tiflash] TableFullScan`; single-MATCH queries have no residual `Selection`, while compound-MATCH predicates must execute in an MPP TiFlash `Selection` with no TiDB-side residual. Fallback queries must retain local `match_against` and must not use TiFlash or TiCI. Result IDs are sorted by the test runner rather than by SQL, because SQL `ORDER BY` can change the chosen TiFlash plan. The default matrix covers required/excluded terms, phrase, prefix, NULL, ordered composite-column MATCH, five multi-MATCH combinations (OR, AND, AND NOT, OR over composite MATCH columns, and NGRAM AND), incorrect composite-index order, committed INSERT/UPDATE/DELETE, Chinese NGRAM and prefix matching, five collations, and a compound MATCH across binary and case/accent-insensitive collations. The NGRAM cases require `ngram_token_size=2` and skip with an explicit message otherwise.

Index-bound analyzer configuration, custom stopword tables, TiCI `IndexRangeScan`, and FTS selectivity statistics are outside this brute-force pushdown suite.

On 2026-09-30, all 22 result-and-plan subtests plus the incorrect-index-order error assertions passed on a local TiUP v8.5.8 cluster using TiDB commit `4fad9b2f76` with the multi-MATCH changes in this worktree and `/Users/solotzg/Work/tiflash/cmake-build-fts-apple/dbms/src/Server/tiflash`. This includes TiDB fallback result parity, a compound NGRAM predicate, a compound predicate across binary and case/accent-insensitive collations, and confirmation that compound Boolean predicates execute in a TiFlash MPP `Selection` rather than a TiDB-side residual.
