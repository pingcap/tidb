# Local TiFlash Boolean MATCH E2E

This suite derives fixed result sets from `endless/testcase/fts/function` and `endless/testcase/fts/mysql-test2`. It verifies the real TiDB → TiFlash Boolean-filter execution path, not the TiCI inverted-index path. It creates and drops only a uniquely named `fts_e2e_` schema. It does not start a cluster, modify the QA repository, or change global system variables. The DSN must connect over local TCP or a Unix socket.

Start a dedicated TiUP playground using the current TiDB and TiFlash builds:

```bash
make server
tiup playground v8.5.8 --without-monitor --db 1 --kv 1 --pd 1 --tiflash 1 --db.binpath "$PWD/bin/tidb-server" --tiflash.binpath /Users/solotzg/Work/tiflash/cmake-build-fts-apple/dbms/src/Server/tiflash
```

In another terminal, run:

```bash
TIDB_FTS_E2E_DSN='root@tcp(127.0.0.1:4000)/?charset=utf8mb4' go test ./tests/ftse2e -run '^Test(BooleanMatchTiFlashE2E|LocalMatch.*TiFlashE2E)$' -count=1 -v
```

Without `TIDB_FTS_E2E_DSN`, the test skips. It prints `tidb_version()`, creates matching tables with and without a TiFlash replica, waits for `information_schema.tiflash_replica.AVAILABLE=1`, and checks both plans and fixed expected row IDs. Supported native queries must use `mpp[tiflash] TableFullScan` and evaluate MATCH in an MPP TiFlash `Selection` with no TiDB-side residual. Queries on the no-replica tables must retain local `match_against` and must not use TiFlash or TiCI. Result IDs are sorted by the test runner rather than by SQL, because SQL `ORDER BY` can change the chosen TiFlash plan. The default matrix covers required/excluded terms, phrase, prefix, NULL, ordered composite-column MATCH, five multi-MATCH combinations (OR, AND, AND NOT, OR over composite MATCH columns, and NGRAM AND), incorrect composite-index order, committed INSERT/UPDATE/DELETE, Chinese NGRAM and prefix matching, five collations, and a compound MATCH across binary and case/accent-insensitive collations. The NGRAM cases require `ngram_token_size=2` and skip with an explicit message otherwise.

The review regressions reuse binary-protocol prepared statements across integer, string, and NULL search parameters. Statement summary checks the sampled executed non-NULL MATCH plans; these prepared statements may use either TiFlash MPP or TiFlash coprocessor Selection, but must not leave MATCH in a root Selection. This requires the test cluster's `tidb_enable_stmt_summary` to be ON (the test checks it without changing GLOBAL variables). It avoids the stale plan returned by this branch's `EXPLAIN FOR CONNECTION` after binary EXECUTE and the independent argument rewriting performed by a prepared EXPLAIN. STANDARD split prefixes such as `+foo.bar*`, `foo.bar*`, and `baz -foo.bar*` are tested separately: MATCH must remain in a root Selection, even if the replica supplies the table scan, and results must match the no-replica TiDB evaluator. These split-prefix terms are not currently eligible for TiFlash scalar pushdown.

The token-semantics matrix also checks ordinary STANDARD split words (`foo.bar`, `+foo.bar`, and `baz -foo.bar`), emoji delimiters, phrases across delimiters, Unicode letters and numbers, and NGRAM queries that must not cross a delimiter. The Local MATCH protocol-v1 word-character rule is fixed to Unicode 15.0.0 categories L and N plus ASCII underscore, using identical generated range tables in TiDB and TiFlash; Go/Poco upgrades must not silently change it. Characters first assigned after that version are delimiters. The Unicode NGRAM cases run with stopwords disabled and support token sizes 2 and 3 without changing GLOBAL variables.

The extended tests add 480 deterministic differential queries (seed 70484/70485) across STANDARD/NGRAM, five MATCH and server collations, and stopwords ON/OFF. They require matching row IDs and TiFlash Selection plans. The snapshot test holds both read transactions open across seven commits on another connection, checks repeatable-read results against newly committed results, and replays a historical `tidb_snapshot`. These are controlled overlapping transactions, not a randomized concurrent stress test. The large-document test checks seven queries over 991,242 bytes of MEDIUMTEXT, including a 1,028-byte Boolean search, and logs single-run wall times. It asserts correctness and plans, not a latency SLA.

To test legacy collation semantics, bootstrap a separate playground with a TiDB configuration file containing `new_collations_enabled_on_first_bootstrap = false`, then run:

```bash
TIDB_FTS_LEGACY_E2E_DSN='root@tcp(127.0.0.1:4401)/?charset=utf8mb4' go test ./tests/ftse2e -run '^TestLegacyCollationLocalMatchTiFlashE2E$' -count=1 -v
```

This test verifies the persisted bootstrap mode, case-sensitive matching and built-in stopword lookup, and root Local MATCH evaluation both with and without a replica. Old-collation mode is not currently eligible for TiFlash scalar pushdown; TiFlash may still supply the scan. The test cannot change the mode of an already bootstrapped cluster.

Index-bound analyzer configuration, custom stopword tables, TiCI `IndexRangeScan`, and FTS selectivity statistics are outside this brute-force pushdown suite.

## Latest verification

On 2026-10-09, the complete selected default suite passed: 43 baseline result-and-plan scenarios, 480 seeded differential scenarios, repeatable-read and historical-snapshot checks across seven commits, and seven large-document queries. A separately bootstrapped legacy-collation cluster passed all four scenarios. The local TiUP playground used official v8.5.8 PD/TiKV with a local TiDB build at runtime commit `b5372777eb` (dirty only because these test/documentation additions were uncommitted) and a DEBUG TiFlash build at committed revision `cc51474ae1`. `tidb_version()` and the refreshed TiFlash version banner confirmed those runtime revisions. These are local-build verification results, not official release-binary acceptance. All 16 Local MATCH/character-classification gtests passed after the TiFlash rebuild; related Go tests had passed before this test-only expansion. Both temporary clusters and their test schemas were cleaned up.

The 13 token-semantics scenarios also passed with token size 3 earlier on the same day, before the metadata-only TiFlash rebuild and this test-only expansion. The new 480-case differential matrix was run at token size 2, not 3.

The large-document run is correctness evidence only. On this small dataset, DEBUG TiFlash took approximately 354–371 ms for the ordinary queries versus 75–79 ms in TiDB; the 1,028-byte query took approximately 1.75 s versus 74 ms. This does not demonstrate acceleration. A Release-build benchmark at representative data volume and concurrency, randomized concurrency stress, and a full repository test run remain outside the completed verification.

Both runtimes currently use Tipb revision `f46c2bcf8ac2`, the head of the still-open feature protocol PR [pingcap/tipb#432](https://github.com/pingcap/tipb/pull/432). The scalar-ID reservation [pingcap/tipb#431](https://github.com/pingcap/tipb/pull/431) is also still open. The official `feature/release-8.5-fts` dependency does not yet contain this protocol; switch both runtimes to the official dependency only after the required changes are merged.
