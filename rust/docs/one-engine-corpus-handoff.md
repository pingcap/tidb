# Handoff: aligning the production engine with Go (2026-10-12)

Companion to `one-engine-corpus-execplan.md`, which stays the living plan.
This file records where the work stopped, how to measure it, and what to do
next, so a new session can pick it up without the previous conversation.

Branch `hparser-integration`, pushed to the `ngaut` remote. Last commit
covered here: `f38c8d2efb`.

## The yardstick

The goal is "align with Go, fix all issues found". It is measured three ways:

1. **Production corpus replay.** Every `tests/integrationtest/t/**.test`
   runs through the production stack (`ClusterSessionFactory` over the
   embedded unistore) and is compared with Go's recording. The harness is
   `rust/crates/tidb-server/src/cluster_session_node/tests/corpus_unistore.rs`.
   - Session start: 46888 matched / 127 diverged.
   - Full replay at `ea92234161`: 46940 / 102.
   - Full replay at `f38c8d2efb` (the code state of this handoff): 46975 / 66,
     no topic worse than at `ea92234161`.
2. **In-process corpus.** The `tidb_session::Session` engine; part of the gate.
   It holds at 9 of 19960 compared statements diverging.
3. **Differential probes.** The same SQL runs on the Go oracle (this
   branch's `pkg/`, built in a worktree) and on the production node, and the
   outputs are compared. The worst bugs this session were found this way, not
   by the corpus: shared partition ids after `CREATE TABLE ... LIKE`,
   corrupted BIT defaults, and BATCH jobs that never committed.

## Commits this session (oldest first)

| Commit | What |
| --- | --- |
| `e327a07608` | `detectAndUpdateJobVersion` at DDL start, so global indexes use the V1 key format (EXCHANGE PARTITION rows no longer collapse). |
| `2bfe6881fd` | A stored string DEFAULT is Go's `types.NewDatum(string)` (utf8mb4), so GBK columns store their own text. |
| `00391ecddc` | BATCH (non-transactional DML) jobs commit one by one through the node, as Go's `ExecuteStmt` does. Clock words (`CURRENT_TIMESTAMP`/`CURRENT_DATE`) only on time columns. Window-function NOT NULL depends on the name as written (`row_number()` vs `ROW_NUMBER()`), and the parser keeps the spelling. The harness deselects the database for `connect` with no db. |
| `ea92234161` | Result-column wire flags follow Go: literal flags from `DefaultTypeForValue`, BINARY on non-string builtins, `adjustNullFlagForReturnType`, the GREATEST/LEAST int arm flags. CASE branches use Go's `FieldType.Equal`, which ignores every flag but UNSIGNED. |
| `fa7cc2d8ca` | `CREATE TABLE ... LIKE` gets its own partition ids. BIT defaults are read through `GetDefaultValue()` (raw bytes, not the lossy JSON string). UnionScan merges added rows by handle. Production `DROP STATS` route. CREATE/RENAME USER length checks (1470, `%-.70s`). Note 1061 on `CREATE INDEX IF NOT EXISTS`. `checkIndexLengthWithNewCharset` on CONVERT TO. The replay harness runs the expensive-query sweeper and per-command query cancellation (`max_execution_time` and KILL work). |
| `f38c8d2efb` | The BINARY attribute keeps its `_bin` collation under a COLLATE option. `ALTER TABLE ... COMPACT` is not a catalog change. The replay harness boots with `tests/integrationtest/config.toml`, the config the corpus was recorded under (`stats-lease = "0"`, table locks, new charset, expression indexes). |

## What is left

These are the divergences of the `ea92234161` replay (topic: count),
classified. The `f38c8d2efb` replay confirmed which were fixed.

**Fixed by `fa7cc2d8ca` and `f38c8d2efb` (36, confirmed by the full replay):**
- `executor/compact_table` 3, `executor/kv` 2, `executor/prepared` 4,
  `executor/executor` 4 (BIT default);
- `session/clustered_index` 2, `session/common` 3, `session/session` 1,
  `table/index` 1;
- `statistics/integration` 1, `planner/cardinality/selectivity` 4,
  `expression/charset_and_collation` 1;
- `executor/grant` 2 of 3 (the name-length checks);
- `explain_generate_column_substitute` 2 (LIKE partition ids);
- with no targeted fix: `planner/core/integration` 2,
  `executor/index_lookup_join` 1, `ddl/db_partition` 1,
  `planner/core/partition_pruner` 1,
  `planner/core/join_reorder_through_projection` 1.

`explain_cte` (1) did not move: its outer-row duplication is not the LIKE
bug. Re-probe it.

**Real gaps still open:**
- `executor/grant` (1): `mysql.tables_priv.Grantor` is NULL. Go writes the
  granting user. The registry stores table grants as a bare mask map
  (`tidb-session/src/privilege/registry_ops.rs`, `lock_table()`). A fix needs
  a grantor carried with each table-grant key:
  - set at GRANT (`account.rs` around the `grant_table` call);
  - loaded from storage (`tidb-exec/src/cluster_privilege_load.rs`), or the
    reconcile would blank existing rows;
  - exported through `ExportedScopedGrant` and `LoadedTableGrant`;
  - written by `cluster_account_write.rs::table_grant_rows`;
  - kept across rename and drop of users and tables.
- `executor/simple` (1): `SET DEFAULT ROLE` rows come back in a different
  physical order. Go deletes and re-inserts the user's rows; the production
  account writer reconciles them, so surviving rows keep their old row ids.
  This is the reconcile-vs-SQL structural mismatch.
- `session/temporary_table` (3): `tidb_tmp_table_max_size` (error 1114) has no
  port in either engine. Go's `checkTempTableSize` in `addRecord` compares the
  committed size plus the transaction's membuffer delta against the limit.
  Rust keeps GLOBAL temporary rows in `TemporaryTableTxnData` and LOCAL ones
  in a session `KvTable`; both need byte accounting.
- `executor/issues` (3):
  - a correlated `NOT IN` that Go refuses with 1815 (`isnull(cast(...))
    cannot be pushed down`);
  - Go's raw `%s value is out of range in '%s'` warning, which Rust emits
    before the cast warnings instead of after;
  - an ORDER BY ... LIMIT OFFSET subquery group count.
- `infoschema/tables` (1): `statements_summary.exec_count` for `select ?`.
- `executor/sample` (3): `TABLESAMPLE REGIONS()` row sets after
  `SPLIT TABLE ... REGIONS n`.
- `executor/index_lookup_join` 2, `executor/index_lookup_merge_join` 2,
  `executor/index_lookup_pushdown` 4, `index_merge` 3: wrong rows or row
  order in index joins, the merge join and index-lookup pushdown.
- `ddl/db_partition` 5, `table/partition` 1, `generated_columns` 1,
  `util/ranger` 1, `collation_pointget` 2, `bindinfo/bind` 2,
  `executor/ddl` 1 (a view over `AS OF TIMESTAMP` must refuse with 1356),
  `executor/show` 1 (`DEFAULT (nextval(seq))` must print the schema).
- Planner plan shapes (about 15, all `planner/*` and `explain_*`): the planner
  has three unbridged logical trees (memory note
  `planner-three-logical-trees`). Read the Go rule before judging an EXPLAIN
  difference (memory note `verify-the-rule-before-probing-explain`).

**Harness, not engine:**
- `new_character_set` (12): the harness decodes wire bytes as UTF-8, while
  the recording client applied the connection charset.

**Not fixable (stale recording or nondeterministic in Go):**
- `session/nontransactional` (3): the recording's `USE_INDEX(`t` )`; this
  branch's Go prints `USE_INDEX(`t`)`, as Rust does.
- `executor/simple` (1): `user_attributes` for a non-root viewer; this
  branch's Go filters with `UserAttrFilter`, as Rust does.
- `executor/aggregate` (1): Go's `var_pop(DISTINCT ...)` sums over a Go map
  in random order.

**Wire flags still differing:**
- a constant-folded JSON builtin (`json_array(1)`): Go 128, Rust 129;
- an uncorrelated scalar subquery: Go nullable. The driver folds it to an AST
  literal at plan time, which cannot carry the subquery's type.

## Next steps, in order

1. Run the four prepared differential batteries (partitions, columns and
   defaults, indexes/views/sequences, misc DDL/DML; listed in *Differential
   probes*). Triage every difference the same way: ask the Go oracle, then
   find the production code path. Write more batteries in areas where
   production has its own code: cluster DDL planner, catalog loader, account
   reconcile, statistics routes.
2. `Grantor`, then `tidb_tmp_table_max_size`, then the index-join and
   index-merge row differences.
3. Milestone 4 of the ExecPlan (retire the in-process transaction model)
   stays the structural goal.

## Commands

All from `rust/` unless noted. Never run two cargo jobs that compile the
tree while also editing source: the gate and the replay rebuild per chunk.

**Gate** (about 7 minutes). Per batch, run the owning crate's nextest; run
the full gate before every commit that is pushed.
- `cargo nextest run -p tidb-planner -p tidb-executor -p tidb-session -p tidb-expr -p tidb-ast -p tidb-exec -p tidb-model -p tidb-parser -p tidb-hint -p tidb-server --no-fail-fast`
- `cargo test -p difftest-result-tests --test integration_diff integrationtest_replay_matches_recorded_tidb_output`
  (the in-process corpus; expect 9 of 19960 diverging);
- `cargo nextest run -p tidb-chunk -p tidb-protocol -p tidb-unistore -p tidb-util -p tidb-datatype -p tidb-metadef -p tidb-lexer -p tidb-vardef -p tidb-ddl-session -p tidb-txnkv -p tidb-meta -p tidb-stmtsummary --no-fail-fast`.

Known failures, unrelated and pre-existing:
- `cluster_catalog_loader_source::loads_databases_tables_and_schema_version_from_stored_bytes`
- `cluster_stats_lock::statement_tests::lock_insert_and_meta_version_are_separate_go_statements`
- `tidb-unistore closure_executor_selection_over_point_range_answers_zero_rows`
- `tidb-txnkv lock_resolver_source caller_cancelled_status_rpc_is_typed_and_lite_cleanup_is_best_effort`

**Production replay of some topics:**

    INTEGRATION_SHOW_OUT_OF_DOMAIN=1 INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC=a/b,c/d cargo test -p tidb-server --lib corpus_unistore -- --ignored --nocapture

**Full production replay** (about 80 minutes, 45 of them `imdbload`). List
the topics (every `.test` path under `tests/integrationtest/t`, without the
extension), split them into chunks of 40, and run each chunk with the command
above, appending the output to one file. Compare the per-topic
`<m> matched <d> diverged of <n>` lines with the previous run, and list any
topic that got worse.

**In-process replay of one topic:**

    INTEGRATION_TOPIC=a/b cargo test -p difftest-result-tests --test integration_diff replay_one_topic -- --ignored --nocapture

**Commit and push**, in separate tool calls:
1. Stage explicit files only. Never stash: a peer session works in the same
   checkout (memory note `shared-checkout-peer-protocol`).
2. Commit with
   `git -c core.hooksPath=hooks -c user.name=ngaut -c user.email=ngaut@users.noreply.github.com commit -F <msg>`
   and check that the hook log prints `OK`. Never use `--no-verify`. The
   message ends with the `Co-Authored-By` line.
3. Run `cargo build --manifest-path <repo>/rust/Cargo.toml --locked -p tidb-server`.
4. Run `git push ngaut hparser-integration`, then confirm with
   `git ls-remote ngaut hparser-integration`. `origin` needs 2FA; never
   force-push.

**Formatting.**
- Format only your own hunks: rustfmt the file, then keep the formatted
  version only for diff hunks you changed. A whole-file rustfmt churns
  untouched code.
- After formatting, check that no `mod`/`use` lines were dropped:
  `git diff | grep '^-' | grep -E '^-\s*(pub )?(mod|use) '`.
- New files get a whole-file `rustfmt --edition 2021`.

## Differential probes

**Go oracle.** Build this branch's Go TiDB in a separate worktree (memory
note `go-oracle-probe-harness`: go1.27 hack tag, `-tags=intest,deadlock`,
root auth).
- For each probe, write a test file into `pkg/executor/test/writetest/`.
  It runs each SQL line through a helper that prints
  `PROBE <sql> => <rows>|OK|ERR <type> code=<n> <msg>`. Rows are
  `session.ResultSetToStringSlice` output, printed as `[[a b] [c d]]`, with
  NULL shown as `<nil>`.
- Run it with
  `go test -tags=intest,deadlock ./pkg/executor/test/writetest/ -run TestZZ<Name>$ -count=1 -v | grep ^PROBE`,
  then delete the file.

**Production side.** Temporarily append this test to `corpus_unistore.rs`,
run it with `PROBE_SQL=<file> cargo test -p tidb-server --lib zz_diff_probe -- --nocapture | grep ^PROBE`,
then restore the file:

    #[test]
    fn zz_diff_probe() {
        let text = std::fs::read_to_string(std::env::var("PROBE_SQL").unwrap()).unwrap();
        let mut pool = UnistoreConnections::open("test").unwrap();
        let session = pool.sessions.get_mut(DEFAULT_CONNECTION).unwrap();
        for line in text.lines() {
            let sql = line.trim().trim_end_matches(';').trim();
            if sql.is_empty() || sql.starts_with('#') { continue; }
            let result = match session.run_with_columns(sql) {
                Ok(StmtOutput::Rows { rows, .. }) => {
                    let cell = |d: &tidb_datatype::Datum| match d {
                        tidb_datatype::Datum::Null => "<nil>".to_owned(),
                        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                        other => format!("{other:?}"),
                    };
                    let rows: Vec<String> = rows.iter()
                        .map(|r| format!("[{}]", r.iter().map(cell).collect::<Vec<_>>().join(" ")))
                        .collect();
                    format!("[{}]", rows.join(" "))
                }
                Ok(_) => "OK".to_owned(),
                Err(DriverError::Mysql(e)) => format!("ERR code={} {}", e.code, e.message),
                Err(other) => format!("ERR code=-1 {other:?}"),
            };
            println!("PROBE {sql} => {result}");
        }
    }

**Comparing.** Match the two outputs statement by statement, reducing errors
to `ERR <code>`.

**Batteries.** Four were written this session and their Go outputs captured;
the production runs were still pending. They exercise:
- partitions: range/hash/list, add/drop/truncate/reorganize/coalesce,
  exchange, LIKE, remove partitioning, analyze;
- columns and defaults: every type's default, add/drop/modify/change,
  generated columns, multi-action ALTER;
- indexes, views and sequences: invisible and expression indexes, prefix
  limits, view column lists, sequence restart and cycle;
- misc: charset conversion, unique with NULLs, ON DUPLICATE, auto_random,
  temporary tables, flashback/recover, non-strict casts.

They live in this session's scratchpad, which may be gone. Re-create them from
this list if needed.

**Wire column flags.** Probe them through the column-definition packets
(`crate::connection_resultset::write_connection_result_set`, or
`ResultSetSource::columns`). The harness's `run_with_columns` decodes cells as
plain strings, so its `FieldType`s carry no flags.

## Lessons from this session

- The replay environment matters as much as the engine. Booting the harness
  with the recording config fixed whole topics; a harness without the
  expensive-query sweeper made `max_execution_time` look broken.
- Ask the Go oracle on this branch before fixing a recording difference.
  Three apparent divergences were stale recordings, and the oracle also
  settled the window-name spelling rule, which the code alone did not reveal.
- A test that pins existing behaviour is an open question, not a contract.
  Five window tests pinned the lowercased-name behaviour Go does not have.
- `git checkout <file>` to drop a probe also drops uncommitted edits in that
  file. Keep a copy of the file before appending probes, and restore from
  that copy.
