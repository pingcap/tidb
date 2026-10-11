# Measure and run the corpus on the production engine

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan must be maintained according to it.


## Purpose / Big Picture


The Go-alignment yardstick of this port is the integration corpus: every `tests/integrationtest/t/**.test` script replayed and compared with TiDB's own recording in `tests/integrationtest/r/**.result`. Today that replay (`rust/difftests/result-tests/tests/integration_diff.rs`) drives `tidb_session::Session` over the IN-PROCESS engine: a `tidb_executor::Catalog` whose tables keep their rows in `MemTableStorage`, with a transaction modelled as a private copy of the whole catalog. The binary users run, `tidb-server --store unistore` (and against TiKV), executes the same SQL layer over a different engine: `ClusterServerSession` on client-rust's native transaction (one MemDB per transaction, MVCC snapshots by timestamp, pessimistic locks and their waiter/deadlock detector in the store).

Two engines mean two answers to every transactional question, and the corpus only measures the one production does not run. Every recent transactional fix to the in-process engine -- per-key commit rebase, pessimistic read views, an in-process lock table, statement-level rollback of the write log -- re-derives on a catalog image what the native transaction already does the Go way. What is still missing there (a writer waiting for a lock, deadlock detection, MemDB-size accounting for temporary tables) would have to be re-derived again.

After this plan, the corpus replays through the production stack -- `ClusterSessionFactory` over the embedded unistore, exactly as Go's own integration tests run over `mockstore.EmbedUnistoreDriver` -- and that number is the alignment measure. The in-process transaction model is then retired rather than extended. A reader can observe the result by running the unistore replay for a topic and seeing its matched/diverged counts next to the in-process ones, and finally by seeing the in-process transaction code gone with the corpus no worse.


## Progress


- [x] (2026-10-11) Survey: `tidb-server --store unistore` builds the production node over `tidb-unistore` (`rust/crates/tidb-server/src/unistore_node.rs`), and `unistore_cluster_session_stack` already serves in-tree tests (`rust/crates/tidb-server/src/cluster_session_node/tests/unistore_cop.rs`). The replay harness has no backend for it.
- [x] (2026-10-11) Milestone 1: the replay engine moved from `difftests/result-tests/tests/integration_diff.rs` into `difftests/result-tests/src/replay.rs` behind `ReplaySession`/`ReplayBackend`; the in-process `Connections` implements it with unchanged counts; `rust/crates/tidb-server/src/cluster_session_node/tests/corpus_unistore.rs` implements it over `unistore_cluster_session_stack`, driving each statement in the connection loop's COM_QUERY order and decoding the server's own result-set packets. First topics measured (Artifacts).
- [ ] Milestone 2: whole-corpus unistore replay with a recorded baseline; triage of every divergence and hidden failure into a list.
- [ ] Milestone 3: unistore replay joins the gate; its divergences are fixed in priority order.
- [ ] Milestone 4: the in-process transaction model is retired.


## Surprises & Discoveries


- Observation: the in-process engine had to grow a second implementation of Go's pessimistic semantics (read at `for_update_ts`, a lock table, NOWAIT/1205) to answer two corpus statements, while the production engine already holds the real ones.
  Evidence: commits 74763de0bb and cf5e5c1dcc (`rust/crates/tidb-session/src/txn.rs`, `rust/crates/tidb-executor/src/driver/catalog/pessimistic_locks.rs`).


- Observation: the production node's DDL is a separate, narrower implementation than the in-process one -- a second structural split beside the transaction model. CREATE/DROP SEQUENCE, a column name qualified by its table in CREATE TABLE, a generated column referring to a later column, and most `ALTER TABLE ... CHANGE COLUMN` forms are refused there ("this node changes the cluster's catalog for CREATE TABLE, DROP TABLE, ..."; "serves the rename-with-default subset only"), and their dependent statements cascade.
  Evidence: `executor/executor` replays 1692 matched / 31 diverged / 48 hidden failures on unistore against 1769 / 0 / 0 in process; all 7 hidden failures of `session/temporary_table` and all 3 of `executor/update` on unistore are sequence DDL.
- Observation: calling `QuerySession::execute` for every statement is not how a connection drives the node: `BEGIN` must go through `control_transaction_parsed` and a write through `execute_write_parsed`, or a later statement panics ("SQL buffer outlived its native transaction"). The backend now follows the connection loop's order; `mysql_connection::record_text_write_failure` is the shared warning rule.
  Evidence: `begin pessimistic; update ...` panicked at `rust/crates/tidb-executor/src/cluster_storage.rs:285` through `execute`, and passes through the connection order.
- Observation: a row-oriented result source carries no `field_types()`; the connection writer formats rows from `columns()`. Comparing the server's framed packets, decoded as a client decodes them, is the only rendering that matches the wire.
  Evidence: SHOW WARNINGS rendered with an empty header until the backend decoded `connection_resultset::write_connection_result_set` output.

## Decision Log


- Decision: measure first, retire second. The unistore backend is added beside the in-process one and both are reported until the unistore number is at least as good on every topic.
  Rationale: the in-process engine is the only one the corpus has ever measured; switching blind would turn every production-path gap into an apparent regression with no baseline to triage against.
  Date/Author: 2026-10-11, agent.
- Decision: the backend lives in a `tidb-server` integration test that reuses the harness's script parser and comparison from `difftest-result-tests` (a dev-dependency), not in `difftest-result-tests` depending on `tidb-server`.
  Rationale: `unistore_cluster_session_stack` and the session fixtures are crate-internal to `tidb-server`; the parser/comparison are already a library (`rust/difftests/result-tests/src`). The dependency points from the bigger crate to the harness library, so no cycle and no widened `tidb-server` API.
  Date/Author: 2026-10-11, agent.


## Outcomes & Retrospective


Nothing yet.


## Context and Orientation


Terms used here:

The corpus is the set of mysqltest scripts under `tests/integrationtest/t`. Each script is a sequence of SQL statements plus directives (`--error`, `--sorted_result`, `connect (...)`, ...); its recording under `tests/integrationtest/r` is TiDB's text output for each statement.

A topic is one script, named by its path without extension (`executor/update`).

A divergence is a statement whose output differs from the recording. A hidden failure ("out of domain" in the harness) is a statement Rust refuses where TiDB answered; the harness counts it as skipped, not as a divergence, which is why both numbers are reported.

The in-process engine: `tidb_session::Session::new()` / `Session::with_catalog(...)` over `tidb_executor::Catalog` (`rust/crates/tidb-executor/src/driver/catalog.rs`) whose `KvTable`s use `MemTableStorage` (`rust/crates/tidb-executor/src/storage.rs`). A transaction is `tidb_session::txn::Transaction`: a cloned catalog (`working`), the image it began on (`read_snapshot`), and, since cf5e5c1dcc, a claim on `PessimisticLocks`.

The production engine: `rust/crates/tidb-server/src/cluster_session_node/mod.rs` (`ClusterSessionFactory`, `ClusterServerSession`) over `cluster_storage` (`rust/crates/tidb-executor/src/cluster_storage.rs`), whose writes stage in client-rust's native MemDB and whose reads go to the store at a timestamp. `rust/crates/tidb-server/src/unistore_node.rs` builds it over `tidb-unistore` (an embedded MVCC store with Go unistore's lock store, lock waiter and deadlock detector) instead of TiKV.

The harness: `rust/difftests/result-tests/src/mysqltest_script.rs` parses scripts and aligns them with recordings; `rust/difftests/result-tests/src/mysqltest_connections.rs` runs `connect`/`connection`/`disconnect` over in-process sessions; `rust/difftests/result-tests/tests/integration_diff.rs` executes and compares (`replay_one_topic`, `integrationtest_replay_matches_recorded_tidb_output`).


## Plan of Work


Milestone 1 adds a connection abstraction to the harness library: the operations `integration_diff.rs` performs on a `Session` (run a statement and get rows with column types, an affected count, or an error with a MySQL code; read the last statement's warnings; open, switch and close named connections) become a trait in `rust/difftests/result-tests/src`, implemented for the in-process `Session` exactly as today. A new `tidb-server` integration test, `rust/crates/tidb-server/tests/corpus_unistore.rs`, implements the same trait over `ClusterSessionFactory::open_session` and `QuerySession::execute` on a fresh `unistore_cluster_session_stack` per topic (Go's integration runner starts one server per run; a fresh store per topic keeps topics independent, matching the in-process replay). It is `#[ignore]`d like `replay_one_topic` and takes the same `INTEGRATION_TOPIC` variable. Result rows are drained from `QueryResult` into the text the wire would carry, which is what the recording holds.

Milestone 2 runs every topic through the unistore backend and records, per topic, matched/diverged/hidden-failure counts next to the in-process counts, in a baseline file under this document's directory. Each divergence and hidden failure is classified: an engine gap present only on the production path, a gap shared by both, or an environment difference of the backend (and then fixed in the backend, not counted).

Milestone 3 adds the unistore replay to the gate script and works the triage list, each fix ported from the Go source it names, with a regression test at the tier where the bug lives.

Milestone 4 retires the in-process transaction model once the unistore replay is the gate's measure and at least as good per topic: `Session::new()` and the harness are backed by the embedded store, and the catalog-image transaction (`Transaction::working`/`read_snapshot`, `Catalog::rebase_transaction`, `with_transaction_writes`, `absorb_writes`, `PessimisticLocks`, the statement write log) is deleted. Its prototype step is to back one session test module by the embedded store and count what breaks; the result decides whether retirement is one change or a sequence.


## Concrete Steps


All commands run from `rust/`.

Milestone 1, after the backend exists:

    INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_SHOW_OUT_OF_DOMAIN=1 INTEGRATION_TOPIC=executor/update cargo test -p tidb-server --test corpus_unistore -- --ignored --nocapture
    # prints one line per topic: "<matched> matched <diverged> diverged of <total> ... executor/update"

    INTEGRATION_TOPIC=executor/update cargo test -p difftest-result-tests --test integration_diff -- --ignored --nocapture replay_one_topic
    # the in-process line for the same topic, for comparison


## Validation and Acceptance


Milestone 1 is accepted when the unistore backend replays `executor/update`, `executor/executor` and `session/temporary_table` end to end and reports counts, and when `session/temporary_table`'s three `Error 1114 (HY000): The table 't' is full` statements -- which the in-process engine cannot produce -- match on the unistore backend or are listed with their cause.

Milestone 2 is accepted when the baseline file lists every topic with both engines' counts.

Milestone 4 is accepted when the files named in its paragraph no longer contain the catalog-image transaction and the whole-corpus unistore numbers are unchanged or better.


## Idempotence and Recovery


Every replay builds its store in memory and drops it with the test process; reruns are independent. The backend and its trait are additive until Milestone 4, so any milestone can stop with both engines working.


## Artifacts and Notes


Milestone 1, matched / diverged / hidden failures per topic (2026-10-11, HEAD 58e51433ef plus this milestone):

    topic                          in-process        unistore
    executor/update                481 / 0 / 0       478 / 0 / 3   (sequence DDL)
    executor/executor             1769 / 0 / 0      1692 / 31 / 48 (view, column and sequence DDL; BDR role; bindings; FOR UPDATE in UNION)
    session/temporary_table        268 / 3 / 0       261 / 3 / 7   (sequence DDL; the 3 are the 1114 size limit on both)
    executor/password_management   353 / 0 / 0       353 / 0 / 0


## Interfaces and Dependencies


In `rust/difftests/result-tests/src/connection.rs` (new), define the trait the replay drives:

    pub trait ReplayConnection {
        /// Runs one statement: rows with their column names and types, an
        /// affected count, or an error carrying the MySQL code and message.
        fn run(&mut self, sql: &str) -> Result<ReplayOutput, ReplayError>;
        /// The warnings the last statement left, as SHOW WARNINGS rows.
        fn warnings(&mut self) -> Vec<Vec<String>>;
    }

and a pool trait for `connect`/`connection`/`disconnect` that both backends implement. `rust/crates/tidb-server/Cargo.toml` gains `difftest-result-tests` as a dev-dependency.
