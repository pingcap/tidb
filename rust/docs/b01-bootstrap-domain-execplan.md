# B01 follow-up: the bootstrap runs in a temporary bootstrap domain, and the DDL owner manager owns its etcd client

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log` and `Outcomes & Retrospective` current as work proceeds. It follows `PLANS.md` at the repository root.


## Purpose / Big Picture

PR #65 (`dbsid:b01-bootstrap-owner`, commit 78107a5) made only the DDL owner bootstrap a fresh store, but its description lists two remaining differences from Go TiDB, and review asked for both to be fixed:

1. Go runs the bootstrap (and the upgrade) in a temporary bootstrap domain: `runInBootstrapSession` (`pkg/session/session.go`) creates a session, which creates a domain; it starts that domain with the start mode (`dom.Start(startMode)`), runs `bootstrap(s)` or `upgrade(s)`, calls `finishBootstrap`, then closes the domain (`dom.Close()`, `domap.Delete`). Only after that does `bootstrapSessionImpl` create the serving domain and start it in normal mode (`dom.Start(ddl.Normal)`). The Rust node (`rust/crates/tidb-server/src/cluster_stack.rs`, `cluster_session_stack`) instead published the bootstrap inside the store-connect closure, checked DDL ownership on the bare owner manager, and then ran the upgrade on the serving domain, which it started in the bootstrap or upgrade mode.
2. Go's `ownerManager.Start` (`pkg/ddl/owner_mgr.go`) opens its own etcd client (`storepkg.NewEtcdCli(store)`, keyspace-namespaced) and `Close` closes it. The Rust `tidb_ddl::owner_mgr::start_owner_manager` reused the node's etcd client.

After this change a TiKV node boots as Go's does: it starts the DDL owner manager over its own etcd client, initializes the DDL tables, and, when the store's bootstrap version is behind, builds a bootstrap domain started in the store's start mode, bootstraps or upgrades through that domain's DDL (whose owner is the process's owner manager), finishes the bootstrap, closes the bootstrap domain, and only then builds the serving domain, which starts in normal mode. Observable: two nodes booting one fresh cluster together both come up, one logs "bootstrap successful", the other logs "bootstrap not done yet, waiting for owner to finish" and then "upgrade successful in bootstrap"; the serving node's `admin show ddl` owner ID matches `information_schema.tidb_servers_info.ddl_id`.


## Terms

A domain (Go `domain.Domain`, Rust `tidb_domain_core::domain::Domain` plus the workers `cluster_session_stack` starts beside it) is the per-store runtime: the catalog reloader, the server-info registration in etcd, the schema-version syncer, the DDL (job submitter, owner campaign, job scheduler), and the session factory. The bootstrap domain is a domain built only to run the bootstrap or upgrade and closed afterwards. The start mode (`tidb_ddl::StartMode`) is Bootstrap for a store with no bootstrap version, Upgrade for one behind `CURRENT_BOOTSTRAP_VERSION`, Normal otherwise. `InitDDLTables` writes the DDL's own system tables (`mysql.tidb_ddl_job` and friends) straight into meta without a DDL job; the DDL scheduler needs them before it can run any job.


## Progress

- [x] Milestone 1: the owner manager opens and closes its own etcd client (`owner_mgr` tests pass).
- [x] Milestone 2: `InitDDLTables` (and the next-gen `bootstrapSchemas`) is its own committed step (`mysql_bootstrap::init_ddl_tables`, `bootstrap_schemas`, `bootstrap_publish::init_system_tables`); the resumed publish skips what it finds.
- [x] Milestone 3: `publish_bootstrap_on` runs the bootstrap DDL through a given domain's DDL; `bootstrap_on_domain_after_init_ddl_tables_matches_one_step_publish` passes.
- [x] Milestone 4: `cluster_session_stack` builds both domains through `new_cluster_domain`; `cargo build --locked -p tidb-server` passes.
- [x] Milestone 5a: two-node fresh boot on an isolated playground (2026-11-18): both nodes ran `InitDDLTables` (the loser retried its write conflict); one logged "bootstrap successful", the other "bootstrap not done yet, waiting for owner to finish" then "upgrade successful in bootstrap"; both logged "bootstrap cluster finished" with `bootMode=bootstrap`. On both nodes `tidb_servers_info.ddl_id` matched `admin show ddl`'s owner; CREATE TABLE, INSERT, ADD INDEX and `admin check table` ran; a restarted node logged "cluster already bootstrapped"; after SIGTERM to the owner the other node owned DDL 0.08s after the owner exited.
- [x] Milestone 5b: realtikv harness, `realtikv_addindextest4`, `realtikv_addindextest2` and `realtikv_brietest`: 20/20 pass.
- [x] Milestone 5c: `tidb-ddl` lib 92/92 (with `NO_PROXY`; without it 11 `tiflash_replica_manager` HTTP tests fail through the proxy); `tidb-session` bootstrap, upgrade, owner, DDL session pool and `mysql_bootstrap_classic_source` tests 109/109. The whole `tidb-session` suite (4406 tests) was stopped after about 500: its two `statistics::load_stats_*` failures also fail without this change (missing fixture file), and two `unistore_cop` failures under full load pass alone. Clippy reports nothing on the changed lines; `cargo build --locked -p tidb-server` passes. Committed as a follow-up to 78107a5 and pushed to `dbsid b01-bootstrap-owner`.


## Surprises & Discoveries

- 2026-11-18: the first two-node run reached another playground (`d02gsrtk`, PD 127.0.0.1:33379): the script derived the PD port from `--port-offset`, but tiup picked other ports for the new playground and another cluster already listened on the derived port. The nodes found that cluster bootstrapped (version 263) and booted normally, and the script's `CREATE DATABASE b01`, `CREATE TABLE b01.t`, inserts and `ALTER TABLE ... ADD INDEX` ran there before the nodes were killed. A later run must read the PD endpoint from the playground's own output and refuse a port it did not start.
- This machine's system HTTP proxy (127.0.0.1:7897) carries the nodes' PD HTTP calls and answers 502, which stalls ADD INDEX's DXF planning. Live runs export `NO_PROXY=127.0.0.1,localhost`.
- The Rust bootstrap already ran its DDL jobs in a private temporary domain (`NodeDdlSessionPool::for_bootstrap`, `rust/crates/tidb-session/src/ddl_session_pool.rs`), but that domain had a local mock owner and no etcd, was built only by the node that had won ownership, and was built after `InitDDLTables` had been committed inside the publish. A bootstrap domain built by every node before the ownership check needs the DDL tables first, because `InitDDLTables` publishes no schema version and the catalog reloader would never pick them up incrementally.


## Decision Log

- Decision: the unistore node and the tests keep `publish_bootstrap` / `publish_bootstrap_with_splits` (private bootstrap pool, `InitDDLTables` in the publish). Only the TiKV node uses the new path. Rationale: the unistore node's store is always fresh and has no etcd or owner election, and its byte-level fixtures pin the one-step publish.
- Decision: the serving-domain builder is the bootstrap-domain builder. Everything `cluster_session_stack` started before the upgrade step (catalog reloader, server info, server ID, schema-version syncer, DDL, infosync managers, session factory and its services) is the builder; everything after it (sysvar and privilege loops, bindings, statistics loops, telemetry, TTL and the rest of Go `BootstrapSession`) runs on the serving domain only. Rationale: this is the cut Go makes between `Domain.Init`/`Start` and `bootstrapSessionImpl`'s later steps.


## Outcomes & Retrospective

Both review differences are gone for the TiKV node. The unistore node still publishes its bootstrap in one step through the private `for_bootstrap` domain (Decision Log). The next-gen cluster path now also writes `NextGenBootTableVersion`, which Go's `bootstrapSchemas` writes and the one-step publish never did.


## Plan of Work

Milestone 1 changes `rust/crates/tidb-ddl/src/owner_mgr.rs`: `start_owner_manager` takes a constructor for the etcd client and calls it only when no manager is registered for the keyspace (Go calls `NewEtcdCli` only when not started). The manager keeps the client and `close_owner_manager` closes the owner manager and then the client. `cluster_stack.rs` passes a constructor that connects a new client to the PD endpoints with the cluster security and sets the keyspace namespace.

Milestone 2 adds `tidb_session::bootstrap_publish::init_ddl_tables(opener, timeout)`: one transaction with retry that writes what `jobs::plan_with_executor` writes before its first job (the `mysql` database, the next-gen tables, the DDL tables and the DDL table version), skipping what is present, and returns the region splits it owes. `plan_with_executor` in resume mode skips whatever is already present and finds the tables `doDMLWorks` seeds in the snapshot.

Milestone 3 adds `publish_bootstrap_on(domain, opener, timeout)`, which runs the same statement sequence but executes each statement through `domain.ddl`.

Milestone 4 restructures `cluster_session_stack` into: store and process services; `start_owner_manager`; the authority (its connect closure no longer bootstraps); `init_ddl_tables`; `run_in_bootstrap_session` when the bootstrap version is behind (bootstrap domain from the builder, `bootstrap`/`upgrade`/`InitMDLVariable`, `finish_bootstrap`, close, release the upgrade lock), else "cluster already bootstrapped" and `InitMDLVariable`; the system time zone; the serving domain from the builder in normal mode; the remaining `BootstrapSession` steps.


## Validation and Acceptance

    cd rust
    CARGO_INCREMENTAL=0 cargo check -p tidb-ddl -p tidb-session -p tidb-server --tests
    CARGO_INCREMENTAL=0 cargo nextest run -p tidb-ddl -E 'test(owner_mgr)'
    CARGO_INCREMENTAL=0 cargo nextest run -p tidb-session -E 'test(bootstrap)'
    cargo build --locked -p tidb-server

Then, on a fresh `tiup playground` with tikv-slim and no TiDB, start two `tidb-server` nodes together and check the log lines and owner IDs named in Purpose, then restart one node to check the normal-mode boot.


## Idempotence and Recovery

Every step is a code change on branch `dbsid/b01-bootstrap-owner`, committed on top of 78107a5 as a follow-up commit. Playgrounds are started with a unique tag and removed afterwards (`tiup clean <tag>`).
