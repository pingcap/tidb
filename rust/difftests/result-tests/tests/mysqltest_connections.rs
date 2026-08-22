// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The several sessions a mysqltest script drives, and the server state they
//! share.
//!
//! 37 of the suite's topics are multi-connection scripts, and they exist
//! precisely to test what one session sees of another: isolation, DDL
//! visibility, privileges granted on one connection and exercised on the
//! next. So the pool is not a `Vec<Session>` -- what makes it faithful is
//! WHICH state is shared and which is per-session:
//!
//! | state | shared | why |
//! | --- | --- | --- |
//! | [`SharedCatalog`] | yes | the store: a peer reads committed writes |
//! | [`GlobalSysvars`] | yes | `SET GLOBAL` on one connection, read at the next connect |
//! | [`PrivilegeRegistry`] | yes | `CREATE USER`/`GRANT` here, login there |
//! | open transaction | NO | `tidb_session::Transaction` stages writes in a private catalog copy, so a peer sees nothing until commit |
//! | session variables, user variables, identity | NO | per-connection by definition |
//!
//! Every one of those handles already exists for the server front end, and
//! this mirrors what [`tidb_server`'s `open_session`] does per accepted
//! connection: set the identity, hand over the shared registries, seed the
//! globals once. Nothing here is a test-only shortcut around the engine.
//!
//! # The topic database
//!
//! mysql-tester runs each `t/<dir>/<name>.test` against a database named
//! `<dir>__<name>` -- the scripts prove it themselves (`executor/admin.test`
//! contains `use executor__admin;`, and `connect` lines pass names like
//! `session__privileges` as the connect database). The harness creates it;
//! this pool creates it the same way so those statements resolve instead of
//! being skipped as an unknown schema.
//!
//! # Refusal, not approximation
//!
//! A `connect` naming an account the engine cannot authenticate is a REFUSAL
//! of the whole topic, not a fallback to the default session: continuing on a
//! root-privileged session where the script asked for an unprivileged one
//! would silently attribute every following statement's outcome to the wrong
//! identity, which is the exact failure the script reader already refuses to
//! commit.
//!
//! Four of the 37 refuse for that reason, and each names a real gap rather
//! than a driver limit:
//!
//!  * `privilege/privileges` -- `CREATE USER tcd1, tcd2;` creates NO account
//!    here, so the `connect (tcd2, ...)` that follows finds no row. The
//!    several-accounts-in-one-statement form of `CREATE USER` is the gap.
//!  * `executor/simple` -- `CREATE USER testuser1 ATTRIBUTE '{"name": ...}'`
//!    is refused, so `testuser1` never exists. The `ATTRIBUTE` clause is the
//!    gap.
//!  * `statistics/lock_table_stats` -- `connect (conn1, localhost, myuser,,
//!    mysql)` cannot select a schema named `mysql`: this tier answers
//!    `mysql.user` and friends without a catalog DATABASE object for them, so
//!    `USE mysql` is `UnknownDatabase`. The same absence is why the onboarded
//!    `executor/admin`'s own `use mysql;` is skipped as out of domain.
//!  * `executor/cluster_table` and the rest replay; the remaining three
//!    `UNALIGNED` topics among the 37 (`planner/core/integration`,
//!    `planner/core/integration_partition`, `planner/core/plan_cache`) fail on
//!    the READER's pre-existing limits -- a `.result` that is not UTF-8 and an
//!    echo sequence that does not line up -- not on connections at all.
#![allow(dead_code)]

use std::collections::BTreeMap;

use tidb_session::{privilege::PrivilegeRegistry, GlobalSysvars, Session, SharedCatalog};

/// The connection every script starts on, and the name `connection default`
/// switches back to.
const DEFAULT_CONNECTION: &str = "default";

/// The host of the account row `root`'s connections match, which is the host
/// [`PrivilegeRegistry::default`] bootstraps `root` for.
const ANY_HOST: &str = "%";

/// The sessions one topic's replay drives, and the server state behind them.
pub struct Connections {
    sessions: BTreeMap<String, Session>,
    current: String,
    catalog: SharedCatalog,
    globals: GlobalSysvars,
    privileges: PrivilegeRegistry,
    /// Go's per-connection `ConnectionID`, which `CONNECTION_ID()` reports and
    /// which must differ between two live connections.
    next_connection_id: u64,
    /// Go's two package-level push-down blacklists, shared by every session
    /// this pool opens because they are one server's.
    pushdown_blacklists: tidb_session::blacklist::PushdownBlacklists,
}

impl Connections {
    /// Opens the default connection for `topic`: `root` on the topic
    /// database, which is the connection mysql-tester starts every script on.
    pub fn open(topic: &str) -> Result<Self, String> {
        let catalog = SharedCatalog::default();
        let mut pool = Connections {
            sessions: BTreeMap::new(),
            current: DEFAULT_CONNECTION.to_owned(),
            catalog,
            globals: GlobalSysvars::new(),
            privileges: PrivilegeRegistry::default(),
            next_connection_id: 1,
            pushdown_blacklists: tidb_session::blacklist::PushdownBlacklists::default(),
        };
        let database = topic_database(topic);
        let mut session = pool.new_session("root", ANY_HOST, None)?;
        // The catalog above was created here, so this IS a fresh store and
        // the `mysql.*` system tables have to be created the way Go's
        // `bootstrap()` creates them for one. A recording's own statements
        // read and write them -- `black_list` inserts into
        // `mysql.expr_pushdown_blacklist` and reloads it -- so a replay over
        // an unbootstrapped catalog answers 1146 where TiDB answers rows.
        session.bootstrap_fresh_store();
        session
            .run(&format!("create database if not exists `{database}`"))
            .map_err(|e| format!("create topic database `{database}`: {e:?}"))?;
        session
            .select_database(&database)
            .map_err(|e| format!("use topic database `{database}`: {e:?}"))?;
        pool.sessions.insert(DEFAULT_CONNECTION.to_owned(), session);
        Ok(pool)
    }

    /// The session statements currently run on.
    pub fn current(&mut self) -> &mut Session {
        let current = self.current.clone();
        self.sessions
            .get_mut(&current)
            .expect("the current connection is open: `apply` never leaves a dangling name")
    }

    /// Applies one connection command, or refuses the topic.
    pub fn apply(&mut self, cmd: &crate::mysqltest_script::ConnectionCmd) -> Result<(), String> {
        use crate::mysqltest_script::ConnectionCmd;
        match cmd {
            ConnectionCmd::Open {
                name,
                host,
                user,
                db,
            } => {
                // The login has to find an account ROW, exactly as Go's
                // `ConnectionVerification` does, and the row it matches is
                // what `CURRENT_USER()` reports. An account the replay never
                // created is an engine gap (a `CREATE USER` that did not
                // take), and the topic is refused rather than run as root.
                let Some(matched_host) = self.match_account(user, host) else {
                    return Err(format!(
                        "cannot authenticate `{user}`@`{host}` for `connect ({name}, ...)`: no \
                         such account in the registry after the replay's own account statements"
                    ));
                };
                let session =
                    self.new_session(user, &matched_host, (!db.is_empty()).then_some(db.as_str()))?;
                // A `connect` MAKES THE NEW CONNECTION CURRENT: the statement
                // after `connect (conn1, localhost, u_version29,, ...)` in
                // `session/privileges` records `u_version29@%` from
                // `current_user()`.
                self.sessions.insert(name.clone(), session);
                self.current = name.clone();
                Ok(())
            }
            ConnectionCmd::Switch(name) => {
                if !self.sessions.contains_key(name) {
                    return Err(format!("`connection {name}` names no open connection"));
                }
                self.current = name.clone();
                Ok(())
            }
            ConnectionCmd::Close(name) => {
                if self.sessions.remove(name).is_none() {
                    return Err(format!("`disconnect {name}` names no open connection"));
                }
                // Closing the current connection falls back to the default
                // one: `session/privileges` runs a root-only
                // `drop database` immediately after `disconnect conn1` with no
                // `connection default` between them, and the recording shows
                // it succeeding.
                if self.current == *name {
                    self.current = DEFAULT_CONNECTION.to_owned();
                }
                Ok(())
            }
        }
    }

    /// The host of the account row a login by `user` from `host` matches, if
    /// there is one.
    ///
    /// Go's `MatchIdentity` picks the most specific row for the connecting
    /// host; the suite only ever creates two kinds -- the connecting host
    /// itself (`CREATE USER myuser@localhost`) and the wildcard
    /// (`CREATE USER 'u'@'%'`) -- so the exact row wins and the wildcard is
    /// the fallback. `127.0.0.1` and `localhost` are treated as the same host
    /// because Go's own loopback normalisation does.
    fn match_account(&self, user: &str, host: &str) -> Option<String> {
        let host = if host == "127.0.0.1" {
            "localhost"
        } else {
            host
        };
        [host, ANY_HOST]
            .into_iter()
            .find(|candidate| self.privileges.user_exists(user, candidate))
            .map(str::to_owned)
    }

    /// One new connection over the shared state, in the order the server's own
    /// `open_session` installs it.
    fn new_session(
        &mut self,
        user: &str,
        host: &str,
        initial_database: Option<&str>,
    ) -> Result<Session, String> {
        let mut session = Session::with_catalog(SharedCatalog::clone(&self.catalog));
        // Go's push-down blacklists are package-level, so `ADMIN RELOAD` on
        // one connection changes what every other connection plans. Every
        // session in this pool is the same server, so they share one handle.
        session.attach_pushdown_blacklists(self.pushdown_blacklists.clone());
        let identity = format!("{user}@{host}");
        session.set_user(identity.clone(), identity);
        session.set_connection_id(self.next_connection_id);
        self.next_connection_id += 1;
        session
            .attach_globals(self.globals.clone())
            .map_err(|error| error.to_string())?;
        // A mysqltest `connect (..., db)` puts `db` in the initial handshake;
        // it is not a SQL `USE db`. The recording proves the distinction in
        // `ddl/sequence`: an account with no visible privilege on the schema
        // still authenticates with it as the current database, and the first
        // table/sequence operation is where 1142 is raised. Select it before
        // installing the privilege registry so the ordinary SQL `USE` path
        // keeps its 1044 visibility gate.
        session.deselect_database();
        if let Some(database) = initial_database {
            session
                .select_database(database)
                .map_err(|error| format!("initial database `{database}`: {error:?}"))?;
        }
        session.attach_privileges(self.privileges.clone());
        session
            .run("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
            .expect("mysql-tester's driver handshake collation is accepted");
        // mysql-tester puts these in the DSN of EVERY connection it opens, so
        // the driver issues them before the script's first statement. They are
        // in the recorded output's provenance, not the engine's defaults:
        // `r/sessionctx/setvar.result` records `select @@tidb_max_chunk_size`
        // as 32 and `@@tidb_init_chunk_size` as 1, while the registry defaults
        // (and a `gorun` session, which is not driven by mysql-tester) answer
        // 1024 and 32.
        //
        // The list is the tester binary's own, read out of
        // `tests/integrationtest/mysql_tester` verbatim:
        //
        // ```sh
        // strings tests/integrationtest/mysql_tester \
        //   | grep -oE "tidb_[a-z_]+=('?[A-Za-z0-9_]+'?)"
        // ```
        //
        // `tidb_hash_join_concurrency=1` is the load-bearing one for PLAN
        // text, and it was the missing entry: `getPlanCostVer24PhysicalHashJoin`
        // divides the probe filter and probe hash by `p.Concurrency`, so at 1
        // a hash join costs what five workers would have shared. Measured on
        // `planner/core/join_reorder_through_projection`'s three-join shape
        // against a `tidb-server` built from this tree, the SAME statement
        // costs `3943972.48` at concurrency 1 and `2969908.48` at 5 -- and at
        // 5 the join even swaps which side it builds. Seven of that topic's
        // eight recorded `IndexHashJoin`/`IndexJoin` plans are unreachable at
        // concurrency 5: replaying the whole file through a HEAD-built server
        // reproduces 87 of its 94 plans without this variable and 94 of 94
        // with it (the four residual diffs are the harness's own
        // `ScalarQueryCol#<id>` regex replacement). See
        // `tidb_executor::driver::index_join_decision::CHOOSER_IS_FAITHFUL`,
        // whose cost seam this is the session half of.
        for setup in [
            "SET @@tidb_init_chunk_size=1",
            "SET @@tidb_max_chunk_size=32",
            "SET @@tidb_hash_join_concurrency=1",
            "SET @@tidb_enable_analyze_snapshot=1",
            "SET @@tidb_enable_pseudo_for_outdated_stats=false",
            "SET @@tidb_multi_statement_mode=1",
            "SET @@tidb_enable_clustered_index='int_only'",
        ] {
            session
                .run(setup)
                .expect("mysql-tester's per-connection setup is accepted");
        }
        Ok(session)
    }

    /// Preserves the account-row side effect of an unsupported account
    /// annotation so a later mysqltest `connect` can authenticate it.
    ///
    /// The statement itself remains an OutOfDomain skip and its annotation is
    /// not fabricated. Only the parsed `CREATE USER ... COMMENT/ATTRIBUTE`
    /// account identities are copied, and only after the caller established
    /// that TiDB expected the statement to succeed.
    pub fn recover_account_row_from_unsupported_create_user(&self, sql: &str) {
        let Ok(tidb_ast::Stmt::Ddl(ddl)) = tidb_parser::parse(sql) else {
            return;
        };
        let tidb_ast::DdlStmt::CreateUser {
            users,
            comment_or_attribute: Some(_),
            ..
        } = ddl.as_ref()
        else {
            return;
        };
        for spec in users {
            if !spec.user.current_user {
                self.privileges
                    .create_user(&spec.user.user, &spec.user.host, "");
            }
        }
    }
}

/// The database mysql-tester runs `t/<path>.test` against: the topic path with
/// each separator replaced by `__`.
pub fn topic_database(topic: &str) -> String {
    topic.replace(['/', '\\'], "__")
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::mysqltest_script::ConnectionCmd;

    #[test]
    fn topic_path_becomes_the_database_the_scripts_name() {
        assert_eq!(topic_database("executor/admin"), "executor__admin");
        assert_eq!(topic_database("subquery"), "subquery");
    }

    fn open_root(name: &str) -> ConnectionCmd {
        ConnectionCmd::Open {
            name: name.to_owned(),
            host: "localhost".to_owned(),
            user: "root".to_owned(),
            db: topic_database("driver/isolation"),
        }
    }

    fn one_cell(pool: &mut Connections, sql: &str) -> String {
        match pool.current().run_with_columns(sql).unwrap() {
            tidb_session::StmtOutput::Rows { rows, .. } => format!("{rows:?}"),
            other => panic!("{sql} answered {other:?}"),
        }
    }

    /// Every connection the replay opens carries mysql-tester's DSN session
    /// variables, on the DEFAULT connection and on a `connect`ed peer alike.
    ///
    /// `tidb_hash_join_concurrency` is the one that decides recorded PLAN
    /// text: `getPlanCostVer24PhysicalHashJoin` divides the probe filter and
    /// probe hash by it, so a replay that let it resolve to the plain-session
    /// `5` would be comparing against plans no recording contains. Seven of
    /// `planner/core/join_reorder_through_projection`'s eight recorded index
    /// joins are unreachable at `5`, measured against a `tidb-server` built
    /// from this tree.
    #[test]
    fn every_connection_carries_the_mysql_tester_session_variables() {
        let expected = [
            ("tidb_init_chunk_size", "[[Int(1)]]"),
            ("tidb_max_chunk_size", "[[Int(32)]]"),
            (
                "tidb_hash_join_concurrency",
                "[[String(StringDatum { bytes: [49], collation: Utf8Mb4GeneralCi })]]",
            ),
            ("tidb_enable_analyze_snapshot", "[[Int(1)]]"),
            ("tidb_enable_pseudo_for_outdated_stats", "[[Int(0)]]"),
            (
                "tidb_multi_statement_mode",
                "[[String(StringDatum { bytes: [79, 78], collation: Utf8Mb4GeneralCi })]]",
            ),
            (
                "tidb_enable_clustered_index",
                "[[String(StringDatum { bytes: [73, 78, 84, 95, 79, 78, 76, 89], collation: \
                 Utf8Mb4GeneralCi })]]",
            ),
        ];
        let mut pool = Connections::open("driver/isolation").unwrap();
        for connection in ["default", "conn1"] {
            if connection != "default" {
                pool.apply(&open_root(connection)).unwrap();
            }
            for (name, value) in expected {
                assert_eq!(
                    one_cell(&mut pool, &format!("select @@{name}")),
                    value,
                    "connection {connection} lost mysql-tester's {name}"
                );
            }
        }
    }

    #[test]
    fn every_connection_uses_mysql_tester_handshake_collation() {
        let mut pool = Connections::open("driver/isolation").unwrap();
        for connection in ["default", "conn1"] {
            if connection != "default" {
                pool.apply(&open_root(connection)).unwrap();
            }
            assert_eq!(
                pool.current()
                    .vars()
                    .get_system("character_set_connection")
                    .unwrap(),
                "utf8mb4"
            );
            assert_eq!(
                pool.current()
                    .vars()
                    .get_system("collation_connection")
                    .unwrap(),
                "utf8mb4_general_ci"
            );
        }
    }

    /// The property that makes these topics worth replaying at all: a peer sees
    /// a committed write and NOT an uncommitted one, over ONE store.
    #[test]
    fn a_peer_sees_committed_writes_and_not_uncommitted_ones() {
        let mut pool = Connections::open("driver/isolation").unwrap();
        pool.current().run("create table t (a int)").unwrap();
        pool.apply(&open_root("conn1")).unwrap();
        pool.current().run("begin").unwrap();
        pool.current().run("insert into t values (1)").unwrap();
        // conn1 reads its own write ...
        assert_eq!(one_cell(&mut pool, "select a from t"), "[[Int(1)]]");
        // ... and the default connection sees nothing of it.
        pool.apply(&ConnectionCmd::Switch("default".to_owned()))
            .unwrap();
        assert_eq!(one_cell(&mut pool, "select a from t"), "[]");

        pool.apply(&ConnectionCmd::Switch("conn1".to_owned()))
            .unwrap();
        pool.current().run("commit").unwrap();
        // `disconnect` falls back to the default connection, which now sees it.
        pool.apply(&ConnectionCmd::Close("conn1".to_owned()))
            .unwrap();
        assert_eq!(one_cell(&mut pool, "select a from t"), "[[Int(1)]]");
    }

    /// An account created on one connection is the account the next connection
    /// logs in as -- and one that was never created refuses the topic instead
    /// of quietly becoming root.
    #[test]
    fn a_connect_matches_the_account_row_the_replay_created() {
        let mut pool = Connections::open("driver/accounts").unwrap();
        assert!(pool
            .apply(&ConnectionCmd::Open {
                name: "conn1".to_owned(),
                host: "localhost".to_owned(),
                user: "nobody".to_owned(),
                db: String::new(),
            })
            .is_err());
        pool.current().run("create user u1@localhost").unwrap();
        pool.apply(&ConnectionCmd::Open {
            name: "conn1".to_owned(),
            host: "localhost".to_owned(),
            user: "u1".to_owned(),
            db: String::new(),
        })
        .unwrap();
        assert_eq!(
            pool.current().authenticated_identity(),
            Some(("u1", "localhost"))
        );
    }

    #[test]
    fn initial_database_is_selected_before_sql_use_privilege_checks() {
        let mut pool = Connections::open("driver/accounts").unwrap();
        pool.current()
            .run("create user no_db_privilege@localhost")
            .unwrap();
        pool.apply(&ConnectionCmd::Open {
            name: "conn1".to_owned(),
            host: "localhost".to_owned(),
            user: "no_db_privilege".to_owned(),
            db: topic_database("driver/accounts"),
        })
        .unwrap();
        assert_eq!(
            pool.current().current_database(),
            topic_database("driver/accounts")
        );
    }

    #[test]
    fn unsupported_account_annotations_still_leave_the_recorded_account_row() {
        let mut pool = Connections::open("driver/accounts").unwrap();
        pool.recover_account_row_from_unsupported_create_user(
            "CREATE USER testuser1 ATTRIBUTE '{\"name\": \"Tom\"}';",
        );
        pool.apply(&ConnectionCmd::Open {
            name: "conn1".to_owned(),
            host: "localhost".to_owned(),
            user: "testuser1".to_owned(),
            db: String::new(),
        })
        .unwrap();
        assert_eq!(
            pool.current().authenticated_identity(),
            Some(("testuser1", "%"))
        );
    }
}
