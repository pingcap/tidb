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
        };
        let database = topic_database(topic);
        let mut session = pool.new_session("root", ANY_HOST);
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
                let mut session = self.new_session(user, &matched_host);
                if db.is_empty() {
                    // An omitted db means NO schema, not the topic's: the
                    // statement after `connect (conn1, localhost, root,,)` in
                    // `executor/show` is a `show tables` whose recording is
                    // `Error 1046 (3D000): No database selected`.
                    session.deselect_database();
                } else {
                    session
                        .select_database(db)
                        .map_err(|e| format!("connect ({name}, ...) database `{db}`: {e:?}"))?;
                }
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
    fn new_session(&mut self, user: &str, host: &str) -> Session {
        let mut session = Session::with_catalog(SharedCatalog::clone(&self.catalog));
        let identity = format!("{user}@{host}");
        session.set_user(identity.clone(), identity);
        session.set_connection_id(self.next_connection_id);
        self.next_connection_id += 1;
        session.attach_privileges(self.privileges.clone());
        session.attach_globals(self.globals.clone());
        // mysql-tester issues these two on EVERY connection it opens, before
        // the script's first statement, to make the executor cross a chunk
        // boundary on small tables. They are in the recorded output's
        // provenance, not the engine's defaults: `r/sessionctx/setvar.result`
        // records `select @@tidb_max_chunk_size` as 32 and
        // `@@tidb_init_chunk_size` as 1, while the registry defaults (and a
        // `gorun` session, which is not driven by mysql-tester) answer 1024
        // and 32. The strings are the tester binary's own, verbatim:
        // `SET @@tidb_init_chunk_size=1` and `SET @@tidb_max_chunk_size=32`.
        for setup in [
            "SET @@tidb_init_chunk_size=1",
            "SET @@tidb_max_chunk_size=32",
        ] {
            session
                .run(setup)
                .expect("mysql-tester's per-connection setup is accepted");
        }
        session
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
}
