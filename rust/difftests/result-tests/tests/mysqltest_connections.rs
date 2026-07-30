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
#![allow(dead_code)]

use std::collections::BTreeMap;

use tidb_session::{privilege::PrivilegeRegistry, GlobalSysvars, Session, SharedCatalog};

/// The connection every script starts on, and the name `connection default`
/// switches back to.
const DEFAULT_CONNECTION: &str = "default";

/// The host every account in the suite is created for. The scripts write
/// `localhost` in the connect line and `CREATE USER 'u'@'%'` in the SQL, so
/// the row a login matches is the `%` one -- the host in the connect line
/// decides nothing here.
const ACCOUNT_HOST: &str = "%";

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
        let mut session = pool.new_session("root");
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
            ConnectionCmd::Open { name, user, db } => {
                // The login has to find a row, exactly as Go's
                // `ConnectionVerification` does. An account the replay never
                // created is an engine gap (a `CREATE USER` that did not
                // take), and the topic is refused rather than run as root.
                if !self.privileges.user_exists(user, ACCOUNT_HOST) {
                    return Err(format!(
                        "cannot authenticate `{user}`@`{ACCOUNT_HOST}` for `connect ({name}, ...)`: \
                         no such account in the registry after the replay's own account statements"
                    ));
                }
                let mut session = self.new_session(user);
                if !db.is_empty() {
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

    /// One new connection over the shared state, in the order the server's own
    /// `open_session` installs it.
    fn new_session(&mut self, user: &str) -> Session {
        let mut session = Session::with_catalog(SharedCatalog::clone(&self.catalog));
        let identity = format!("{user}@{ACCOUNT_HOST}");
        session.set_user(identity.clone(), identity);
        session.set_connection_id(self.next_connection_id);
        self.next_connection_id += 1;
        session.attach_privileges(self.privileges.clone());
        session.attach_globals(self.globals.clone());
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

    #[test]
    fn topic_path_becomes_the_database_the_scripts_name() {
        assert_eq!(topic_database("executor/admin"), "executor__admin");
        assert_eq!(topic_database("subquery"), "subquery");
    }
}
