// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `mysql.tidb_ddl_notifier` adapter for the cluster session owner.

use std::any::Any;
use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_ddl_notifier::{
    ListResult, NotifierError, NotifierSession, SchemaChange, SessionPool, Store,
};

use super::{ClusterServerSession, ClusterStatsSessionContext};
use crate::sql_node::QuerySession;

fn notifier_error(error: impl std::fmt::Display) -> NotifierError {
    NotifierError::Message(error.to_string())
}

struct ClusterNotifierSession {
    session: Option<tidb_syssession::Session<ClusterStatsSessionContext>>,
}

impl ClusterNotifierSession {
    fn session(
        &self,
    ) -> Result<&tidb_syssession::Session<ClusterStatsSessionContext>, NotifierError> {
        self.session
            .as_ref()
            .ok_or_else(|| NotifierError::Message("notifier session already returned".to_owned()))
    }

    fn with_server<T>(
        &self,
        callback: impl FnOnce(&mut ClusterServerSession) -> Result<T, NotifierError>,
    ) -> Result<T, NotifierError> {
        self.session()?
            .with_session_context(|context| {
                context
                    .with_session(|session| {
                        callback(session)
                            .map_err(|error| super::stats_session_error(error.to_string()))
                    })
                    .map_err(|error| tidb_syssession::SysSessionError::new(error.to_string()))
            })
            .map_err(notifier_error)
    }

    fn write(&self, sql: &str) -> Result<u64, NotifierError> {
        self.with_server(|session| {
            session
                .execute_write(sql)
                .map(|outcome| outcome.map_or(0, |outcome| outcome.affected_rows))
                .map_err(|error| NotifierError::Message(error.message))
        })
    }

    fn query(&self, sql: &str) -> Result<Vec<Vec<Datum>>, NotifierError> {
        self.session()?
            .with_session_context(|context| {
                context
                    .state
                    .materialize(sql)
                    .map(|(rows, _)| rows)
                    .map_err(|error| tidb_syssession::SysSessionError::new(error.to_string()))
            })
            .map_err(notifier_error)
    }
}

impl NotifierSession for ClusterNotifierSession {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn begin_pessimistic(&mut self) -> Result<(), NotifierError> {
        self.write("BEGIN PESSIMISTIC").map(|_| ())
    }

    fn commit(&mut self) -> Result<(), NotifierError> {
        self.write("COMMIT").map(|_| ())
    }

    fn rollback(&mut self) {
        let _ = self.write("ROLLBACK");
    }
}

pub(super) struct ClusterNotifierSessionPool {
    pool: Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>>,
}

impl ClusterNotifierSessionPool {
    pub(super) fn new(
        pool: Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>>,
    ) -> Self {
        Self { pool }
    }
}

impl SessionPool for ClusterNotifierSessionPool {
    fn get(&self) -> Result<Box<dyn NotifierSession>, NotifierError> {
        self.pool
            .get()
            .map(|session| {
                Box::new(ClusterNotifierSession {
                    session: Some(session),
                }) as Box<dyn NotifierSession>
            })
            .map_err(notifier_error)
    }

    fn put(&self, mut session: Box<dyn NotifierSession>) {
        let Some(session) = session
            .as_any_mut()
            .downcast_mut::<ClusterNotifierSession>()
        else {
            return;
        };
        if let Some(session) = session.session.take() {
            self.pool.put(&session);
        }
    }
}

/// Go `OpenTableStore` over the bootstrapped notifier system table.
pub(super) struct ClusterNotifierTableStore;

fn cluster_session(
    session: &mut dyn NotifierSession,
) -> Result<&mut ClusterNotifierSession, NotifierError> {
    session
        .as_any_mut()
        .downcast_mut::<ClusterNotifierSession>()
        .ok_or_else(|| NotifierError::Message("wrong notifier session implementation".to_owned()))
}

fn bytes_literal(bytes: &[u8]) -> String {
    let mut literal = String::with_capacity(bytes.len() * 2 + 3);
    literal.push_str("X'");
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        literal.push(HEX[usize::from(byte >> 4)] as char);
        literal.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    literal.push('\'');
    literal
}

impl Store for ClusterNotifierTableStore {
    fn insert(
        &self,
        session: &mut dyn NotifierSession,
        change: &SchemaChange,
    ) -> Result<(), NotifierError> {
        let event = serde_json::to_vec(&change.event).map_err(notifier_error)?;
        cluster_session(session)?
            .write(&format!(
                "INSERT INTO mysql.tidb_ddl_notifier \
                 (ddl_job_id, sub_job_id, schema_change, processed_by_flag) \
                 VALUES ({}, {}, {}, 0)",
                change.ddl_job_id,
                change.sub_job_id,
                bytes_literal(&event)
            ))
            .map(|_| ())
    }

    fn update_processed(
        &self,
        session: &mut dyn NotifierSession,
        ddl_job_id: i64,
        sub_job_id: i64,
        old_processed_by: u64,
        new_processed_by: u64,
    ) -> Result<(), NotifierError> {
        let affected = cluster_session(session)?.write(&format!(
            "UPDATE mysql.tidb_ddl_notifier SET processed_by_flag = {new_processed_by} \
             WHERE ddl_job_id = {ddl_job_id} AND sub_job_id = {sub_job_id} \
             AND processed_by_flag = {old_processed_by}"
        ))?;
        if affected == 0 {
            return Err(NotifierError::Message(format!(
                "failed to update processed_by_flag, maybe the row has been updated by other owner. \
                 ddl_job_id: {ddl_job_id}, sub_job_id: {sub_job_id}"
            )));
        }
        Ok(())
    }

    fn delete_and_commit(
        &self,
        session: &mut dyn NotifierSession,
        ddl_job_id: i64,
        sub_job_id: i64,
    ) -> Result<(), NotifierError> {
        let session = cluster_session(session)?;
        session.write("BEGIN")?;
        let result = session.write(&format!(
            "DELETE FROM mysql.tidb_ddl_notifier \
             WHERE ddl_job_id = {ddl_job_id} AND sub_job_id = {sub_job_id}"
        ));
        match result {
            Ok(_) => session.write("COMMIT").map(|_| ()),
            Err(error) => {
                let _ = session.write("ROLLBACK");
                Err(error)
            }
        }
    }

    fn list(
        &self,
        _session: &mut dyn NotifierSession,
    ) -> Result<Box<dyn ListResult>, NotifierError> {
        Ok(Box::new(ClusterNotifierListResult {
            started: false,
            max_job_id: 0,
            max_sub_job_id: 0,
        }))
    }
}

struct ClusterNotifierListResult {
    started: bool,
    max_job_id: i64,
    max_sub_job_id: i64,
}

impl ListResult for ClusterNotifierListResult {
    fn read(
        &mut self,
        session: &mut dyn NotifierSession,
        capacity: usize,
    ) -> Result<Vec<SchemaChange>, NotifierError> {
        let session = cluster_session(session)?;
        if !self.started {
            session.write("BEGIN")?;
            self.started = true;
        }
        let rows = session.query(&format!(
            "SELECT ddl_job_id, sub_job_id, schema_change, processed_by_flag \
             FROM mysql.tidb_ddl_notifier \
             WHERE (ddl_job_id, sub_job_id) > ({}, {}) \
             ORDER BY ddl_job_id, sub_job_id LIMIT {capacity}",
            self.max_job_id, self.max_sub_job_id
        ))?;
        let mut changes = Vec::with_capacity(rows.len());
        for row in rows {
            let [Datum::Int(ddl_job_id), Datum::Int(sub_job_id), event, processed] = row.as_slice()
            else {
                return Err(NotifierError::Message(
                    "invalid mysql.tidb_ddl_notifier row shape".to_owned(),
                ));
            };
            let bytes = match event {
                Datum::Bytes(bytes) | Datum::Raw(bytes) => bytes.as_slice(),
                Datum::String(value) => value.bytes(),
                _ => {
                    return Err(NotifierError::Message(
                        "invalid notifier schema_change value".to_owned(),
                    ))
                }
            };
            let processed_by_flag = match processed {
                Datum::UInt(value) => *value,
                Datum::Int(value) => *value as u64,
                _ => {
                    return Err(NotifierError::Message(
                        "invalid notifier processed_by_flag value".to_owned(),
                    ))
                }
            };
            changes.push(SchemaChange {
                ddl_job_id: *ddl_job_id,
                sub_job_id: *sub_job_id,
                event: serde_json::from_slice(bytes).map_err(notifier_error)?,
                processed_by_flag,
            });
        }
        if let Some(last) = changes.last() {
            self.max_job_id = last.ddl_job_id;
            self.max_sub_job_id = last.sub_job_id;
        }
        Ok(changes)
    }

    fn close(self: Box<Self>, session: &mut dyn NotifierSession) {
        if let Ok(session) = cluster_session(session) {
            let _ = session.write("ROLLBACK");
        }
    }
}
