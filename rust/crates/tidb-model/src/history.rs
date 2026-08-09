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

//! Go `HistoryInfo` from `pkg/meta/model/job.go`.

use crate::db::DBInfo;
use crate::go_runtime::{GoShared, GoSharedPointerSlice};
use crate::serde_helpers::GoJsonMerge;
use crate::table_info::TableInfo;

/// The schema snapshot recorded when a DDL job finishes.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct HistoryInfo {
    /// The schema version after the job.
    #[serde(rename = "SchemaVersion", default)]
    pub schema_version: i64,
    /// The affected database, if any.
    #[serde(rename = "DBInfo", default)]
    pub db_info: Option<GoShared<DBInfo>>,
    /// The affected table, if any.
    #[serde(rename = "TableInfo", default)]
    pub table_info: Option<GoShared<TableInfo>>,
    /// The finish timestamp (a TSO).
    #[serde(rename = "FinishedTS", default)]
    pub finished_ts: u64,
    /// Multiple affected tables (for multi-table jobs).
    #[serde(rename = "MultipleTableInfos", default)]
    pub multiple_table_infos: GoSharedPointerSlice<TableInfo>,
}

impl HistoryInfo {
    /// Decodes Go JSON into the existing receiver, retaining History's shared
    /// pointer/backing aliases and field updates applied before an error.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        let raw: &serde_json::value::RawValue = serde_json::from_slice(bytes)?;
        if raw.get() == "null" {
            return Ok(());
        }
        let mut deserializer = serde_json::Deserializer::from_str(raw.get());
        self.go_json_merge(&mut deserializer)
            .map_err(crate::serde_helpers::normalize_fatal_json_error)?;
        deserializer.end()
    }

    /// Go `HistoryInfo.AddDBInfo`.
    pub fn add_db_info(&mut self, schema_version: i64, db_info: Option<GoShared<DBInfo>>) {
        self.schema_version = schema_version;
        self.db_info = db_info;
    }

    /// Go `HistoryInfo.AddTableInfo`.
    pub fn add_table_info(&mut self, schema_version: i64, table_info: Option<GoShared<TableInfo>>) {
        self.schema_version = schema_version;
        self.table_info = table_info;
    }

    /// Go `HistoryInfo.SetTableInfos`.
    pub fn set_table_infos(
        &mut self,
        schema_version: i64,
        table_infos: &GoSharedPointerSlice<TableInfo>,
    ) {
        self.schema_version = schema_version;
        self.multiple_table_infos = table_infos.copy_outer();
    }

    /// Go `HistoryInfo.Clean`. `finished_ts` deliberately survives, as it does
    /// in Go.
    pub fn clean(&mut self) {
        self.schema_version = 0;
        self.db_info = None;
        self.table_info = None;
        self.multiple_table_infos = GoSharedPointerSlice::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use crate::job_enums::JobState;
    use crate::schema_state::SchemaState;

    fn table(id: i64) -> GoShared<TableInfo> {
        GoShared::new(TableInfo {
            id,
            ..Default::default()
        })
    }

    #[test]
    fn pointer_slice_decode_reuses_initialized_slots_hidden_by_len() {
        let first = table(1);
        let second = table(2);
        let third = table(3);
        let mut history = HistoryInfo {
            multiple_table_infos: GoSharedPointerSlice::from_handles(vec![
                Some(first.clone()),
                Some(second.clone()),
                Some(third.clone()),
            ]),
            ..Default::default()
        };
        let sibling = history.multiple_table_infos.clone();

        history
            .decode(br#"{"MultipleTableInfos":[{"id":10}]}"#)
            .unwrap();
        assert_eq!(history.multiple_table_infos.len(), 1);
        assert_eq!(history.multiple_table_infos.capacity(), 3);
        assert!(history.multiple_table_infos.backing_ptr_eq(&sibling));
        assert!(history.multiple_table_infos.get(0).unwrap().ptr_eq(&first));

        history
            .decode(br#"{"MultipleTableInfos":[{"id":11},{"id":12},{"id":13}]}"#)
            .unwrap();
        assert!(history.multiple_table_infos.get(1).unwrap().ptr_eq(&second));
        assert!(history.multiple_table_infos.get(2).unwrap().ptr_eq(&third));
        assert_eq!(sibling.get(0).unwrap().read().id, 11);
        assert_eq!(sibling.get(1).unwrap().read().id, 12);
        assert_eq!(sibling.get(2).unwrap().read().id, 13);
    }

    #[test]
    fn pointer_slice_fatal_error_preserves_old_len_and_untouched_tail() {
        let first = table(1);
        let second = table(2);
        let third = table(3);
        let original = GoSharedPointerSlice::from_handles(vec![
            Some(first.clone()),
            Some(second.clone()),
            Some(third.clone()),
        ]);

        let mut fatal_first = HistoryInfo {
            multiple_table_infos: original.clone(),
            ..Default::default()
        };
        assert!(fatal_first
            .decode(br#"{"MultipleTableInfos":[{"name":{"L":"partial-first","O":1}}]}"#)
            .is_err());
        assert_eq!(fatal_first.multiple_table_infos.len(), 3);
        assert!(fatal_first.multiple_table_infos.backing_ptr_eq(&original));
        assert!(fatal_first
            .multiple_table_infos
            .get(0)
            .unwrap()
            .ptr_eq(&first));
        assert_eq!(first.read().name.lowercase(), "partial-first");
        assert!(fatal_first
            .multiple_table_infos
            .get(1)
            .unwrap()
            .ptr_eq(&second));
        assert!(fatal_first
            .multiple_table_infos
            .get(2)
            .unwrap()
            .ptr_eq(&third));

        let mut fatal_middle = HistoryInfo {
            multiple_table_infos: original.clone(),
            ..Default::default()
        };
        assert!(fatal_middle
            .decode(br#"{"MultipleTableInfos":[null,{"name":{"L":"partial-middle","O":1}}]}"#,)
            .is_err());
        assert_eq!(fatal_middle.multiple_table_infos.len(), 3);
        assert!(original.get(0).is_none());
        assert!(fatal_middle
            .multiple_table_infos
            .get(1)
            .unwrap()
            .ptr_eq(&second));
        assert_eq!(second.read().name.lowercase(), "partial-middle");
        assert!(fatal_middle
            .multiple_table_infos
            .get(2)
            .unwrap()
            .ptr_eq(&third));
    }

    #[test]
    fn pointer_slice_growth_writes_old_backing_before_detaching() {
        let first = table(1);
        let second = table(2);
        let mut history = HistoryInfo {
            multiple_table_infos: GoSharedPointerSlice::from_handles(vec![
                Some(first),
                Some(second.clone()),
            ]),
            ..Default::default()
        };
        let sibling = history.multiple_table_infos.clone();
        history
            .decode(br#"{"MultipleTableInfos":[null,{"id":22},{"id":33}]}"#)
            .unwrap();
        assert!(!history.multiple_table_infos.backing_ptr_eq(&sibling));
        assert_eq!(history.multiple_table_infos.len(), 3);
        assert!(sibling.get(0).is_none());
        assert!(sibling.get(1).unwrap().ptr_eq(&second));
        assert_eq!(sibling.get(1).unwrap().read().id, 22);
        assert_eq!(history.multiple_table_infos.get(2).unwrap().read().id, 33);

        let first = table(4);
        let second = table(5);
        let mut fatal_after_growth = HistoryInfo {
            multiple_table_infos: GoSharedPointerSlice::from_handles(vec![
                Some(first),
                Some(second.clone()),
            ]),
            ..Default::default()
        };
        let old_backing = fatal_after_growth.multiple_table_infos.clone();
        assert!(fatal_after_growth
            .decode(
                br#"{"MultipleTableInfos":[null,{"id":24},{"name":{"L":"partial-new","O":1}}]}"#,
            )
            .is_err());
        assert!(!fatal_after_growth
            .multiple_table_infos
            .backing_ptr_eq(&old_backing));
        assert_eq!(fatal_after_growth.multiple_table_infos.len(), 3);
        assert!(old_backing.get(0).is_none());
        assert!(old_backing.get(1).unwrap().ptr_eq(&second));
        assert_eq!(old_backing.get(1).unwrap().read().id, 24);
        assert_eq!(
            fatal_after_growth
                .multiple_table_infos
                .get(2)
                .unwrap()
                .read()
                .name
                .lowercase(),
            "partial-new"
        );
    }

    #[test]
    fn finish_multiple_updates_job_state_before_nil_history_panic() {
        let mut job = Job::default();
        let tables = GoSharedPointerSlice::from_nullable(vec![Some(TableInfo::default())]);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            job.finish_multiple_table_job(JobState::DONE, SchemaState::PUBLIC, 9, &tables);
        }))
        .is_err());
        assert_eq!(job.state, JobState::DONE);
        assert_eq!(job.schema_state, SchemaState::PUBLIC);
        assert!(job.binlog_info.is_none());

        let stale = table(17);
        let mut nil_tables_job = Job {
            binlog_info: Some(GoShared::new(HistoryInfo {
                table_info: Some(stale.clone()),
                multiple_table_infos: GoSharedPointerSlice::from_nullable(vec![Some(
                    TableInfo::default(),
                )]),
                ..Default::default()
            })),
            ..Default::default()
        };
        let nil_tables = GoSharedPointerSlice::default();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            nil_tables_job.finish_multiple_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                18,
                &nil_tables,
            );
        }))
        .is_err());
        assert_eq!(nil_tables_job.state, JobState::DONE);
        assert_eq!(nil_tables_job.schema_state, SchemaState::PUBLIC);
        let history = nil_tables_job.binlog_info.as_ref().unwrap().read();
        assert_eq!(history.schema_version, 18);
        assert!(!history.multiple_table_infos.is_allocated());
        assert!(history.table_info.as_ref().unwrap().ptr_eq(&stale));
    }
}
