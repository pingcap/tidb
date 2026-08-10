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

//! The prepared-cursor owner: typed materialization, streaming fetch, and
//! one idempotent cleanup path for memory, disk, source, reader, and action.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DiskError;
use tidb_chunk::row_container::RowContainer;
use tidb_chunk::row_container_reader::RowContainerReader;
use tidb_util::memory::{
    ActionWithPriority, ArcAction, DEF_CURSOR_FETCH_SPILL_PRIORITY, LABEL_FOR_CURSOR_FETCH,
};

use crate::connection_writers::{write_eof_or_ok, write_packet_to};
use crate::mysql_connection::MysqlConnectionError;
use crate::mysql_tls::ClientStream;
use crate::resultset_source::ResultSetSource;
use crate::sql_node::{CursorMaterializationAuthority, QueryResult, SqlQueryError};

pub(crate) struct CursorState {
    columns: Vec<tidb_protocol::ColumnInfo>,
    field_types: Vec<tidb_datatype::FieldType>,
    init_chunk_size: usize,
    max_chunk_size: usize,
    memory: tidb_executor::StatementMemory,
    rows: RowContainer,
    reader: Option<RowContainerReader>,
    registered_action: Option<ArcAction>,
    next_row: usize,
    total_rows: usize,
}

pub(crate) enum CursorFetchError {
    Protocol { message: String, sequence: u8 },
    Transport(MysqlConnectionError),
}

impl CursorState {
    /// Materializes a result with the exact statement authority captured by
    /// its producer. Both general prepared reads and optimized point reads go
    /// through this door, so neither route can silently fall back to an
    /// unrelated quota or spill directory.
    pub(crate) fn materialize_result(result: &mut QueryResult<'_>) -> Result<Self, SqlQueryError> {
        let Some(authority) = result.take_cursor_materialization() else {
            let close = result.source().close().map_err(SqlQueryError::unknown);
            return match close {
                Ok(()) => Err(SqlQueryError::unknown(
                    "prepared cursor result is missing its materialization authority",
                )),
                Err(error) => Err(error),
            };
        };
        Self::materialize(result, authority)
    }

    pub(crate) fn materialize(
        result: &mut QueryResult<'_>,
        authority: CursorMaterializationAuthority,
    ) -> Result<Self, SqlQueryError> {
        let CursorMaterializationAuthority {
            field_types,
            init_chunk_size,
            max_chunk_size,
            memory,
        } = authority;
        let mut rows =
            RowContainer::new(&field_types, max_chunk_size.max(1), memory.spill_storage());
        rows.mem_tracker().set_label(LABEL_FOR_CURSOR_FETCH);
        rows.mem_tracker().attach_to(memory.session_tracker());
        rows.disk_tracker().set_label(LABEL_FOR_CURSOR_FETCH);
        rows.disk_tracker().attach_to(memory.session_disk_tracker());

        let registered_action = if memory.tmp_storage_on_oom() {
            let spill: ArcAction = rows.action_spill();
            let wrapped: ArcAction = Arc::new(ActionWithPriority::new(
                spill,
                DEF_CURSOR_FETCH_SPILL_PRIORITY,
            ));
            memory
                .session_tracker()
                .fallback_old_and_set_new_action(Arc::clone(&wrapped));
            Some(wrapped)
        } else {
            None
        };

        let mut cursor = Self {
            columns: Vec::new(),
            field_types,
            init_chunk_size: init_chunk_size.max(1).min(max_chunk_size.max(1)),
            max_chunk_size: max_chunk_size.max(1),
            memory,
            rows,
            reader: None,
            registered_action,
            next_row: 0,
            total_rows: 0,
        };

        let materialized = cursor.materialize_source(result);
        let closed = result.source().close().map_err(SqlQueryError::unknown);
        materialized?;
        closed?;

        // Go opens the cursor even when the reader worker has already latched
        // a spill-read failure. COM_STMT_FETCH owns reporting that failure and
        // atomically removes the cursor; surfacing it from COM_STMT_EXECUTE
        // changes both the wire command and cleanup point.
        cursor.reader = Some(RowContainerReader::new(&cursor.rows));
        cursor.finish_materialization();
        Ok(cursor)
    }

    fn materialize_source(&mut self, result: &mut QueryResult<'_>) -> Result<(), SqlQueryError> {
        loop {
            let batch = result
                .source()
                .next_batch(self.max_chunk_size)
                .map_err(SqlQueryError::unknown)?;
            if batch.is_empty() {
                break;
            }
            let mut chunk =
                Chunk::new(&self.field_types, self.init_chunk_size, self.max_chunk_size);
            for row in batch {
                if row.len() != self.field_types.len() {
                    return Err(SqlQueryError::unknown(format!(
                        "cursor row has {} values for {} columns",
                        row.len(),
                        self.field_types.len()
                    )));
                }
                for (column, datum) in row.iter().enumerate() {
                    chunk.append_datum(column, datum);
                }
            }
            self.total_rows = self.total_rows.saturating_add(chunk.num_rows());
            self.rows.add(chunk).map_err(cursor_disk_error)?;
            self.memory.check().map_err(cursor_exec_error)?;
        }
        self.columns = result.source().columns().map_err(SqlQueryError::unknown)?;
        if self.columns.len() != self.field_types.len() {
            return Err(SqlQueryError::unknown(format!(
                "cursor metadata has {} columns for {} field types",
                self.columns.len(),
                self.field_types.len()
            )));
        }
        result.source().finish().map_err(SqlQueryError::unknown)
    }

    pub(crate) fn columns(&self) -> &[tidb_protocol::ColumnInfo] {
        &self.columns
    }

    pub(crate) fn fetch_plan(&self, fetch_size: u32) -> (usize, bool) {
        let remaining = self.total_rows.saturating_sub(self.next_row);
        let row_count = remaining.min(fetch_size as usize);
        (row_count, row_count >= remaining)
    }

    pub(crate) fn write_fetch(
        &mut self,
        output: &mut ClientStream,
        row_count: usize,
        options: tidb_protocol::ResultSetOptions,
    ) -> Result<(), CursorFetchError> {
        let mut stream = tidb_protocol::BinaryResultSetStream::new(self.columns.clone(), options)
            .map_err(|error| CursorFetchError::Protocol {
            message: error.to_string(),
            sequence: 1,
        })?;
        let _ = stream
            .metadata_packets()
            .map_err(|error| CursorFetchError::Protocol {
                message: error.to_string(),
                sequence: 1,
            })?;
        let mut sequence = 1_u8;
        for _ in 0..row_count {
            let reader = self
                .reader
                .as_mut()
                .expect("materialized cursor has a reader");
            if let Some(error) = reader.error() {
                return Err(CursorFetchError::Protocol {
                    message: error.to_owned(),
                    sequence,
                });
            }
            let row = reader.current().ok_or_else(|| CursorFetchError::Protocol {
                message: "cursor ended before its retained row count".to_owned(),
                sequence,
            })?;
            let datums = row.get_datum_row(&self.field_types);
            let cells: Vec<tidb_protocol::BinaryResultCell> = datums
                .into_iter()
                .zip(&self.columns)
                .map(|(datum, column)| {
                    crate::connection_resultset::datum_to_binary_cell(datum, column.type_code)
                        .ok_or_else(|| CursorFetchError::Protocol {
                            message: format!(
                                "cursor row datum does not match column type {}",
                                column.type_code
                            ),
                            sequence,
                        })
                })
                .collect::<Result<_, _>>()?;
            let packet = stream
                .row_packet(&cells)
                .map_err(|error| CursorFetchError::Protocol {
                    message: error.to_string(),
                    sequence,
                })?;
            write_packet_to(output, sequence, &packet).map_err(CursorFetchError::Transport)?;
            sequence = sequence.wrapping_add(1);
            reader.next_row();
            self.next_row += 1;
            if let Some(error) = reader.error() {
                return Err(CursorFetchError::Protocol {
                    message: error.to_owned(),
                    sequence,
                });
            }
        }
        write_eof_or_ok(output, sequence, options).map_err(CursorFetchError::Transport)
    }

    fn finish_materialization(&mut self) {
        if let Some(action) = self.registered_action.take() {
            self.memory
                .session_tracker()
                .unbind_action_from_hard_limit(&action);
            action.set_finished();
        }
        self.memory.finish_statement();
    }

    fn close(&mut self) {
        if let Some(reader) = self.reader.as_mut() {
            reader.close();
        }
        self.finish_materialization();
        self.rows.close();
    }
}

impl Drop for CursorState {
    fn drop(&mut self) {
        self.close();
    }
}

fn cursor_exec_error(error: tidb_executor::ExecError) -> SqlQueryError {
    let mapped = tidb_executor::DriverError::Exec(error).to_mysql_error();
    SqlQueryError::new(mapped.code, mapped.state, mapped.message)
}

fn cursor_disk_error(error: DiskError) -> SqlQueryError {
    cursor_exec_error(tidb_executor::ExecError::SpillFailed(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_executor::{OomAction, StatementMemory};
    use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

    use super::*;
    use crate::pipeline_session::select_columns;

    #[derive(Default)]
    struct SourceLifecycle {
        finish: usize,
        close: usize,
    }

    struct CountingRows {
        columns: Vec<tidb_protocol::ColumnInfo>,
        rows: VecDeque<Vec<Datum>>,
        lifecycle: Arc<Mutex<SourceLifecycle>>,
    }

    impl ResultSetSource for CountingRows {
        fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
            Ok((0..max_rows.max(1))
                .map_while(|_| self.rows.pop_front())
                .collect())
        }

        fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
            Ok(self.columns.clone())
        }

        fn finish(&mut self) -> Result<(), String> {
            self.lifecycle.lock().unwrap().finish += 1;
            Ok(())
        }

        fn close(&mut self) -> Result<(), String> {
            self.lifecycle.lock().unwrap().close += 1;
            Ok(())
        }
    }

    fn non_lock_files(path: &std::path::Path) -> usize {
        fs::read_dir(path)
            .expect("spill directory remains readable")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != "_dir.lock")
            .count()
    }

    #[test]
    fn quota_spills_cursor_rows_and_drop_releases_every_resource() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tidb_cursor_state_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        let storage = Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: path.clone(),
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("isolated cursor spill authority"),
        );
        let memory = StatementMemory::new(1, OomAction::Cancel, 7)
            .with_spill_storage(Arc::clone(&storage))
            .with_tmp_storage_on_oom(true);
        let session_tracker = Arc::clone(memory.session_tracker());
        let disk_tracker = Arc::clone(memory.session_disk_tracker());
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let lifecycle = Arc::new(Mutex::new(SourceLifecycle::default()));
        let source = CountingRows {
            columns: select_columns(&[("v".to_owned(), fields[0].clone())]),
            rows: (0..64).map(|value| vec![Datum::Int(value)]).collect(),
            lifecycle: Arc::clone(&lifecycle),
        };
        let mut result = QueryResult::new(Box::new(source));

        let cursor = CursorState::materialize(
            &mut result,
            CursorMaterializationAuthority {
                field_types: fields,
                init_chunk_size: 2,
                max_chunk_size: 8,
                memory,
            },
        )
        .expect("cursor spill releases memory before CANCEL");
        assert!(cursor.rows.already_spilled());
        assert_eq!(cursor.fetch_plan(u32::MAX), (64, true));
        assert_eq!(session_tracker.bytes_consumed(), 0);
        assert!(disk_tracker.bytes_consumed() > 0);
        assert!(storage.global_tracker().bytes_consumed() > 0);
        assert!(non_lock_files(&path) > 0);
        assert_eq!(lifecycle.lock().unwrap().finish, 1);
        assert_eq!(lifecycle.lock().unwrap().close, 1);

        drop(cursor);
        assert_eq!(session_tracker.bytes_consumed(), 0);
        assert_eq!(disk_tracker.bytes_consumed(), 0);
        assert_eq!(storage.global_tracker().bytes_consumed(), 0);
        assert_eq!(non_lock_files(&path), 0);
        drop(result);
        drop(storage);
        fs::remove_dir_all(path).expect("remove isolated cursor spill directory");
    }
}
