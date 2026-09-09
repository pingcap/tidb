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

//! Selected record keys for Go's `SelectLockExec`. The executor extracts
//! identities from its output rows; the session owns lock RPCs and replay.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use tidb_chunk::{chunk::Chunk, row::Row};
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

use crate::{ExecError, Executor};

/// Keys selected by all locking operators in one statement attempt.
/// The owner must drain this set at every attempt boundary, including errors.
#[derive(Clone, Debug, Default)]
pub struct SelectedLockKeys(Arc<Mutex<BTreeSet<Vec<u8>>>>);

impl SelectedLockKeys {
    /// Adds a matched DML row that needs a lock even without a mutation.
    pub(crate) fn insert(&self, key: Vec<u8>) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).insert(key);
    }

    /// Takes the attempt's unique keys in encoded-key order.
    pub fn take(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(|p| p.into_inner()))
            .into_iter()
            .collect()
    }

    fn extend(&self, keys: BTreeSet<Vec<u8>>) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(keys);
    }
}

/// A physical plan's record-key expression over a selected row. Each joined
/// base table supplies one expression. `None` denotes a null-extended input
/// without a physical row, not an unknown or pruned handle.
pub type SelectedRecordKey = Box<dyn Fn(Row<'_>) -> Result<Option<Vec<u8>>, ExecError>>;

/// Passes rows through unchanged and collects their physical record keys.
/// Like Go's SelectLockExec, it publishes keys only after draining its child.
/// The containing session must acquire them before exposing query results.
pub struct SelectLockExec {
    child: Box<dyn Executor>,
    expressions: Vec<SelectedRecordKey>,
    selected: SelectedLockKeys,
    pending: BTreeSet<Vec<u8>>,
}

impl SelectLockExec {
    /// Wraps the plan at Go's SelectLock position; filters, sorting and LIMIT
    /// placement are planner responsibilities, not storage-read heuristics.
    pub fn new(
        child: Box<dyn Executor>,
        expressions: Vec<SelectedRecordKey>,
        selected: SelectedLockKeys,
    ) -> Self {
        Self {
            child,
            expressions,
            selected,
            pending: BTreeSet::new(),
        }
    }
}

impl Executor for SelectLockExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.pending.clear();
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.child.next(req)?;
        if req.num_rows() == 0 {
            self.selected.extend(std::mem::take(&mut self.pending));
        } else {
            for index in 0..req.num_rows() {
                for expression in &self.expressions {
                    if let Some(key) = expression(req.get_row(index))? {
                        self.pending.insert(key);
                    }
                }
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.pending.clear();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
    fn ret_field_types(&self) -> &[FieldType] {
        self.child.ret_field_types()
    }
    fn init_cap(&self) -> usize {
        self.child.init_cap()
    }
    fn max_chunk_size(&self) -> usize {
        self.child.max_chunk_size()
    }
    fn new_chunk(&self) -> Chunk {
        self.child.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutorMeta, LimitExec};
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;

    struct Source {
        meta: ExecutorMeta,
        emitted: bool,
    }

    impl Executor for Source {
        fn open(&mut self) -> Result<(), ExecError> {
            self.emitted = false;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if !self.emitted {
                for id in [1, 2, 2, 3] {
                    req.append_int64(0, id);
                }
                self.emitted = true;
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn schema(&self) -> &Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            self.meta.init_cap()
        }
        fn max_chunk_size(&self) -> usize {
            self.meta.max_chunk_size()
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    fn source() -> Box<dyn Executor> {
        let schema = Schema::new(vec![Column::new(
            1,
            FieldType::new(FieldTypeCode::LongLong),
        )]);
        Box::new(Source {
            meta: ExecutorMeta::new(schema, 1, 4, 4),
            emitted: false,
        })
    }

    fn key(row: Row<'_>) -> Result<Option<Vec<u8>>, ExecError> {
        Ok(Some(row.get_int64(0).to_be_bytes().to_vec()))
    }

    #[test]
    fn collects_only_limit_output_and_publishes_at_eof() {
        let child = source();
        let meta = ExecutorMeta::new(child.schema().clone(), 2, 4, 4);
        let child = Box::new(LimitExec::new(meta, 1, 2, child));
        let selected = SelectedLockKeys::default();
        let mut exec = SelectLockExec::new(child, vec![Box::new(key)], selected.clone());
        let mut chunk = exec.new_chunk();
        exec.open().unwrap();
        exec.next(&mut chunk).unwrap();
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.get_row(0).get_int64(0), 2);
        assert!(
            selected.take().is_empty(),
            "incomplete results cannot authorize locking"
        );
        exec.next(&mut chunk).unwrap();
        assert_eq!(selected.take(), vec![2_i64.to_be_bytes().to_vec()]);
        exec.close().unwrap();
        assert!(selected.take().is_empty());
    }

    #[test]
    fn failure_does_not_publish_a_partial_lock_set() {
        let selected = SelectedLockKeys::default();
        let expression: SelectedRecordKey = Box::new(|row| {
            if row.get_int64(0) == 2 {
                return Err(ExecError::internal("missing handle"));
            }
            key(row)
        });
        let mut exec = SelectLockExec::new(source(), vec![expression], selected.clone());
        let mut chunk = exec.new_chunk();
        exec.open().unwrap();
        assert!(exec.next(&mut chunk).is_err());
        exec.close().unwrap();
        assert!(selected.take().is_empty());
    }

    #[test]
    fn joined_identities_skip_absent_rows_and_replay_without_old_keys() {
        let selected = SelectedLockKeys::default();
        let right: SelectedRecordKey = Box::new(|row| {
            // This fixture's row 1 has no matching right-side record.
            if row.get_int64(0) == 1 {
                return Ok(None);
            }
            Ok(Some((100 + row.get_int64(0)).to_be_bytes().to_vec()))
        });
        let mut exec = SelectLockExec::new(source(), vec![Box::new(key), right], selected.clone());
        let mut chunk = exec.new_chunk();
        for _ in 0..2 {
            exec.open().unwrap();
            loop {
                exec.next(&mut chunk).unwrap();
                if chunk.num_rows() == 0 {
                    break;
                }
            }
            assert_eq!(
                selected.take(),
                [1_i64, 2, 3, 102, 103]
                    .map(|id| id.to_be_bytes().to_vec())
                    .to_vec()
            );
            exec.close().unwrap();
            assert!(selected.take().is_empty());
        }
    }
}
