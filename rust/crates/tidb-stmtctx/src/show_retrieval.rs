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

//! The rows a `SHOW` statement is handed before its executor runs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use tidb_datatype::Datum;

/// The rows Go's `ShowExec.fetchAll` produced for each `PhysicalShow` of one
/// statement, keyed by its plan id. Go's `ShowExec` fetches through its own
/// session; the fetchers here read the session, which an executor cannot
/// borrow, so the session supplies them before the executor is built.
/// Clones of a statement context share it.
#[derive(Default)]
pub struct ShowRetrieval {
    rows: Mutex<HashMap<i32, Arc<Vec<Vec<Datum>>>>>,
}

impl ShowRetrieval {
    /// Supplies the rows of the `PhysicalShow` numbered `plan_id`, one value
    /// per column of its schema.
    pub fn supply(&self, plan_id: i32, rows: Vec<Vec<Datum>>) {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(plan_id, Arc::new(rows));
    }

    /// The rows supplied for the `PhysicalShow` numbered `plan_id`.
    #[must_use]
    pub fn rows(&self, plan_id: i32) -> Option<Arc<Vec<Vec<Datum>>>> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&plan_id)
            .cloned()
    }
}
