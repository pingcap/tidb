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

//! Go `br/pkg/utils/db.go`'s log-backup task count, which the log-backup
//! advancer (`br/pkg/streamhelper`) raises and lowers as tasks come and go,
//! and telemetry reads.

use std::sync::atomic::{AtomicI32, Ordering};

use tidb_log::{Field, Value};

/// Go `logBackupTaskCount`.
static LOG_BACKUP_TASK_COUNT: AtomicI32 = AtomicI32::new(0);

/// Go `LogBackupTaskCountInc`.
pub fn log_backup_task_count_inc() {
    let count = LOG_BACKUP_TASK_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
    tidb_log::info("inc log backup task", &[Field::new("count", Value::I64(i64::from(count)))]);
}

/// Go `LogBackupTaskCountDec`.
pub fn log_backup_task_count_dec() {
    let count = LOG_BACKUP_TASK_COUNT.fetch_sub(1, Ordering::SeqCst) - 1;
    tidb_log::info("dec log backup task", &[Field::new("count", Value::I64(i64::from(count)))]);
}

/// Go `CheckLogBackupTaskExist`.
#[must_use]
pub fn check_log_backup_task_exist() -> bool {
    LOG_BACKUP_TASK_COUNT.load(Ordering::SeqCst) > 0
}

/// Go `IsLogBackupInUse`.
#[must_use]
pub fn is_log_backup_in_use() -> bool {
    check_log_backup_task_exist()
}
