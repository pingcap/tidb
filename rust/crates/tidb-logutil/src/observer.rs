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

//! zap's `zaptest/observer`: a logger whose entries stay in memory, for
//! tests that assert what was logged.

use std::sync::{Arc, Mutex};

use tidb_log::{Entry, Field, Level};

use crate::Logger;

/// zap `observer.LoggedEntry`: one observed entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggedEntry {
    /// The entry's level.
    pub level: Level,
    /// The entry's message.
    pub message: String,
}

/// zap `observer.ObservedLogs`.
#[derive(Default)]
pub struct ObservedLogs {
    entries: Mutex<Vec<LoggedEntry>>,
}

impl ObservedLogs {
    pub(crate) fn add(&self, level: Level, message: &str) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(LoggedEntry { level, message: message.to_owned() });
    }

    /// zap `All`.
    pub fn all(&self) -> Vec<LoggedEntry> {
        self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// zap `FilterMessage(msg).All()`.
    pub fn filter_message(&self, message: &str) -> Vec<LoggedEntry> {
        self.all().into_iter().filter(|entry| entry.message == message).collect()
    }
}

/// zap `observer.New(level)`'s core: entries at or above `level` kept in
/// memory.
struct ObserverCore {
    level: tidb_log::AtomicLevel,
    logs: Arc<ObservedLogs>,
}

impl tidb_log::Core for ObserverCore {
    fn enabled(&self, level: Level) -> bool {
        level >= self.level.get()
    }
    fn with(&self, _fields: &[Field]) -> Arc<dyn tidb_log::Core> {
        Arc::new(ObserverCore {
            level: self.level.clone(),
            logs: Arc::clone(&self.logs),
        })
    }
    fn write(&self, entry: &Entry, _fields: &[Field]) -> Result<(), String> {
        self.logs.add(entry.level, &entry.message);
        Ok(())
    }
    fn level(&self) -> tidb_log::AtomicLevel {
        self.level.clone()
    }
}

/// zap `observer.New(level)` wrapped in `zap.New`: a logger at `level`
/// whose entries go to the returned logs.
pub fn new(level: Level) -> (Logger, Arc<ObservedLogs>) {
    let logs = Arc::new(ObservedLogs::default());
    let core = ObserverCore {
        level: tidb_log::AtomicLevel::new(level),
        logs: Arc::clone(&logs),
    };
    (Logger::new(Arc::new(core)), logs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_entries_are_kept_by_level() {
        let (logger, logs) = new(Level::Info);
        logger.debug("dropped", &[]);
        logger.info("kept", &[]);
        logger.with_fields(&[]).warn("kept", &[]);
        assert_eq!(logs.filter_message("dropped"), Vec::new());
        assert_eq!(
            logs.filter_message("kept"),
            vec![
                LoggedEntry { level: Level::Info, message: "kept".to_owned() },
                LoggedEntry { level: Level::Warn, message: "kept".to_owned() },
            ]
        );
    }
}
