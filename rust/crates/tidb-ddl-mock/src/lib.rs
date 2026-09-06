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

//! Complete native test-double transcreation of generated Go `pkg/ddl/mock`.

use std::collections::VecDeque;
use std::sync::Mutex;

use tidb_exec::cluster_catalog::MetaSnapshot;
use tidb_exec::ddl_job_scheduler::SchemaLoader;
use tidb_exec::ddl_systable::{Manager, SystemTableManagerError};
use tidb_model::JobW;

type ReloadCall = Box<dyn FnOnce() -> Result<(), String> + Send>;
type JobCall =
    Box<dyn FnOnce(&mut dyn MetaSnapshot, i64) -> Result<JobW, SystemTableManagerError> + Send>;
type BytesCall =
    Box<dyn FnOnce(&mut dyn MetaSnapshot, i64) -> Result<Vec<u8>, SystemTableManagerError> + Send>;
type I64Call =
    Box<dyn FnOnce(&mut dyn MetaSnapshot, i64) -> Result<i64, SystemTableManagerError> + Send>;
type BoolCall =
    Box<dyn FnOnce(&mut dyn MetaSnapshot, i64) -> Result<bool, SystemTableManagerError> + Send>;

/// Native recorder for generated Go `MockSchemaLoader`.
#[derive(Default)]
pub struct MockSchemaLoader {
    reload_calls: Mutex<VecDeque<ReloadCall>>,
}

impl MockSchemaLoader {
    /// Go `NewMockSchemaLoader` without the language-specific controller.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `EXPECT`.
    pub const fn expect(&self) -> MockSchemaLoaderRecorder<'_> {
        MockSchemaLoaderRecorder { mock: self }
    }

    /// Go `ISGOMOCK`'s zero-sized marker.
    pub const fn is_mock(&self) {}

    /// Go `Controller.Finish`/`Satisfied` missing-call check.
    pub fn verify(&self) {
        let pending = self.reload_calls.lock().unwrap().len();
        assert_eq!(pending, 0, "missing {pending} SchemaLoader call(s)");
    }
}

impl Drop for MockSchemaLoader {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.verify();
        }
    }
}

/// Recorder returned by [`MockSchemaLoader::expect`].
pub struct MockSchemaLoaderRecorder<'a> {
    mock: &'a MockSchemaLoader,
}

impl MockSchemaLoaderRecorder<'_> {
    /// Records one expected Go `Reload` call.
    pub fn reload(self, call: impl FnOnce() -> Result<(), String> + Send + 'static) {
        self.mock
            .reload_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }
}

impl SchemaLoader for MockSchemaLoader {
    fn reload(&self) -> Result<(), String> {
        self.reload_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected Reload call")()
    }
}

/// Native recorder for generated Go `MockManager`.
#[derive(Default)]
pub struct MockManager {
    job_calls: Mutex<VecDeque<JobCall>>,
    bytes_calls: Mutex<VecDeque<BytesCall>>,
    mdl_calls: Mutex<VecDeque<I64Call>>,
    min_calls: Mutex<VecDeque<I64Call>>,
    flashback_calls: Mutex<VecDeque<BoolCall>>,
}

impl MockManager {
    /// Go `NewMockManager` without the language-specific controller.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `EXPECT`.
    pub const fn expect(&self) -> MockManagerRecorder<'_> {
        MockManagerRecorder { mock: self }
    }

    /// Go `ISGOMOCK`'s zero-sized marker.
    pub const fn is_mock(&self) {}

    /// Go `Controller.Finish`/`Satisfied` missing-call check.
    pub fn verify(&self) {
        let pending = self.job_calls.lock().unwrap().len()
            + self.bytes_calls.lock().unwrap().len()
            + self.mdl_calls.lock().unwrap().len()
            + self.min_calls.lock().unwrap().len()
            + self.flashback_calls.lock().unwrap().len();
        assert_eq!(pending, 0, "missing {pending} system-table manager call(s)");
    }
}

impl Drop for MockManager {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.verify();
        }
    }
}

/// Recorder returned by [`MockManager::expect`].
pub struct MockManagerRecorder<'a> {
    mock: &'a MockManager,
}

impl MockManagerRecorder<'_> {
    /// Records one expected Go `GetJobByID` call.
    pub fn get_job_by_id(
        self,
        call: impl FnOnce(&mut dyn MetaSnapshot, i64) -> Result<JobW, SystemTableManagerError>
            + Send
            + 'static,
    ) {
        self.mock
            .job_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }

    /// Records one expected Go `GetJobBytesByIDWithSe` call.
    pub fn get_job_bytes_by_id_with_session(
        self,
        call: impl FnOnce(&mut dyn MetaSnapshot, i64) -> Result<Vec<u8>, SystemTableManagerError>
            + Send
            + 'static,
    ) {
        self.mock
            .bytes_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }

    /// Records one expected Go `GetMDLVer` call.
    pub fn get_mdl_version(
        self,
        call: impl FnOnce(&mut dyn MetaSnapshot, i64) -> Result<i64, SystemTableManagerError>
            + Send
            + 'static,
    ) {
        self.mock
            .mdl_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }

    /// Records one expected Go `GetMinJobID` call.
    pub fn get_min_job_id(
        self,
        call: impl FnOnce(&mut dyn MetaSnapshot, i64) -> Result<i64, SystemTableManagerError>
            + Send
            + 'static,
    ) {
        self.mock
            .min_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }

    /// Records one expected Go `HasFlashbackClusterJob` call.
    pub fn has_flashback_cluster_job(
        self,
        call: impl FnOnce(&mut dyn MetaSnapshot, i64) -> Result<bool, SystemTableManagerError>
            + Send
            + 'static,
    ) {
        self.mock
            .flashback_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }
}

impl Manager for MockManager {
    fn get_job_by_id(
        &self,
        snapshot: &mut dyn MetaSnapshot,
        job_id: i64,
    ) -> Result<JobW, SystemTableManagerError> {
        self.job_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected GetJobByID call")(snapshot, job_id)
    }

    fn get_job_bytes_by_id_with_session(
        &self,
        snapshot: &mut dyn MetaSnapshot,
        job_id: i64,
    ) -> Result<Vec<u8>, SystemTableManagerError> {
        self.bytes_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected GetJobBytesByIDWithSe call")(snapshot, job_id)
    }

    fn get_mdl_version(
        &self,
        snapshot: &mut dyn MetaSnapshot,
        job_id: i64,
    ) -> Result<i64, SystemTableManagerError> {
        self.mdl_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected GetMDLVer call")(snapshot, job_id)
    }

    fn get_min_job_id(
        &self,
        snapshot: &mut dyn MetaSnapshot,
        previous_min_job_id: i64,
    ) -> Result<i64, SystemTableManagerError> {
        self.min_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected GetMinJobID call")(snapshot, previous_min_job_id)
    }

    fn has_flashback_cluster_job(
        &self,
        snapshot: &mut dyn MetaSnapshot,
        min_job_id: i64,
    ) -> Result<bool, SystemTableManagerError> {
        self.flashback_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected HasFlashbackClusterJob call")(snapshot, min_job_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use tidb_exec::ddl_job_scheduler::{must_reload_schemas, UnsyncedJobTracker};

    use super::*;

    #[deny(unused_must_use)]
    #[test]
    fn go_mock_constructor_and_expect_returns_can_be_ignored() {
        MockSchemaLoader::new();
        let schema_loader = MockSchemaLoader::new();
        schema_loader.expect();
        MockManager::new();
        let manager = MockManager::new();
        manager.expect();
    }

    #[test]
    fn schema_loader_and_scheduler_contract() {
        let loader = MockSchemaLoader::new();
        let (_cancel, cancelled) = mpsc::channel();
        loader.expect().reload(|| Ok(()));
        must_reload_schemas(&loader, &cancelled, Duration::from_millis(10));

        loader.expect().reload(|| Err("mock err".to_owned()));
        loader.expect().reload(|| Ok(()));
        must_reload_schemas(&loader, &cancelled, Duration::from_millis(10));

        let (cancel, cancelled) = mpsc::channel();
        loader.expect().reload(move || {
            cancel.send(()).unwrap();
            Err("mock err".to_owned())
        });
        must_reload_schemas(&loader, &cancelled, Duration::from_millis(10));
        loader.verify();
    }

    #[test]
    fn unsynced_job_tracker_contract() {
        let tracker = UnsyncedJobTracker::new();
        tracker.add_unsynced(1);
        assert!(tracker.is_unsynced(1));
        tracker.remove_unsynced(1);
        assert!(!tracker.is_unsynced(1));
    }
}
