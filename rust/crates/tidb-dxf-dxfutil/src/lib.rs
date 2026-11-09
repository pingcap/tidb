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

//! Go `pkg/dxf/framework/dxfutil`: the runtime of a task's keyspace, which
//! the scheduler and the task executor reach the keyspace through.

use std::sync::Arc;

use tidb_domain_sqlsvrapi::Runtime;
use tidb_dxf_storage::{Result, SessionContext, StorageError, TaskManager};

/// The runtime of a task's keyspace, as the framework holds it.
pub type TaskRuntime = Arc<dyn Runtime<dyn SessionContext>>;

/// Ends the holding of a runtime [`acquire_task_runtime`] returned.
pub type ReleaseFn = Box<dyn FnOnce() + Send>;

/// Go `sessionProvider`: provides a session to reach the SQL server.
pub trait SessionProvider {
    /// Go `WithNewSession`.
    ///
    /// # Errors
    /// The session or `f` failed.
    fn with_new_session(&self, f: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()>;
}

impl SessionProvider for TaskManager {
    fn with_new_session(&self, f: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
        TaskManager::with_new_session(self, f)
    }
}

/// Go `AcquireTaskRuntime`: the runtime of the task's keyspace and the
/// function that releases it. The provider's sessions must be of the
/// current node's keyspace, which tells whether the task belongs to another
/// keyspace. Callers must call the release function once done with the
/// runtime.
///
/// # Errors
/// The session failed, or the task's keyspace cannot be reached.
pub fn acquire_task_runtime(
    session_provider: &dyn SessionProvider,
    task_ks: &str,
    holder_id: &str,
) -> Result<(TaskRuntime, ReleaseFn)> {
    let mut acquired: Option<(TaskRuntime, ReleaseFn)> = None;
    session_provider.with_new_session(&mut |se| {
        let current_ks = se.store_keyspace();
        let sql_server = se.sql_server();
        if task_ks != current_ks {
            let handle = sql_server.acquire_ks_runtime(task_ks, holder_id).map_err(StorageError::Other)?;
            let release_handle = Arc::clone(&handle);
            // Go `releaseTaskRuntime`: a keyspace runtime handle is released.
            acquired = Some((handle.as_runtime(), Box::new(move || release_handle.release())));
            return Ok(());
        }
        acquired = Some((sql_server.get_runtime(), Box::new(|| {})));
        Ok(())
    })?;
    Ok(acquired.expect("the session callback ran"))
}

/// Go `CheckTaskRuntime`: whether the runtime is valid for a task of the
/// target keyspace.
///
/// # Errors
/// The runtime's store, or its sessions' store, is of another keyspace.
pub fn check_task_runtime(runtime: &dyn Runtime<dyn SessionContext>, task_ks: &str) -> Result<()> {
    let store_ks = runtime.store_keyspace();
    if store_ks != task_ks {
        // shouldn't happen normally, but since keyspace mismatch might cause
        // correctness error, we check it at runtime too.
        return Err(StorageError::Other(format!("store keyspace mismatch with task: {store_ks} vs {task_ks}")));
    }
    let task_mgr = TaskManager::new(runtime.sys_session_pool());
    task_mgr.with_new_session(&mut |se| {
        let sess_ks = se.store_keyspace();
        if store_ks != sess_ks {
            // shouldn't happen normally. we do it for the same reason as above.
            return Err(StorageError::Other(format!(
                "invalid task runtime with mismatched keyspace: {store_ks} vs {sess_ks}"
            )));
        }
        Ok(())
    })
}

/// Go `GenHolderID`: the holder ID of a DXF component working on a task.
#[must_use]
pub fn gen_holder_id(component: &str, task_id: i64) -> String {
    format!("DXF/{component}/{task_id}")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use tidb_domain_sqlsvrapi::{KsRuntimeHandle, Runtime, Server};
    use tidb_dxf_storage::{Result, SessionContext, StorageError};

    use super::{acquire_task_runtime, check_task_runtime, SessionProvider, TaskRuntime};

    /// Go `utilmock.NewContext()` with a store of the keyspace and the
    /// server bound.
    struct MockSession {
        keyspace: String,
        server: Option<Arc<dyn Server<dyn SessionContext>>>,
    }

    impl tidb_syssession::SessionContext for MockSession {
        fn close(&self) {}
        fn rollback_txn(&self, _: &dyn tidb_sqlexec::ExecutionContext) {}
        fn has_prepared_txn_future(&self) -> bool {
            false
        }
        fn txn_valid(&self) -> std::result::Result<bool, tidb_sqlexec::SqlExecError> {
            Ok(false)
        }
        fn sql_executor(&self) -> Arc<dyn tidb_sqlexec::SqlExecutor> {
            unreachable!("the tests run no SQL")
        }
        fn restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
            unreachable!("the tests run no SQL")
        }
        fn register_internal_session(&self) {}
        fn unregister_internal_session(&self) {}
        fn contains_internal_session(&self) -> bool {
            false
        }
        fn store_internal_session(&self) -> bool {
            true
        }
    }

    impl SessionContext for MockSession {
        fn commit_txn(&self, _: &dyn tidb_sqlexec::ExecutionContext) -> Result<()> {
            Ok(())
        }
        fn txn_entry_size_limit(&self) -> u64 {
            0
        }
        fn set_txn_entry_size_limit(&self, _: u64) {}
        fn global_txn_entry_size_limit(&self) -> u64 {
            0
        }
        fn affected_rows(&self) -> u64 {
            0
        }
        fn mem_quota_query(&self) -> i64 {
            0
        }
        fn set_system_var(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn store_keyspace(&self) -> String {
            self.keyspace.clone()
        }
        fn sql_server(&self) -> Arc<dyn Server<dyn SessionContext>> {
            Arc::clone(self.server.as_ref().expect("a server is bound"))
        }
    }

    /// Go `newCheckTaskRuntimeSessionPool`: one session of the store's
    /// keyspace.
    fn new_check_task_runtime_session_pool(keyspace: &str) -> Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
        let keyspace = keyspace.to_owned();
        Arc::new(tidb_syssession::AdvancedSessionPool::<dyn SessionContext>::new(1, move || {
            Ok(Arc::new(MockSession { keyspace: keyspace.clone(), server: None }) as Arc<dyn SessionContext>)
        }))
    }

    /// Go `sqlsvrapimock.MockRuntime` (and `MockKSRuntimeHandle`).
    struct MockRuntime {
        keyspace: String,
        se_pool: Option<Arc<dyn tidb_syssession::Pool<dyn SessionContext>>>,
        released: AtomicBool,
    }

    fn mock_runtime(keyspace: &str, se_pool: Option<Arc<dyn tidb_syssession::Pool<dyn SessionContext>>>) -> Arc<MockRuntime> {
        Arc::new(MockRuntime { keyspace: keyspace.to_owned(), se_pool, released: AtomicBool::new(false) })
    }

    impl Runtime<dyn SessionContext> for MockRuntime {
        fn store_keyspace(&self) -> String {
            self.keyspace.clone()
        }
        fn sys_session_pool(&self) -> Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
            Arc::clone(self.se_pool.as_ref().expect("the test sets a session pool"))
        }
    }

    impl KsRuntimeHandle<dyn SessionContext> for MockRuntime {
        fn release(&self) {
            assert!(!self.released.swap(true, Ordering::SeqCst), "released once");
        }
        fn as_runtime(self: Arc<Self>) -> Arc<dyn Runtime<dyn SessionContext>> {
            self
        }
    }

    /// Go `sqlsvrapimock.MockServer`.
    struct MockServer {
        runtime: Option<Arc<MockRuntime>>,
        acquired: Mutex<Option<std::result::Result<Arc<MockRuntime>, String>>>,
    }

    impl Server<dyn SessionContext> for MockServer {
        fn get_runtime(&self) -> Arc<dyn Runtime<dyn SessionContext>> {
            Arc::clone(self.runtime.as_ref().expect("the test expects GetRuntime")) as Arc<dyn Runtime<dyn SessionContext>>
        }
        fn acquire_ks_runtime(
            &self,
            target_ks: &str,
            holder_id: &str,
        ) -> std::result::Result<Arc<dyn KsRuntimeHandle<dyn SessionContext>>, String> {
            assert_eq!((target_ks, holder_id), ("task_ks", "holder"));
            self.acquired
                .lock()
                .unwrap()
                .take()
                .expect("the test expects AcquireKSRuntime once")
                .map(|handle| handle as Arc<dyn KsRuntimeHandle<dyn SessionContext>>)
        }
    }

    /// Go `taskSessionProvider`.
    struct TaskSessionProvider {
        se: Option<MockSession>,
        err: Option<StorageError>,
    }

    impl SessionProvider for TaskSessionProvider {
        fn with_new_session(&self, f: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
            if let Some(err) = &self.err {
                return Err(err.clone());
            }
            f(self.se.as_ref().expect("a session"))
        }
    }

    /// Go `newTaskSessionProvider`.
    fn new_task_session_provider(server: Arc<MockServer>, current_ks: &str) -> TaskSessionProvider {
        TaskSessionProvider {
            se: Some(MockSession { keyspace: current_ks.to_owned(), server: Some(server) }),
            err: None,
        }
    }

    fn same(runtime: &TaskRuntime, expected: &Arc<MockRuntime>) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(runtime), Arc::as_ptr(expected))
    }

    /// Go `TestAcquireTaskRuntime`.
    #[test]
    fn test_acquire_task_runtime() {
        // current keyspace uses server runtime
        let runtime = mock_runtime("task_ks", None);
        let server = Arc::new(MockServer { runtime: Some(Arc::clone(&runtime)), acquired: Mutex::new(None) });
        let (got_runtime, release_runtime) =
            acquire_task_runtime(&new_task_session_provider(server, "task_ks"), "task_ks", "holder").unwrap();
        assert!(same(&got_runtime, &runtime));
        release_runtime();

        // different keyspace acquires and releases handle
        let runtime_handle = mock_runtime("task_ks", None);
        let server = Arc::new(MockServer { runtime: None, acquired: Mutex::new(Some(Ok(Arc::clone(&runtime_handle)))) });
        let (got_runtime, release_runtime) =
            acquire_task_runtime(&new_task_session_provider(server, "current_ks"), "task_ks", "holder").unwrap();
        assert!(same(&got_runtime, &runtime_handle));
        release_runtime();
        assert!(runtime_handle.released.load(Ordering::SeqCst));

        // acquire error
        let server =
            Arc::new(MockServer { runtime: None, acquired: Mutex::new(Some(Err("ks runtime not found".to_owned()))) });
        let error = acquire_task_runtime(&new_task_session_provider(server, "current_ks"), "task_ks", "holder")
            .err()
            .unwrap();
        assert_eq!(error, StorageError::Other("ks runtime not found".to_owned()));

        // session error
        let session_err = StorageError::Other("session error".to_owned());
        let error = acquire_task_runtime(&TaskSessionProvider { se: None, err: Some(session_err.clone()) }, "task_ks", "holder")
            .err()
            .unwrap();
        assert_eq!(error, session_err);
    }

    /// Go `TestCheckTaskRuntime`.
    #[test]
    fn test_check_task_runtime() {
        // valid runtime
        let runtime = mock_runtime("task_ks", Some(new_check_task_runtime_session_pool("task_ks")));
        check_task_runtime(runtime.as_ref(), "task_ks").unwrap();

        // store keyspace mismatch
        let runtime = mock_runtime("store_ks", None);
        let error = check_task_runtime(runtime.as_ref(), "task_ks").unwrap_err();
        assert!(error.to_string().contains("store keyspace mismatch with task: store_ks vs task_ks"), "{error}");

        // session keyspace mismatch
        let runtime = mock_runtime("task_ks", Some(new_check_task_runtime_session_pool("session_ks")));
        let error = check_task_runtime(runtime.as_ref(), "task_ks").unwrap_err();
        assert!(error.to_string().contains("invalid task runtime with mismatched keyspace: task_ks vs session_ks"), "{error}");
    }
}
