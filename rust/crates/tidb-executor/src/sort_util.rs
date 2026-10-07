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

//! Shared Go-style executor and worker panic recovery.
//! Sorting, spilling and cursor ownership live in the active Sort and TopN operators.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::executor::ExecError;

/// Recovers one Go-style executor panic and turns it into its query error.
///
/// Go's `processPanicAndLog` calls `util.GetRecoverError`: string panic values
/// retain their text, while non-string values use a stable formatted fallback.
pub(crate) fn recover_executor_panic<T>(
    operation: impl FnOnce() -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    catch_unwind(AssertUnwindSafe(operation)).map_err(|payload| {
        tidb_util::traceevent::dump_flight_recorder_to_logger("GetRecoverError");
        ExecError::internal(panic_message(payload.as_ref()))
    })?
}

/// Recovers a worker panic before it unwinds through the persistent executor
/// pool, where the receiver would otherwise report a misleading "dropped
/// result" error.
pub(crate) fn recover_worker_panic<T>(
    operation: impl FnOnce() -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    recover_executor_panic(operation)
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_panic_is_recovered_without_poisoning_the_pool() {
        let recovered = crate::worker_pool::submit(|| {
            recover_worker_panic(|| -> Result<(), ExecError> {
                panic!("sort worker boom");
            })
        });
        assert!(matches!(
            recovered,
            Err(ExecError::Internal(message)) if message == "sort worker boom"
        ));

        // Go's recovered worker remains part of the goroutine pool.  A later
        // task must still run after the panic boundary has converted the
        // first task to an executor error.
        assert_eq!(crate::worker_pool::submit(|| 7_u8), 7);
    }
}
