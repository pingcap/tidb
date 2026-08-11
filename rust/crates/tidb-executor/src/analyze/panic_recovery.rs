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

//! The shared recovery boundary for both `ANALYZE TABLE` execution tiers.
//!
//! Go runs analyze workers and result handling in goroutines and recovers a
//! panic at each goroutine boundary. Rust's in-process and cluster analyzers
//! are synchronous, so both source recovery sites collapse to this one guard.
//! Keeping it with the shared analyze engine prevents either production caller
//! from accidentally running the same computation without the source's panic
//! contract.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// The exact panic text emitted by the source global analyze-memory tracker.
pub const GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED: &str = "Out Of Global Analyze Memory Limit!";

const ANALYZE_WORKER_PANIC_MESSAGE: &str = "analyze worker panic";
const ANALYZE_OOM_MESSAGE: &str =
    "analyze panic due to memory quota exceeds, please try with smaller samplerate(refer to 110000/count)";

/// The recoverable value passed to Go's `getAnalyzePanicErr` helper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnalyzePanicValue {
    /// A panic carrying a plain string.
    Text(String),
    /// A panic carrying an error and its rendered message.
    Error(String),
    /// Any non-string, non-error panic value.
    Other,
}

/// The source error selected for an analyze-worker panic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnalyzePanicError {
    /// The source `errAnalyzeWorkerPanic` sentinel.
    WorkerPanic,
    /// The source `errAnalyzeOOM` sentinel.
    OutOfMemory,
    /// A non-sentinel error returned unchanged by the source helper.
    Propagated(String),
}

impl AnalyzePanicError {
    /// Returns the source-rendered sentinel message.
    #[must_use]
    pub const fn message(&self) -> &'static str {
        match self {
            Self::WorkerPanic => ANALYZE_WORKER_PANIC_MESSAGE,
            Self::OutOfMemory => ANALYZE_OOM_MESSAGE,
            Self::Propagated(_) => "",
        }
    }

    /// Returns the rendered message, including a propagated error message.
    #[must_use]
    pub fn rendered_message(&self) -> &str {
        match self {
            Self::WorkerPanic | Self::OutOfMemory => self.message(),
            Self::Propagated(message) => message,
        }
    }
}

/// Maps one recovered value using the source `getAnalyzePanicErr` branches.
#[must_use]
pub fn get_analyze_panic_error(value: AnalyzePanicValue) -> AnalyzePanicError {
    match value {
        AnalyzePanicValue::Text(message) => {
            if message == GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED {
                AnalyzePanicError::OutOfMemory
            } else {
                AnalyzePanicError::WorkerPanic
            }
        }
        AnalyzePanicValue::Error(message) => {
            if message == GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED {
                AnalyzePanicError::OutOfMemory
            } else {
                AnalyzePanicError::Propagated(message)
            }
        }
        AnalyzePanicValue::Other => AnalyzePanicError::WorkerPanic,
    }
}

/// Reports whether an error is one of the two source analyze-worker sentinels.
#[must_use]
pub const fn is_analyze_worker_panic(error: &AnalyzePanicError) -> bool {
    matches!(
        error,
        AnalyzePanicError::WorkerPanic | AnalyzePanicError::OutOfMemory
    )
}

/// Runs one synchronous analyze phase under the source panic boundary.
///
/// The returned value may itself be a `Result`; this guard owns only panic
/// recovery and leaves ordinary error types to the caller that already owns
/// them.
pub fn recover_analyze_panic<T>(operation: impl FnOnce() -> T) -> Result<T, AnalyzePanicError> {
    catch_unwind(AssertUnwindSafe(operation))
        .map_err(|payload| get_analyze_panic_error(panic_value(payload.as_ref())))
}

fn panic_value(payload: &(dyn Any + Send)) -> AnalyzePanicValue {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return AnalyzePanicValue::Text((*message).to_owned());
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return AnalyzePanicValue::Text(message.clone());
    }
    if let Some(value) = payload.downcast_ref::<AnalyzePanicValue>() {
        return value.clone();
    }
    AnalyzePanicValue::Other
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::panic_any;

    #[test]
    fn recovery_decodes_source_shaped_panic_payloads() {
        let out_of_memory = recover_analyze_panic(|| -> () {
            panic!("{GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED}");
        })
        .expect_err("the source memory sentinel is a panic");
        assert_eq!(out_of_memory, AnalyzePanicError::OutOfMemory);

        let propagated = recover_analyze_panic(|| -> () {
            panic_any(AnalyzePanicValue::Error(
                "wrapped analyze failure".to_owned(),
            ));
        })
        .expect_err("an error payload is a panic");
        assert_eq!(
            propagated,
            AnalyzePanicError::Propagated("wrapped analyze failure".to_owned())
        );

        let non_string = recover_analyze_panic(|| -> () { panic_any(7_u64) })
            .expect_err("a non-string payload is a panic");
        assert_eq!(non_string, AnalyzePanicError::WorkerPanic);
    }
}
