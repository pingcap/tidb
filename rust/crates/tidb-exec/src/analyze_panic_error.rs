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

//! Analyze-worker panic classification from `pkg/executor/analyze_utils.go`.
//!
//! The Go worker recovers an `any` value. This leaf keeps that dynamic input
//! explicit instead of guessing an executor-wide error type: text panics are
//! matched against the source analyze-memory sentinel, error panics preserve
//! their message unless they carry that sentinel, and every other value maps
//! to the source worker-panic error. Recovery, logging, goroutine lifecycle,
//! and analyze retry behavior remain outside this dependency-closed seam.

/// The exact panic text emitted by the source global analyze-memory tracker.
pub const GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED: &str = "Out Of Global Analyze Memory Limit!";

const ANALYZE_WORKER_PANIC_MESSAGE: &str = "analyze worker panic";
const ANALYZE_OOM_MESSAGE: &str =
    "analyze panic due to memory quota exceeds, please try with smaller samplerate(refer to 110000/count)";

/// The recoverable value passed to the source `getAnalyzePanicErr` helper.
///
/// Go's `any` can contain a string, an error, or any other value. An error is
/// represented by its already-rendered message here; preserving a concrete
/// error implementation would pull error-wrapper semantics into this leaf.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnalyzePanicValue {
    /// A panic carrying a plain Go string.
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
