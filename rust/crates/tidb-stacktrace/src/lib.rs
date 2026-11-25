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

//! Captures the native caller stack of a TiDB error and resolves it on demand.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt;

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[allow(unsafe_code)]
mod darwin;

/// A captured native stack. Capturing does not resolve addresses to symbols.
pub struct StackTrace(Inner);

enum Inner {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    Darwin(darwin::Frames),
    Portable(Backtrace),
}

impl StackTrace {
    /// Captures the caller's stack regardless of the backtrace environment.
    pub fn capture() -> Self {
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        if let Some(frames) = darwin::Frames::capture() {
            return Self(Inner::Darwin(frames));
        }

        Self(Inner::Portable(Backtrace::force_capture()))
    }

    /// Reports whether the capture found a usable native stack.
    pub fn status(&self) -> BacktraceStatus {
        match &self.0 {
            #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
            Inner::Darwin(_) => BacktraceStatus::Captured,
            Inner::Portable(trace) => trace.status(),
        }
    }
}

impl fmt::Display for StackTrace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
            Inner::Darwin(frames) => frames.fmt(formatter, backtrace::PrintFmt::Short),
            Inner::Portable(trace) => fmt::Display::fmt(trace, formatter),
        }
    }
}

impl fmt::Debug for StackTrace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
            Inner::Darwin(frames) => frames.fmt(formatter, backtrace::PrintFmt::Full),
            Inner::Portable(trace) => fmt::Debug::fmt(trace, formatter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[inline(never)]
    fn known_stack_capture_caller() -> StackTrace {
        std::hint::black_box(StackTrace::capture())
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn captures_and_renders_the_calling_function() {
        let trace = known_stack_capture_caller();
        assert_eq!(trace.status(), BacktraceStatus::Captured);
        let rendered = trace.to_string();
        assert!(
            rendered.contains("known_stack_capture_caller"),
            "{rendered}"
        );
    }

    #[test]
    fn portable_stack_can_be_rendered() {
        let native = Backtrace::force_capture();
        let expected = native.to_string();
        let trace = StackTrace(Inner::Portable(native));
        assert_eq!(trace.to_string(), expected);
    }
}
