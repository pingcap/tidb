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

//! String-backed context-key metadata from `pkg/sessionctx/context.go`.
//!
//! TiDB keeps three process-local context keys for query text, bootstrap, and
//! the last DDL statement. The Go type is intentionally an integer so code
//! can still format an unknown key as `"unknown"`; this Rust value preserves
//! that same open-ended representation without claiming ownership of the
//! session context itself.

/// A TiDB session-context key value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ContextKey(isize);

impl ContextKey {
    /// Key for the original query string.
    pub const QUERY_STRING: Self = Self(1);
    /// Key indicating that the server is running bootstrap or upgrade work.
    pub const INITING: Self = Self(2);
    /// Key indicating that the session last executed a DDL statement.
    pub const LAST_EXECUTE_DDL: Self = Self(3);

    /// Creates a key from the source integer representation.
    #[must_use]
    pub const fn new(value: isize) -> Self {
        Self(value)
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn value(self) -> isize {
        self.0
    }

    /// Returns the source-compatible display label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::QUERY_STRING => "query_string",
            Self::INITING => "initing",
            Self::LAST_EXECUTE_DDL => "last_execute_ddl",
            Self(_) => "unknown",
        }
    }
}

impl std::fmt::Display for ContextKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.label())
    }
}
