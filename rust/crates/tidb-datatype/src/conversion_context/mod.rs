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

//! Datatype conversion context translated from `pkg/types/context.go`.

mod flags;

use std::borrow::Cow;
use std::fmt;

use tidb_error::terror::TerrorError;

pub use flags::{ConversionFlags, DEFAULT_STATEMENT_FLAGS, STRICT_FLAGS};

/// Opaque source `time.Location` identity carried by datatype conversion.
///
/// `pkg/types/context.go` only stores and returns the location; timezone rule
/// evaluation belongs to temporal conversion consumers. Retaining its source
/// name avoids inventing a second timezone database in this dependency leaf.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConversionLocation(Cow<'static, str>);

impl ConversionLocation {
    /// Source `time.UTC`.
    pub const UTC: Self = Self(Cow::Borrowed("UTC"));

    /// Creates an opaque named location identity.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self(Cow::Owned(name.into()))
    }

    /// Returns the source location name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.0
    }
}

/// Capability required by [`ConversionContext`] to publish a typed warning.
///
/// This is an input port, not warning storage. Executor/session warning
/// handlers implement it and remain the sole owners of warning ordering,
/// caps, levels, snapshots, and publication.
pub trait ConversionWarningAppender {
    /// Appends one original generated conversion error as a warning.
    fn append_conversion_warning(&self, warning: TerrorError);
}

/// Source `contextutil.IgnoreWarn` for dependency-leaf contexts.
#[derive(Clone, Copy, Debug, Default)]
pub struct IgnoreConversionWarnings;

impl ConversionWarningAppender for IgnoreConversionWarnings {
    fn append_conversion_warning(&self, _warning: TerrorError) {}
}

/// Shared no-op warning sink used by strict/default dependency-leaf contexts.
pub static IGNORE_CONVERSION_WARNINGS: IgnoreConversionWarnings = IgnoreConversionWarnings;

/// Information carried while converting between datatype representations.
#[derive(Clone)]
pub struct ConversionContext<'a> {
    flags: ConversionFlags,
    location: ConversionLocation,
    warning_appender: &'a dyn ConversionWarningAppender,
}

impl<'a> ConversionContext<'a> {
    /// Source `NewContext`. Rust references eliminate nil location/handler
    /// states instead of reproducing Go's defensive nil branches.
    #[must_use]
    pub fn new(
        flags: ConversionFlags,
        location: ConversionLocation,
        warning_appender: &'a dyn ConversionWarningAppender,
    ) -> Self {
        Self {
            flags,
            location,
            warning_appender,
        }
    }

    /// Returns the conversion flags.
    #[must_use]
    pub const fn flags(&self) -> ConversionFlags {
        self.flags
    }

    /// Returns a copy with new flags, preserving location and warning sink.
    #[must_use]
    pub fn with_flags(&self, flags: ConversionFlags) -> Self {
        Self {
            flags,
            location: self.location.clone(),
            warning_appender: self.warning_appender,
        }
    }

    /// Returns a copy with a new location, preserving flags and warning sink.
    #[must_use]
    pub fn with_location(&self, location: ConversionLocation) -> Self {
        Self {
            flags: self.flags,
            location,
            warning_appender: self.warning_appender,
        }
    }

    /// Returns the opaque source location identity.
    #[must_use]
    pub const fn location(&self) -> &ConversionLocation {
        &self.location
    }

    /// Publishes one warning through the existing caller-owned sink.
    pub fn append_warning(&self, warning: TerrorError) {
        self.warning_appender.append_conversion_warning(warning);
    }
}

impl ConversionContext<'static> {
    /// Returns the strict source context with UTC and the no-op sink.
    #[must_use]
    pub fn strict() -> Self {
        Self::new(
            STRICT_FLAGS,
            ConversionLocation::UTC,
            &IGNORE_CONVERSION_WARNINGS,
        )
    }

    /// Returns `DefaultStmtNoWarningContext`.
    #[must_use]
    pub fn default_statement_no_warning() -> Self {
        Self::new(
            DEFAULT_STATEMENT_FLAGS,
            ConversionLocation::UTC,
            &IGNORE_CONVERSION_WARNINGS,
        )
    }
}

impl fmt::Debug for ConversionContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConversionContext")
            .field("flags", &self.flags)
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}
