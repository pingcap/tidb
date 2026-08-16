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

//! Go `errors.go`: the package's sentinel errors.
//!
//! Go declares each sentinel with `errors.New` and wraps it with
//! `fmt.Errorf("%w: ...")`, so callers classify failures with `errors.Is`.
//! [`PlacementErrorKind`] is that sentinel identity and [`PlacementError`]
//! carries it alongside the fully rendered Go message, making
//! [`PlacementError::is`] the exact `errors.Is` equivalent: wrapping only
//! appends explanatory text and never changes the identity.

use std::fmt;

/// The identity of one Go sentinel error value declared in `errors.go`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PlacementErrorKind {
    /// Go `ErrInvalidConstraintFormat`.
    InvalidConstraintFormat,
    /// Go `ErrUnsupportedConstraint`.
    UnsupportedConstraint,
    /// Go `ErrConflictingConstraints`.
    ConflictingConstraints,
    /// Go `ErrInvalidConstraintsMapcnt`.
    InvalidConstraintsMapcnt,
    /// Go `ErrInvalidConstraintsFormat`.
    InvalidConstraintsFormat,
    /// Go `ErrInvalidSurvivalPreferenceFormat`.
    InvalidSurvivalPreferenceFormat,
    /// Go `ErrInvalidConstraintsReplicas`.
    InvalidConstraintsReplicas,
    /// Go `ErrInvalidBundleID`.
    InvalidBundleId,
    /// Go `ErrInvalidBundleIDFormat`.
    InvalidBundleIdFormat,
    /// Go `ErrLeaderReplicasMustOne`.
    LeaderReplicasMustOne,
    /// Go `ErrMissingRoleField`.
    MissingRoleField,
    /// Go `ErrNoRulesToDrop`.
    NoRulesToDrop,
    /// Go `ErrInvalidPlacementOptions`.
    InvalidPlacementOptions,
    /// Go `ErrInvalidConstraintsMappingWrongSeparator`.
    InvalidConstraintsMappingWrongSeparator,
    /// Go `ErrInvalidConstraintsMappingNoColonFound`.
    InvalidConstraintsMappingNoColonFound,
}

impl PlacementErrorKind {
    /// The byte-exact text Go passed to `errors.New`.
    #[must_use]
    pub const fn text(self) -> &'static str {
        match self {
            Self::InvalidConstraintFormat => {
                "label constraint should be in format '{+|-}key=value'"
            }
            Self::UnsupportedConstraint => "unsupported label constraint",
            Self::ConflictingConstraints => "conflicting label constraints",
            Self::InvalidConstraintsMapcnt => {
                "label constraints in map syntax have invalid replicas"
            }
            Self::InvalidConstraintsFormat => "invalid label constraints format",
            Self::InvalidSurvivalPreferenceFormat => {
                "survival preference format should be in format [xxx=yyy, ...]"
            }
            Self::InvalidConstraintsReplicas => "label constraints with invalid REPLICAS",
            Self::InvalidBundleId => "invalid bundle ID",
            Self::InvalidBundleIdFormat => "invalid bundle ID format",
            Self::LeaderReplicasMustOne => "REPLICAS must be 1 if ROLE=leader",
            Self::MissingRoleField => "the ROLE field is not specified",
            Self::NoRulesToDrop => "no rule of such role to drop",
            Self::InvalidPlacementOptions => "invalid placement option",
            Self::InvalidConstraintsMappingWrongSeparator => {
                "mappings use a colon and space (\u{201c}: \u{201d}) to mark each key/value pair"
            }
            Self::InvalidConstraintsMappingNoColonFound => "no colon found",
        }
    }
}

impl fmt::Display for PlacementErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.text())
    }
}

/// One Go `error` produced by this package, retaining its sentinel identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementError {
    kind: PlacementErrorKind,
    message: String,
}

impl PlacementError {
    /// A bare sentinel return, as in Go's `return ErrInvalidPlacementOptions`.
    #[must_use]
    pub fn new(kind: PlacementErrorKind) -> Self {
        Self {
            kind,
            message: kind.text().to_owned(),
        }
    }

    /// Go `fmt.Errorf("%w: <detail>", sentinel)`.
    #[must_use]
    pub fn wrap(kind: PlacementErrorKind, detail: impl fmt::Display) -> Self {
        Self {
            kind,
            message: format!("{}: {detail}", kind.text()),
        }
    }

    /// Go `fmt.Errorf("%w: <detail>", err)`, keeping `err`'s sentinel identity.
    #[must_use]
    pub fn wrapping(mut self, detail: impl fmt::Display) -> Self {
        self.message = format!("{}: {detail}", self.message);
        self
    }

    /// The wrapped sentinel's identity.
    #[must_use]
    pub const fn kind(&self) -> PlacementErrorKind {
        self.kind
    }

    /// Go `errors.Is(err, sentinel)`.
    #[must_use]
    pub fn is(&self, kind: PlacementErrorKind) -> bool {
        self.kind == kind
    }
}

impl fmt::Display for PlacementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PlacementError {}
