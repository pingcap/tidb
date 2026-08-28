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

//! Shared enums and errors for Go's physical table-reader operator.
//!
//! The operator itself is [`crate::physical::PhysicalTableReader`]. Keeping a
//! second metadata-only `PhysicalTableReaderPlan` here caused bounded scans to
//! bypass the real physical tree, so that facade has been removed.

/// Opaque storage kind copied by the source Clone implementation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StoreType {
    /// TiKV storage.
    #[default]
    TiKv,
    /// TiFlash storage.
    TiFlash,
    /// Unknown storage values remain opaque.
    Unknown(u8),
}

/// Go `physicalop.ReadReqType`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadReqType {
    /// Read through a regular coprocessor request.
    #[default]
    Cop,
    /// TiFlash batch-coprocessor request.
    BatchCop,
    /// TiFlash MPP request.
    Mpp,
    /// Unknown values use Go's default (`cop`) name.
    Unknown(u8),
}

impl ReadReqType {
    /// Go `ReadReqType.Name`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::BatchCop => "batchCop",
            Self::Mpp => "mpp",
            Self::Cop | Self::Unknown(_) => "cop",
        }
    }
}

/// Error returned when a raw DAG scan is used as a planner TableReader child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MissingTableDescriptorError;

impl std::fmt::Display for MissingTableDescriptorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("table reader requires a source-resolved table descriptor")
    }
}

impl std::error::Error for MissingTableDescriptorError {}

/// Error returned when a reader does not contain exactly one table scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableScanCountError {
    actual: usize,
}

impl TableScanCountError {
    /// Returns the observed number of table scans.
    #[must_use]
    pub const fn actual(self) -> usize {
        self.actual
    }

    pub(crate) const fn new(actual: usize) -> Self {
        Self { actual }
    }
}

impl std::fmt::Display for TableScanCountError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the count of table scan != 1")
    }
}

impl std::error::Error for TableScanCountError {}

#[cfg(test)]
mod tests {
    use super::ReadReqType;

    #[test]
    fn request_type_names_match_go_defaults() {
        assert_eq!(ReadReqType::Cop.name(), "cop");
        assert_eq!(ReadReqType::BatchCop.name(), "batchCop");
        assert_eq!(ReadReqType::Mpp.name(), "mpp");
        assert_eq!(ReadReqType::Unknown(9).name(), "cop");
    }
}
