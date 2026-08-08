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

//! Source storage-width and allocation accounting for [`FieldType`].

use super::{FieldType, FieldTypeCode, VAR_STORAGE_LEN};
use crate::GoString;

impl FieldType {
    /// Returns the source storage-width estimate.
    pub fn storage_length(&self) -> i64 {
        match self.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Double
            | FieldTypeCode::Float
            | FieldTypeCode::Year
            | FieldTypeCode::Duration
            | FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp
            | FieldTypeCode::Enum
            | FieldTypeCode::Set
            | FieldTypeCode::Bit => 8,
            FieldTypeCode::NewDecimal => {
                const DIGITS_TO_BYTES: [i64; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
                let integer = self.flen - self.decimal;
                integer / 9 * 4
                    + DIGITS_TO_BYTES[(integer % 9) as usize]
                    + self.decimal / 9 * 4
                    + DIGITS_TO_BYTES[(self.decimal % 9) as usize]
            }
            _ => VAR_STORAGE_LEN,
        }
    }

    /// Mirrors Go `FieldType.MemoryUsage` on supported 64-bit targets.
    pub fn memory_usage(&self) -> usize {
        const GO_EMPTY_FIELD_TYPE_SIZE: usize = 120;
        GO_EMPTY_FIELD_TYPE_SIZE
            + self.charset_name.len()
            + self.collation_name.len()
            + self.elems.capacity() * 16
            + self
                .elems
                .with_visible(|elements| elements.iter().map(GoString::len).sum::<usize>())
            + self.elems_is_binary_literal.capacity() * std::mem::size_of::<bool>()
    }
}
