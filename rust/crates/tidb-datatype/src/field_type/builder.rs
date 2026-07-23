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

use super::{FieldType, FieldTypeCode};

/// Fluent constructor corresponding to Go `FieldTypeBuilder`.
#[derive(Clone, Debug)]
pub struct FieldTypeBuilder {
    field_type: FieldType,
}

impl Default for FieldTypeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldTypeBuilder {
    /// Creates a builder around the source zero-value field type.
    pub fn new() -> Self {
        Self {
            field_type: FieldType::parser(FieldTypeCode::Unspecified),
        }
    }

    /// Returns the effective MySQL type code.
    pub const fn code(&self) -> FieldTypeCode {
        self.field_type.code()
    }

    /// Returns the raw flag mask.
    pub const fn flags(&self) -> u32 {
        self.field_type.flags()
    }

    /// Returns the display length.
    pub const fn flen(&self) -> i64 {
        self.field_type.flen()
    }

    /// Returns the decimal scale.
    pub const fn decimal(&self) -> i64 {
        self.field_type.decimal()
    }

    /// Returns the character-set spelling.
    pub fn charset(&self) -> &str {
        self.field_type.charset_name()
    }

    /// Returns the collation spelling.
    pub fn collation(&self) -> &str {
        self.field_type.collation_name()
    }

    /// Replaces the type and clears ARRAY state.
    pub fn with_code(mut self, code: FieldTypeCode) -> Self {
        self.field_type.set_code(code);
        self
    }

    /// Replaces all flags.
    pub fn flags_set(mut self, flags: u32) -> Self {
        self.field_type = self.field_type.with_flags(flags);
        self
    }

    /// Adds flags.
    pub fn add_flags(mut self, flags: u32) -> Self {
        self.field_type = self.field_type.with_added_flags(flags);
        self
    }

    /// Toggles flags.
    pub fn toggle_flags(mut self, flags: u32) -> Self {
        self.field_type = self.field_type.with_toggled_flags(flags);
        self
    }

    /// Removes flags.
    pub fn remove_flags(mut self, flags: u32) -> Self {
        self.field_type = self.field_type.with_removed_flags(flags);
        self
    }

    /// Replaces the display length.
    pub fn flen_set(mut self, flen: i64) -> Self {
        self.field_type = self.field_type.with_flen(flen);
        self
    }

    /// Replaces the decimal scale.
    pub fn decimal_set(mut self, decimal: i64) -> Self {
        self.field_type = self.field_type.with_decimal(decimal);
        self
    }

    /// Replaces the character-set spelling.
    pub fn charset_set(mut self, charset: impl Into<String>) -> Self {
        self.field_type = self.field_type.with_charset_name(charset);
        self
    }

    /// Replaces the collation spelling.
    pub fn collation_set(mut self, collation: impl Into<String>) -> Self {
        self.field_type = self.field_type.with_collation_name(collation);
        self
    }

    /// Replaces ENUM/SET elements.
    pub fn elems(mut self, elems: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.field_type = self.field_type.with_elems(elems);
        self
    }

    /// Sets ARRAY state.
    pub fn array(mut self, array: bool) -> Self {
        self.field_type = self.field_type.with_array(array);
        self
    }

    /// Finishes construction.
    pub fn build(self) -> FieldType {
        self.field_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldTypeFlags;

    #[test]
    fn source_builder_operations_preserve_order_and_bit_semantics() {
        let builder = FieldTypeBuilder::new()
            .with_code(FieldTypeCode::NewDecimal)
            .flags_set(FieldTypeFlags::BINARY)
            .add_flags(FieldTypeFlags::UNSIGNED)
            .toggle_flags(FieldTypeFlags::BINARY)
            .remove_flags(FieldTypeFlags::ZEROFILL)
            .flen_set(12)
            .decimal_set(3)
            .charset_set("utf8mb4")
            .collation_set("utf8mb4_bin")
            .elems(["a", "b"])
            .array(false);
        assert_eq!(builder.code(), FieldTypeCode::NewDecimal);
        assert_eq!(builder.flags(), FieldTypeFlags::UNSIGNED);
        assert_eq!(builder.flen(), 12);
        assert_eq!(builder.decimal(), 3);
        assert_eq!(builder.charset(), "utf8mb4");
        assert_eq!(builder.collation(), "utf8mb4_bin");
        let field_type = builder.build();
        assert_eq!(field_type.elems(), ["a", "b"]);
    }
}
