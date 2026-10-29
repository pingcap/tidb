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
// See the License for the specific language governing permissions and
// limitations under the License.


//! Go `pkg/types/field_type.go`: the functions over `parser/types.FieldType`
//! (`type FieldType = types.FieldType`), and the collator its collation
//! names (Go `collate.GetCollator(ft.GetCollate())`).

mod aggregate;
mod value;

pub use aggregate::{agg_field_type, aggregate_eval_type, merge_field_type, set_type_flag};
pub use tidb_parser_types::field_type::*;
pub use value::{
    default_field_type_for_value, infer_param_type_from_datum, parser_default_field_type_for_value,
    FieldTypeValue,
};

/// Go `collate.GetCollator(ft.GetCollate())`: the collator a field type's
/// collation names.
pub trait RuntimeCollator {
    /// Resolves the exact source spelling through the process collation mode.
    fn runtime_collator(&self) -> crate::Collator;

    /// Resolves the exact source spelling under a caller-captured mode.
    fn runtime_collator_with_mode(&self, use_new_collation: bool) -> crate::Collator;
}

impl RuntimeCollator for FieldType {
    fn runtime_collator(&self) -> crate::Collator {
        crate::get_collator(self.collation_name())
    }

    fn runtime_collator_with_mode(&self, use_new_collation: bool) -> crate::Collator {
        crate::get_collator_with_mode(use_new_collation, self.collation_name())
    }
}

#[cfg(test)]
mod tests {
    use super::{default_field_type_for_value, FieldTypeCode, FieldTypeValue, UNSPECIFIED_LENGTH};

    #[test]
    fn default_float_type_width_uses_go_infinity_spelling() {
        let float32 = default_field_type_for_value(
            FieldTypeValue::Float32(f32::INFINITY),
            "utf8mb4",
            "utf8mb4_bin",
        );
        assert_eq!(float32.code(), FieldTypeCode::Float);
        assert_eq!(float32.flen(), 4);

        let float64 = default_field_type_for_value(
            FieldTypeValue::Float64(f64::INFINITY),
            "utf8mb4",
            "utf8mb4_bin",
        );
        assert_eq!(float64.code(), FieldTypeCode::Double);
        assert_eq!(float64.flen(), 4);

        for value in [f32::NEG_INFINITY, f32::NAN] {
            let field_type = default_field_type_for_value(
                FieldTypeValue::Float32(value),
                "utf8mb4",
                "utf8mb4_bin",
            );
            assert_eq!(field_type.flen(), if value.is_nan() { 3 } else { 4 });
        }
    }
}
