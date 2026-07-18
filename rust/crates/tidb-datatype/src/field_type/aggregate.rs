// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::{FieldType, FieldTypeCode, FieldTypeFlags};
use crate::EvalType;

/// Exact `fieldTypeMergeRules` from `pkg/types/field_type.go`.
const MERGE_RULES: [[u8; 29]; 29] = [
    [
        246, 246, 246, 246, 5, 5, 246, 15, 0, 0, 15, 15, 15, 15, 15, 15, 15, 15, 246, 15, 15, 249,
        250, 251, 252, 15, 254, 15, 15,
    ],
    [
        246, 1, 2, 3, 4, 5, 1, 15, 8, 9, 15, 15, 15, 1, 15, 15, 8, 15, 246, 15, 15, 249, 250, 251,
        252, 15, 254, 15, 15,
    ],
    [
        246, 2, 2, 3, 4, 5, 2, 15, 8, 9, 15, 15, 15, 2, 15, 15, 8, 15, 246, 15, 15, 249, 250, 251,
        252, 15, 254, 15, 15,
    ],
    [
        246, 3, 3, 3, 5, 5, 3, 15, 8, 3, 15, 15, 15, 3, 15, 15, 8, 15, 246, 15, 15, 249, 250, 251,
        252, 15, 254, 15, 15,
    ],
    [
        5, 4, 4, 5, 4, 5, 4, 15, 4, 4, 15, 15, 15, 4, 15, 15, 5, 15, 5, 15, 15, 249, 250, 251, 252,
        15, 254, 15, 15,
    ],
    [
        5, 5, 5, 5, 5, 5, 5, 15, 5, 5, 15, 15, 15, 5, 15, 15, 5, 15, 5, 15, 15, 249, 250, 251, 252,
        15, 254, 15, 15,
    ],
    [
        246, 1, 2, 3, 4, 5, 6, 7, 8, 8, 10, 11, 12, 13, 14, 15, 16, 245, 246, 247, 248, 249, 250,
        251, 252, 15, 254, 255, 225,
    ],
    [
        15, 15, 15, 15, 15, 15, 7, 7, 15, 15, 12, 12, 12, 15, 14, 15, 15, 15, 15, 15, 15, 249, 250,
        251, 252, 15, 254, 15, 15,
    ],
    [
        246, 8, 8, 8, 5, 5, 8, 15, 8, 3, 15, 15, 15, 8, 14, 15, 8, 15, 246, 15, 15, 249, 250, 251,
        252, 15, 254, 15, 15,
    ],
    [
        246, 9, 9, 3, 4, 5, 9, 15, 8, 9, 15, 15, 15, 9, 14, 15, 8, 15, 246, 15, 15, 249, 250, 251,
        252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 10, 12, 15, 15, 10, 12, 12, 15, 14, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 11, 12, 15, 15, 12, 11, 12, 15, 14, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 12, 12, 15, 15, 12, 12, 12, 15, 14, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 254, 15, 15,
    ],
    [
        0, 1, 2, 3, 4, 5, 13, 15, 8, 9, 15, 15, 15, 13, 15, 15, 8, 15, 246, 15, 15, 249, 250, 251,
        252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 14, 12, 15, 15, 14, 12, 12, 15, 14, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 15, 15, 15,
    ],
    [
        15, 8, 8, 8, 5, 5, 16, 15, 8, 8, 15, 15, 15, 8, 15, 15, 16, 15, 246, 15, 15, 249, 250, 251,
        252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 245, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 245, 15, 15, 15, 251,
        251, 251, 251, 15, 254, 15, 15,
    ],
    [
        246, 246, 246, 246, 5, 5, 246, 15, 246, 246, 15, 15, 15, 246, 15, 15, 246, 15, 246, 15, 15,
        249, 250, 251, 252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 247, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 254, 15, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 248, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 254, 15, 15,
    ],
    [
        249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 249, 251,
        249, 249, 249, 249, 250, 251, 252, 249, 249, 249, 251,
    ],
    [
        250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 251,
        250, 250, 250, 250, 250, 251, 250, 250, 250, 250, 251,
    ],
    [
        251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251,
        251, 251, 251, 251, 251, 251, 251, 251, 251, 251, 251,
    ],
    [
        252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 252, 251,
        252, 252, 252, 252, 250, 251, 252, 252, 252, 252, 251,
    ],
    [
        15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 15, 15, 15,
    ],
    [
        254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 15, 254, 254,
        254, 254, 254, 249, 250, 251, 252, 15, 254, 254, 254,
    ],
    [
        15, 15, 15, 15, 15, 15, 255, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 249,
        250, 251, 252, 15, 254, 255, 15,
    ],
    [
        15, 15, 15, 15, 15, 15, 225, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 251,
        251, 251, 251, 15, 254, 15, 225,
    ],
];

const fn type_index(code: FieldTypeCode) -> usize {
    match code {
        FieldTypeCode::Unspecified => 0,
        FieldTypeCode::Tiny => 1,
        FieldTypeCode::Short => 2,
        FieldTypeCode::Long => 3,
        FieldTypeCode::Float => 4,
        FieldTypeCode::Double => 5,
        FieldTypeCode::Null => 6,
        FieldTypeCode::Timestamp => 7,
        FieldTypeCode::LongLong => 8,
        FieldTypeCode::Int24 => 9,
        FieldTypeCode::Date => 10,
        FieldTypeCode::Duration => 11,
        FieldTypeCode::Datetime => 12,
        FieldTypeCode::Year => 13,
        FieldTypeCode::NewDate => 14,
        FieldTypeCode::Varchar => 15,
        FieldTypeCode::Bit => 16,
        FieldTypeCode::Json => 17,
        FieldTypeCode::NewDecimal => 18,
        FieldTypeCode::Enum => 19,
        FieldTypeCode::Set => 20,
        FieldTypeCode::TinyBlob => 21,
        FieldTypeCode::MediumBlob => 22,
        FieldTypeCode::LongBlob => 23,
        FieldTypeCode::Blob => 24,
        FieldTypeCode::VarString => 25,
        FieldTypeCode::String => 26,
        FieldTypeCode::Geometry => 27,
        FieldTypeCode::VectorFloat32 => 28,
        // Go's `fieldTypeIndexes[tp]` is a map lookup without an `ok` check,
        // so every unregistered byte uses the zero-value index.
        FieldTypeCode::Unknown(_) => 0,
    }
}

/// Exact table lookup used by Go `mergeFieldType`.
pub const fn merge_field_type(left: FieldTypeCode, right: FieldTypeCode) -> FieldTypeCode {
    FieldTypeCode::from_mysql_type(MERGE_RULES[type_index(left)][type_index(right)])
}

const fn merge_type_flags(left: u32, right: u32) -> u32 {
    left & (right & FieldTypeFlags::NOT_NULL | !FieldTypeFlags::NOT_NULL)
        & (right & FieldTypeFlags::UNSIGNED | !FieldTypeFlags::UNSIGNED)
}

/// Exact `AggFieldType`, including mixed-sign integral promotion.
pub fn agg_field_type(types: &[FieldType]) -> FieldType {
    let Some(first) = types.first() else {
        return FieldType::parser(FieldTypeCode::Unspecified)
            .with_flen(0)
            .with_decimal(0);
    };
    let mut current = first.clone();
    let mut mixed_sign = false;
    for next in &types[1..] {
        mixed_sign |= current.is_unsigned() != next.is_unsigned();
        current.set_code(merge_field_type(current.code(), next.code()));
        let merged_flags = merge_type_flags(current.flags(), next.flags());
        current = current.with_flags(merged_flags);
    }
    if mixed_sign && current.code().is_type_integer() {
        let bumps_range = types.iter().any(|field_type| {
            field_type.is_unsigned()
                && (field_type.code() == current.code() || field_type.code() == FieldTypeCode::Bit)
        });
        if bumps_range {
            current.set_code(match current.code() {
                FieldTypeCode::Tiny => FieldTypeCode::Short,
                FieldTypeCode::Short => FieldTypeCode::Int24,
                FieldTypeCode::Int24 => FieldTypeCode::Long,
                FieldTypeCode::Long => FieldTypeCode::LongLong,
                FieldTypeCode::LongLong => FieldTypeCode::NewDecimal,
                other => other,
            });
        }
    }
    if current.is_unsigned() && !mixed_sign {
        current = current.with_added_flags(FieldTypeFlags::UNSIGNED);
    }
    current
}

/// Sets or clears a source type flag.
pub const fn set_type_flag(flags: &mut u32, item: u32, on: bool) {
    if on {
        *flags |= item
    } else {
        *flags &= !item
    }
}

/// Exact `AggregateEvalType` merge and output-flag behavior.
pub fn aggregate_eval_type(types: &[FieldType], flags: &mut u32) -> EvalType {
    let mut aggregate = EvalType::String;
    let mut unsigned = false;
    let mut first = false;
    let mut binary_string = false;
    let mut left = types
        .first()
        .expect("AggregateEvalType requires an argument");
    for field_type in types {
        if field_type.code() == FieldTypeCode::Null {
            continue;
        }
        let right_eval = field_type.eval_type();
        if (field_type.code().is_type_blob()
            || field_type.code().is_type_varchar()
            || field_type.code().is_type_char())
            && field_type.has_flag(FieldTypeFlags::BINARY)
        {
            binary_string = true;
        }
        if !first {
            first = true;
            aggregate = right_eval;
            unsigned = field_type.is_unsigned();
        } else {
            aggregate = merge_eval_type(
                aggregate,
                right_eval,
                left,
                field_type,
                unsigned,
                field_type.is_unsigned(),
            );
            unsigned &= field_type.is_unsigned();
        }
        left = field_type;
    }
    set_type_flag(flags, FieldTypeFlags::UNSIGNED, unsigned);
    set_type_flag(
        flags,
        FieldTypeFlags::BINARY,
        !aggregate.is_string_kind() || binary_string,
    );
    aggregate
}

fn merge_eval_type(
    mut left_eval: EvalType,
    mut right_eval: EvalType,
    left: &FieldType,
    right: &FieldType,
    left_unsigned: bool,
    right_unsigned: bool,
) -> EvalType {
    if left.code() == FieldTypeCode::Unspecified || right.code() == FieldTypeCode::Unspecified {
        if left.code() == right.code() {
            return EvalType::String;
        }
        if left.code() == FieldTypeCode::Unspecified {
            left_eval = right_eval;
        } else {
            right_eval = left_eval;
        }
    }
    if left_eval.is_string_kind() || right_eval.is_string_kind() {
        EvalType::String
    } else if left_eval == EvalType::Real || right_eval == EvalType::Real {
        EvalType::Real
    } else if left_eval == EvalType::Decimal
        || right_eval == EvalType::Decimal
        || left_unsigned != right_unsigned
    {
        EvalType::Decimal
    } else {
        EvalType::Int
    }
}
