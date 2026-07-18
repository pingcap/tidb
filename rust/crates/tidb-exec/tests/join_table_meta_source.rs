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

//! Source-backed tests for hash-join row metadata decisions.

use tidb_exec::join_table_meta::{ColumnType, JoinTableMeta, KeyMode, SerializeMode};

fn meta(
    keys: &[usize],
    build: &[ColumnType],
    build_keys: &[ColumnType],
    probe_keys: &[ColumnType],
    other: Option<&[usize]>,
    output: Option<&[usize]>,
    used_flag: bool,
) -> JoinTableMeta {
    JoinTableMeta::new(
        keys, build, build_keys, probe_keys, other, output, used_flag,
    )
}

#[test]
fn test_join_table_meta_key_mode_source() {
    // Source: pkg/executor/join/join_table_meta_test.go:27-87 (TestJoinTableMetaKeyMode).
    let cases = [
        (
            vec![0],
            vec![ColumnType::Int],
            vec![ColumnType::Int],
            vec![ColumnType::Int],
            KeyMode::OneInt64,
        ),
        (
            vec![0],
            vec![ColumnType::Year],
            vec![ColumnType::Year],
            vec![ColumnType::Year],
            KeyMode::OneInt64,
        ),
        (
            vec![0],
            vec![ColumnType::Duration],
            vec![ColumnType::Duration],
            vec![ColumnType::Duration],
            KeyMode::OneInt64,
        ),
        (
            vec![0],
            vec![ColumnType::Bit],
            vec![ColumnType::Bit],
            vec![ColumnType::Bit],
            KeyMode::OneInt64,
        ),
        (
            vec![0],
            vec![ColumnType::DateTime],
            vec![ColumnType::DateTime],
            vec![ColumnType::DateTime],
            KeyMode::OneInt64,
        ),
        (
            vec![0],
            vec![ColumnType::EnumInt],
            vec![ColumnType::EnumInt],
            vec![ColumnType::EnumInt],
            KeyMode::OneInt64,
        ),
        (
            vec![0],
            vec![ColumnType::Int],
            vec![ColumnType::Int],
            vec![ColumnType::UnsignedInt],
            KeyMode::FixedSerializedKey,
        ),
        (
            vec![0],
            vec![ColumnType::UnsignedInt],
            vec![ColumnType::UnsignedInt],
            vec![ColumnType::Int],
            KeyMode::FixedSerializedKey,
        ),
        (
            vec![0],
            vec![ColumnType::Float],
            vec![ColumnType::Float],
            vec![ColumnType::Float],
            KeyMode::FixedSerializedKey,
        ),
        (
            vec![0, 1],
            vec![ColumnType::DateTime, ColumnType::Int],
            vec![ColumnType::DateTime, ColumnType::Int],
            vec![ColumnType::DateTime, ColumnType::Int],
            KeyMode::FixedSerializedKey,
        ),
        (
            vec![0],
            vec![ColumnType::Decimal],
            vec![ColumnType::Decimal],
            vec![ColumnType::Decimal],
            KeyMode::VariableSerializedKey,
        ),
        (
            vec![0],
            vec![ColumnType::String],
            vec![ColumnType::String],
            vec![ColumnType::String],
            KeyMode::VariableSerializedKey,
        ),
        (
            vec![0],
            vec![ColumnType::Enum],
            vec![ColumnType::Enum],
            vec![ColumnType::Enum],
            KeyMode::VariableSerializedKey,
        ),
        (
            vec![0],
            vec![ColumnType::Set],
            vec![ColumnType::Set],
            vec![ColumnType::Set],
            KeyMode::VariableSerializedKey,
        ),
        (
            vec![0],
            vec![ColumnType::Json],
            vec![ColumnType::Json],
            vec![ColumnType::Json],
            KeyMode::VariableSerializedKey,
        ),
        (
            vec![0, 1],
            vec![ColumnType::Int, ColumnType::String],
            vec![ColumnType::Int, ColumnType::String],
            vec![ColumnType::Int, ColumnType::String],
            KeyMode::VariableSerializedKey,
        ),
    ];
    for (keys, build, build_keys, probe_keys, expected) in cases {
        assert_eq!(
            meta(
                &keys,
                &build,
                &build_keys,
                &probe_keys,
                None,
                Some(&[]),
                false
            )
            .key_mode,
            expected
        );
    }
}

#[test]
fn test_join_table_meta_key_inlined_and_fixed_source() {
    // Source: pkg/executor/join/join_table_meta_test.go:89-159 (TestJoinTableMetaKeyInlinedAndFixed).
    let cases = [
        (ColumnType::Int, true, true, 8),
        (ColumnType::UnsignedInt, true, true, 8),
        (ColumnType::Year, true, true, 8),
        (ColumnType::Duration, true, true, 8),
        (ColumnType::BinaryString, true, false, -1),
        (ColumnType::Bit, false, true, 8),
        (ColumnType::DateTime, false, true, 8),
        (ColumnType::EnumInt, false, true, 8),
        (ColumnType::Float, false, true, 8),
        (ColumnType::Decimal, false, false, -1),
        (ColumnType::Enum, false, false, -1),
        (ColumnType::Set, false, false, -1),
        (ColumnType::String, false, false, -1),
        (ColumnType::Json, false, false, -1),
    ];
    for (kind, inlined, fixed, length) in cases {
        let result = meta(&[0], &[kind], &[kind], &[kind], None, Some(&[]), false);
        assert_eq!(result.is_join_keys_inlined, inlined);
        assert_eq!(result.is_join_keys_fixed_length, fixed);
        assert_eq!(result.join_keys_length, length);
    }
    let mixed = meta(
        &[0],
        &[ColumnType::Int],
        &[ColumnType::Int],
        &[ColumnType::UnsignedInt],
        None,
        Some(&[]),
        false,
    );
    assert!(!mixed.is_join_keys_inlined);
    assert_eq!(mixed.join_keys_length, 9);
}

#[test]
fn test_read_null_map_thread_safe_source() {
    // Source: pkg/executor/join/join_table_meta_test.go:161-173 (TestReadNullMapThreadSafe).
    let with_flag = meta(
        &[0],
        &[ColumnType::Int],
        &[ColumnType::Int],
        &[ColumnType::Int],
        None,
        Some(&[]),
        true,
    );
    for index in 0..100 {
        assert_eq!(with_flag.is_read_null_map_thread_safe(index), index >= 31);
    }
    let without_flag = meta(
        &[0],
        &[ColumnType::Int],
        &[ColumnType::Int],
        &[ColumnType::Int],
        None,
        Some(&[]),
        false,
    );
    for index in 0..100 {
        assert!(without_flag.is_read_null_map_thread_safe(index));
    }
}

#[test]
fn test_join_table_meta_serialized_mode_source() {
    // Source: pkg/executor/join/join_table_meta_test.go:175-225 (TestJoinTableMetaSerializedMode).
    let cases = [
        (
            vec![ColumnType::Decimal, ColumnType::Int],
            vec![ColumnType::Decimal, ColumnType::Int],
            vec![SerializeMode::Normal, SerializeMode::Normal],
        ),
        (
            vec![ColumnType::UnsignedInt, ColumnType::Int],
            vec![ColumnType::Int, ColumnType::Int],
            vec![SerializeMode::NeedSignFlag, SerializeMode::Normal],
        ),
        (
            vec![ColumnType::Int],
            vec![ColumnType::UnsignedInt],
            vec![SerializeMode::NeedSignFlag],
        ),
        (
            vec![ColumnType::Int, ColumnType::BinaryString],
            vec![ColumnType::Int, ColumnType::BinaryString],
            vec![SerializeMode::Normal, SerializeMode::KeepVarColumnLength],
        ),
        (
            vec![ColumnType::BinaryString],
            vec![ColumnType::BinaryString],
            vec![SerializeMode::KeepVarColumnLength],
        ),
        (
            vec![ColumnType::String, ColumnType::BinaryString],
            vec![ColumnType::String, ColumnType::BinaryString],
            vec![
                SerializeMode::KeepVarColumnLength,
                SerializeMode::KeepVarColumnLength,
            ],
        ),
        (
            vec![ColumnType::String, ColumnType::Decimal],
            vec![ColumnType::String, ColumnType::Decimal],
            vec![
                SerializeMode::KeepVarColumnLength,
                SerializeMode::KeepVarColumnLength,
            ],
        ),
        (
            vec![
                ColumnType::Set,
                ColumnType::Json,
                ColumnType::Decimal,
                ColumnType::Enum,
            ],
            vec![
                ColumnType::Set,
                ColumnType::Json,
                ColumnType::Decimal,
                ColumnType::Enum,
            ],
            vec![SerializeMode::KeepVarColumnLength; 4],
        ),
        (
            vec![ColumnType::EnumInt, ColumnType::Enum],
            vec![ColumnType::EnumInt, ColumnType::Enum],
            vec![SerializeMode::Normal, SerializeMode::Normal],
        ),
    ];
    for (build_keys, probe_keys, expected) in cases {
        let keys: Vec<usize> = (0..build_keys.len()).collect();
        let result = meta(
            &keys,
            &build_keys,
            &build_keys,
            &probe_keys,
            None,
            Some(&[]),
            false,
        );
        assert_eq!(result.serialize_modes, expected);
    }
    let mixed = meta(
        &[0, 1],
        &[ColumnType::Int, ColumnType::BinaryString],
        &[ColumnType::Int, ColumnType::BinaryString],
        &[ColumnType::UnsignedInt, ColumnType::BinaryString],
        None,
        Some(&[]),
        false,
    );
    assert_eq!(
        mixed.serialize_modes,
        [SerializeMode::NeedSignFlag, SerializeMode::Normal]
    );
}

#[test]
fn test_join_table_meta_row_columns_order_source() {
    // Source: pkg/executor/join/join_table_meta_test.go:227-272 (TestJoinTableMetaRowColumnsOrder).
    let result = meta(
        &[2],
        &[ColumnType::Int, ColumnType::Int, ColumnType::Int],
        &[ColumnType::Int],
        &[ColumnType::Int],
        None,
        Some(&[0, 1, 2]),
        false,
    );
    assert_eq!(result.row_columns_order, [2, 0, 1]);

    let result = meta(
        &[0],
        &[
            ColumnType::String,
            ColumnType::String,
            ColumnType::DateTime,
            ColumnType::Decimal,
        ],
        &[ColumnType::String],
        &[ColumnType::String],
        Some(&[3, 2]),
        Some(&[]),
        false,
    );
    assert_eq!(result.row_columns_order, [3, 2]);

    let result = meta(
        &[0],
        &[
            ColumnType::String,
            ColumnType::String,
            ColumnType::DateTime,
            ColumnType::Decimal,
            ColumnType::Int,
        ],
        &[ColumnType::String],
        &[ColumnType::String],
        None,
        Some(&[4, 1, 0, 2, 3]),
        false,
    );
    assert_eq!(result.row_columns_order, [4, 1, 0, 2, 3]);
}

#[test]
fn test_join_table_meta_null_map_length_source() {
    // Source: pkg/executor/join/join_table_meta_test.go:274-327 (TestJoinTableMetaNullMapLength).
    let cases = [
        (vec![ColumnType::Int], false, Some(&[][..]), 1),
        (vec![ColumnType::Int; 9], false, None, 2),
        (vec![ColumnType::String], false, Some(&[][..]), 0),
        (vec![ColumnType::Int], true, None, 4),
        (
            vec![ColumnType::String, ColumnType::Int],
            true,
            Some(&[1][..]),
            4,
        ),
        (
            vec![ColumnType::String, ColumnType::Int],
            true,
            Some(&[0][..]),
            4,
        ),
        (
            vec![ColumnType::String, ColumnType::String],
            true,
            Some(&[][..]),
            4,
        ),
    ];
    for (build, used_flag, output, expected) in cases {
        let result = meta(
            &[0],
            &build,
            &[build[0]],
            &[build[0]],
            None,
            output,
            used_flag,
        );
        assert_eq!(result.null_map_length, expected);
    }
}
