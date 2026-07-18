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

//! Dependency-closed hash-join row metadata from
//! `pkg/executor/join/join_table_meta.go`.
//!
//! This leaf ports the metadata decisions that sit immediately before hash
//! join row encoding: key mode, key inlining/fixed width, serializer mode,
//! saved-column order, and null-map alignment/thread-safety. The live TiDB
//! `FieldType`/collation registry, chunk fixed-length ABI, codec bytes,
//! unsafe row pointers, atomic used flags, and hash-join execution remain
//! external.

/// Abstract source field categories needed by the metadata decision table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    /// Signed integer family (including tiny/int/year-like fixed integers).
    Int,
    /// Unsigned integer family.
    UnsignedInt,
    /// Duration, always signed and inlineable.
    Duration,
    /// Year, always unsigned and inlineable.
    Year,
    /// Datetime/date/timestamp serialized as an unsigned fixed key.
    DateTime,
    /// Float/double serialized as a fixed-width non-integer key.
    Float,
    /// Binary string/blob, raw-memory inlineable variable key.
    BinaryString,
    /// Non-binary string key.
    String,
    /// Decimal variable serialized key.
    Decimal,
    /// Enum with its integer representation flag.
    EnumInt,
    /// Textual enum.
    Enum,
    /// Set value.
    Set,
    /// JSON value.
    Json,
    /// Bit is unsigned but never inlineable in the source.
    Bit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct KeyProperty {
    can_be_inlined: bool,
    key_length: Option<usize>,
    is_integer: bool,
    is_unsigned: bool,
}

impl ColumnType {
    fn key_property(self) -> KeyProperty {
        match self {
            Self::Int => KeyProperty {
                can_be_inlined: true,
                key_length: Some(8),
                is_integer: true,
                is_unsigned: false,
            },
            Self::UnsignedInt => KeyProperty {
                can_be_inlined: true,
                key_length: Some(8),
                is_integer: true,
                is_unsigned: true,
            },
            Self::Duration => KeyProperty {
                can_be_inlined: true,
                key_length: Some(8),
                is_integer: true,
                is_unsigned: false,
            },
            Self::Year => KeyProperty {
                can_be_inlined: true,
                key_length: Some(8),
                is_integer: true,
                is_unsigned: true,
            },
            Self::DateTime => KeyProperty {
                can_be_inlined: false,
                key_length: Some(8),
                is_integer: true,
                is_unsigned: true,
            },
            Self::Float => KeyProperty {
                can_be_inlined: false,
                key_length: Some(8),
                is_integer: false,
                is_unsigned: false,
            },
            Self::BinaryString => KeyProperty {
                can_be_inlined: true,
                key_length: None,
                is_integer: false,
                is_unsigned: false,
            },
            Self::String | Self::Decimal | Self::Enum | Self::Set | Self::Json => KeyProperty {
                can_be_inlined: false,
                key_length: None,
                is_integer: false,
                is_unsigned: false,
            },
            Self::EnumInt => KeyProperty {
                can_be_inlined: false,
                key_length: Some(8),
                is_integer: true,
                is_unsigned: true,
            },
            Self::Bit => KeyProperty {
                can_be_inlined: false,
                key_length: Some(8),
                is_integer: true,
                is_unsigned: true,
            },
        }
    }

    fn fixed_row_length(self) -> Option<usize> {
        match self {
            Self::String
            | Self::BinaryString
            | Self::Decimal
            | Self::Enum
            | Self::Set
            | Self::Json => None,
            _ => Some(8),
        }
    }
}

/// Serializer mode for one join key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SerializeMode {
    /// Normal codec encoding.
    Normal,
    /// Mixed signed/unsigned integer encoding needs a sign marker.
    NeedSignFlag,
    /// Variable values retain a length marker to disambiguate key columns.
    KeepVarColumnLength,
}

/// Physical key representation selected for a join table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyMode {
    /// One compatible integer key can be used directly.
    OneInt64,
    /// All serialized key components have fixed widths.
    FixedSerializedKey,
    /// At least one key component has variable width.
    VariableSerializedKey,
}

/// Metadata decisions used by hash-join row layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinTableMeta {
    /// Whether all saved build columns have fixed row width.
    pub is_fixed_length: bool,
    /// Sum of fixed saved-column widths, or zero for variable rows.
    pub row_length: usize,
    /// Whether key bytes can be stored inline in the row.
    pub is_join_keys_inlined: bool,
    /// Whether serialized key bytes have fixed width.
    pub is_join_keys_fixed_length: bool,
    /// Fixed key width, or `-1` for variable keys.
    pub join_keys_length: isize,
    /// Serializer mode for each build key.
    pub serialize_modes: Vec<SerializeMode>,
    /// Saved build-column order in the row layout.
    pub row_columns_order: Vec<usize>,
    /// Number of bytes reserved for null/used flags.
    pub null_map_length: usize,
    /// Used-flag bit offset in the null map.
    pub col_offset_in_null_map: usize,
    /// Number of columns needed for an additional join condition.
    pub column_count_needed_for_other_condition: usize,
    /// Number of build-side columns.
    pub total_column_number: usize,
    /// Selected key mode.
    pub key_mode: KeyMode,
}

impl JoinTableMeta {
    /// Builds metadata from dependency-closed source field categories.
    #[must_use]
    pub fn new(
        build_key_index: &[usize],
        build_types: &[ColumnType],
        build_key_types: &[ColumnType],
        probe_key_types: &[ColumnType],
        columns_used_by_other_condition: Option<&[usize]>,
        output_columns: Option<&[usize]>,
        need_used_flag: bool,
    ) -> Self {
        let mut meta = Self {
            is_fixed_length: true,
            row_length: 0,
            is_join_keys_inlined: true,
            is_join_keys_fixed_length: true,
            join_keys_length: 0,
            serialize_modes: Vec::with_capacity(build_key_index.len()),
            row_columns_order: Vec::new(),
            null_map_length: 0,
            col_offset_in_null_map: 0,
            column_count_needed_for_other_condition: 0,
            total_column_number: build_types.len(),
            key_mode: KeyMode::VariableSerializedKey,
        };

        let mut saved = vec![false; build_types.len()];
        let mut save_column = |index: usize| {
            if !saved[index] {
                saved[index] = true;
                if let Some(length) = build_types[index].fixed_row_length() {
                    meta.row_length += length;
                } else {
                    meta.is_fixed_length = false;
                }
            }
        };
        if let Some(output) = output_columns {
            for &index in output {
                save_column(index);
            }
            if let Some(other) = columns_used_by_other_condition {
                for &index in other {
                    save_column(index);
                }
            }
        } else {
            for index in 0..build_types.len() {
                save_column(index);
            }
        }

        let mut all_integer = true;
        let mut variable_key_count = 0usize;
        let mut seen_key_indices = std::collections::BTreeSet::new();
        for (index, &key_index) in build_key_index.iter().enumerate() {
            let build_prop = build_key_types[index].key_property();
            let probe_prop = probe_key_types[index].key_property();
            if let Some(length) = build_prop.key_length {
                meta.join_keys_length += length as isize;
            } else {
                meta.is_join_keys_fixed_length = false;
                variable_key_count += 1;
            }
            if !build_prop.can_be_inlined {
                meta.is_join_keys_inlined = false;
            }
            if build_prop.is_integer {
                assert!(
                    probe_prop.is_integer,
                    "integer build key requires integer probe key"
                );
                if build_prop.is_unsigned != probe_prop.is_unsigned {
                    meta.serialize_modes.push(SerializeMode::NeedSignFlag);
                    meta.is_join_keys_inlined = false;
                    if meta.is_join_keys_fixed_length {
                        meta.join_keys_length += 1;
                    }
                } else {
                    meta.serialize_modes.push(SerializeMode::Normal);
                }
            } else {
                all_integer = false;
                if build_prop.key_length.is_none() {
                    meta.serialize_modes
                        .push(SerializeMode::KeepVarColumnLength);
                } else {
                    meta.serialize_modes.push(SerializeMode::Normal);
                }
            }
            if !seen_key_indices.insert(key_index) {
                meta.is_join_keys_inlined = false;
            }
        }
        if !meta.is_join_keys_fixed_length {
            meta.join_keys_length = -1;
        }
        if !meta.is_join_keys_inlined && variable_key_count == 1 {
            for mode in &mut meta.serialize_modes {
                if *mode == SerializeMode::KeepVarColumnLength {
                    *mode = SerializeMode::Normal;
                }
            }
        }
        meta.key_mode = if all_integer
            && build_key_index.len() == 1
            && meta.serialize_modes.first() != Some(&SerializeMode::NeedSignFlag)
        {
            KeyMode::OneInt64
        } else if meta.is_join_keys_fixed_length {
            KeyMode::FixedSerializedKey
        } else {
            KeyMode::VariableSerializedKey
        };

        if meta.is_join_keys_inlined {
            for &index in build_key_index {
                save_column(index);
            }
        }
        if !meta.is_fixed_length {
            meta.row_length = 0;
        }
        meta.row_columns_order = Vec::new();
        let mut used = vec![false; build_types.len()];
        if meta.is_join_keys_inlined {
            for &index in build_key_index {
                append_unique_column(&mut meta.row_columns_order, &mut used, index);
            }
        }
        if let Some(other) = columns_used_by_other_condition {
            for &index in other {
                append_unique_column(&mut meta.row_columns_order, &mut used, index);
            }
            meta.column_count_needed_for_other_condition = used.iter().filter(|&&v| v).count();
        }
        if let Some(output) = output_columns {
            for &index in output {
                append_unique_column(&mut meta.row_columns_order, &mut used, index);
            }
        } else {
            for index in 0..build_types.len() {
                append_unique_column(&mut meta.row_columns_order, &mut used, index);
            }
        }

        let saved_column_count = saved.iter().filter(|&&v| v).count();
        if need_used_flag {
            meta.col_offset_in_null_map = 1;
            meta.null_map_length = (saved_column_count + 1).div_ceil(32) * 4;
        } else {
            meta.null_map_length = saved_column_count.div_ceil(8);
        }
        meta
    }

    /// Reports whether a null-map byte can be read without atomic access.
    #[must_use]
    pub const fn is_read_null_map_thread_safe(&self, column_index: usize) -> bool {
        !(self.col_offset_in_null_map == 1 && column_index < 31)
    }
}

fn append_unique_column(order: &mut Vec<usize>, used: &mut [bool], index: usize) {
    if !used[index] {
        used[index] = true;
        order.push(index);
    }
}
