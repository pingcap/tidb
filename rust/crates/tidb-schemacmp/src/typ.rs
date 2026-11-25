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

//! Go `pkg/util/schemacmp/type.go`: the column field-type lattice.

use std::{any::Any, fmt::Display};

use tidb_datatype::{FieldType, FieldTypeBuilder, FieldTypeCode};
use tidb_goruntime::GoString;
use tidb_mysql::types::{
    AutoIncrementFlag, MultipleKeyFlag, NoDefaultValueFlag, NotNullFlag, PriKeyFlag, TypeDate,
    TypeDatetime, TypeDouble, TypeDuration, TypeEnum, TypeFloat, TypeInt24, TypeJSON, TypeLong,
    TypeLonglong, TypeNewDecimal, TypeShort, TypeString, TypeTiDBVectorFloat32, TypeTimestamp,
    TypeTiny, TypeYear, UniqueKeyFlag,
};

use crate::charset_collation::{CollationLattice, charset, collation};
use crate::lattice::{
    IncompatibleError, Lattice, StringList, Value, combine_compare_result,
    compare_field_type_codes, distinct_singletons_error, type_mismatch_error,
    wrap_tuple_index_error,
};

const FLAG_MASK_KEYS: u64 = (PriKeyFlag | UniqueKeyFlag | MultipleKeyFlag) as u64;
const FLAG_MASK_DEF_VAL: u64 = (AutoIncrementFlag | NoDefaultValueFlag) as u64;

/// Go `notPartOfKeys` (`^byte(0)`).
pub(crate) const NOT_PART_OF_KEYS: u8 = !0;

// Indices in Go's field type tuple, in comparison and join order.
const TYPE_INDEX: usize = 0;
const LENGTH_INDEX: usize = 1;
const DECIMAL_INDEX: usize = 2;
const FLAGS_INDEX: usize = 3;
const NULLABLE_INDEX: usize = 4;
const ANTI_KEYS_INDEX: usize = 5;
const DEFAULT_INDEX: usize = 6;
const COLLATION_INDEX: usize = 7;
const ELEMS_INDEX: usize = 8;

/// Go `ErrMsgAutoTypeWithoutKey`.
pub const ERR_MSG_AUTO_TYPE_WITHOUT_KEY: &str = "auto type but not defined as a key";

/// Go `encodeAntiKeys`: encodes the key flags so that
///  1. "not part of keys" (flag = 0) is the maximum, and
///  2. multiple keys (8) > unique key (4) > primary key (2).
fn encode_anti_keys(flag: u64) -> u8 {
    !((flag & FLAG_MASK_KEYS) as u8).reverse_bits()
}

/// Go `decodeAntiKeys`.
fn decode_anti_keys(encoded: u8) -> u64 {
    u64::from((!encoded).reverse_bits())
}

fn compare_singletons<T: Copy + Eq + Display>(left: T, right: T) -> Result<i32, IncompatibleError> {
    if left == right {
        Ok(0)
    } else {
        Err(distinct_singletons_error(
            &left.to_string(),
            &right.to_string(),
        ))
    }
}

/// Go's unexported `typ` struct, returned by [`Typ::new`].
///
/// The Go tuple's nine components have fixed types. Rust stores them together
/// while keeping the same comparison and join order.
#[derive(Clone, Debug)]
pub struct Typ {
    tp: u8,
    flen: i64,
    decimal: i64,
    flags: u64,
    nullable: bool,
    anti_keys: u8,
    default_flags: Option<u64>,
    collation: CollationLattice,
    elems: StringList,
}

impl Typ {
    /// Go `Type` and `encodeFieldTypeToLattice`.
    pub fn new(ft: &FieldType) -> Self {
        let flag = ft.raw_flags();
        Self {
            tp: ft.code().mysql_type(),
            flen: ft.flen(),
            decimal: ft.decimal(),
            flags: flag & !(FLAG_MASK_DEF_VAL | NotNullFlag as u64 | FLAG_MASK_KEYS),
            nullable: flag & NotNullFlag as u64 == 0,
            anti_keys: encode_anti_keys(flag),
            default_flags: (flag & AutoIncrementFlag as u64 != 0
                || flag & NoDefaultValueFlag as u64 == 0)
                .then_some(flag & FLAG_MASK_DEF_VAL),
            collation: collation(ft.collation_name()),
            elems: StringList(ft.elems_snapshot()),
        }
    }

    /// Go `decodeFieldTypeFromLattice`.
    pub(crate) fn decode_field_type(&self) -> FieldType {
        let mut flags = self.flags | decode_anti_keys(self.anti_keys);
        if !self.nullable {
            flags |= NotNullFlag as u64;
        }
        flags |= self.default_flags.unwrap_or(NoDefaultValueFlag as u64);

        let collate = self.collation.unwrap_string();
        let charset_name = collate
            .split_once('_')
            .map_or(collate.as_str(), |cut| cut.0);
        let charset_name = if charset_name.is_empty() {
            collate.as_str()
        } else {
            charset_name
        };
        let Value::Str(charset_name) = charset(charset_name).unwrap() else {
            unreachable!("a charset lattice unwraps to a Go string");
        };

        FieldTypeBuilder::new()
            .with_code(FieldTypeCode::from_mysql_type(self.tp))
            .flen_set(self.flen)
            .decimal_set(self.decimal)
            .charset_set(charset_name.to_utf8_lossy_go())
            .collation_set(collate)
            .elems(self.elems.0.iter().cloned())
            .build()
            .with_raw_flags(flags)
    }

    /// Go `hasDefault`.
    pub(crate) fn has_default(&self) -> bool {
        self.default_flags.is_some()
    }

    /// Go `setFlagForMissingColumn`: adjusts the flags of the type for
    /// filling in a missing column. Returns whether the column had no default
    /// values.
    pub(crate) fn set_flag_for_missing_column(&mut self) -> bool {
        self.anti_keys = NOT_PART_OF_KEYS;
        let default = self.default_flags.unwrap_or(NoDefaultValueFlag as u64);
        if default & NoDefaultValueFlag as u64 != 0 {
            self.default_flags = Some(default & !(NoDefaultValueFlag as u64));
            return true;
        }
        false
    }

    /// Go `isNotNull`.
    pub(crate) fn is_not_null(&self) -> bool {
        !self.nullable
    }

    /// Go `inAutoIncrement`.
    pub(crate) fn in_auto_increment(&self) -> bool {
        self.default_flags
            .is_some_and(|flag| flag & AutoIncrementFlag as u64 != 0)
    }

    /// Go `setAntiKeyFlags`.
    pub(crate) fn set_anti_key_flags(&mut self, flag: u64) {
        self.anti_keys = encode_anti_keys(flag);
    }

    fn compare_dimension(&self, left: i64, right: i64) -> Result<i32, IncompatibleError> {
        if self.tp == TypeNewDecimal {
            compare_singletons(left, right)
        } else {
            Ok(left.cmp(&right) as i32)
        }
    }

    fn compare_default_flags(&self, other: &Self) -> Result<i32, IncompatibleError> {
        match (self.default_flags, other.default_flags) {
            (Some(left), Some(right)) => compare_singletons(left, right),
            (None, None) => Ok(0),
            (Some(_), None) => Ok(1),
            (None, Some(_)) => Ok(-1),
        }
    }

    /// Go `getStandardDefaultValue`.
    // The `mysql.Type*` constants keep their Go spellings in `tidb_mysql`.
    #[allow(non_upper_case_globals)]
    pub(crate) fn get_standard_default_value(&self) -> Value {
        let tail = if self.decimal > 0 {
            format!(
                ".{}",
                "0".repeat(
                    usize::try_from(self.decimal).expect("a positive decimal count fits usize")
                )
            )
        } else {
            String::new()
        };

        let text: String = match self.tp {
            TypeTiny | TypeInt24 | TypeShort | TypeLong | TypeLonglong | TypeFloat | TypeDouble
            | TypeNewDecimal => "0".to_owned(),
            TypeTimestamp | TypeDatetime => format!("0000-00-00 00:00:00{tail}"),
            TypeDate => "0000-00-00".to_owned(),
            TypeDuration => format!("00:00:00{tail}"),
            TypeYear => "0000".to_owned(),
            TypeJSON => "null".to_owned(),
            TypeTiDBVectorFloat32 => "[]".to_owned(),
            TypeEnum => return Value::Str(self.elems.0[0].clone()),
            TypeString => {
                if self.collation.is_binary() {
                    let zeroes =
                        vec![0_u8; usize::try_from(self.flen).expect("a display width fits usize")];
                    return Value::Str(GoString::from(zeroes));
                }
                String::new()
            }
            _ => String::new(),
        };
        Value::Str(GoString::from(text))
    }
}

impl Lattice for Typ {
    fn unwrap(&self) -> Value {
        Value::FieldType(Box::new(self.decode_field_type()))
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return Err(type_mismatch_error(self, other));
        };
        let mut order = 0;
        let mut accumulate = |index, comparison: Result<i32, IncompatibleError>| {
            let cmp = comparison.map_err(|error| wrap_tuple_index_error(index, error))?;
            order = combine_compare_result(order, cmp)
                .map_err(|error| wrap_tuple_index_error(index, error))?;
            Ok(())
        };
        accumulate(TYPE_INDEX, compare_field_type_codes(self.tp, b.tp))?;
        accumulate(LENGTH_INDEX, self.compare_dimension(self.flen, b.flen))?;
        accumulate(
            DECIMAL_INDEX,
            self.compare_dimension(self.decimal, b.decimal),
        )?;
        accumulate(FLAGS_INDEX, compare_singletons(self.flags, b.flags))?;
        accumulate(NULLABLE_INDEX, Ok(self.nullable.cmp(&b.nullable) as i32))?;
        accumulate(ANTI_KEYS_INDEX, Ok(self.anti_keys.cmp(&b.anti_keys) as i32))?;
        accumulate(DEFAULT_INDEX, self.compare_default_flags(b))?;
        accumulate(
            COLLATION_INDEX,
            self.collation.compare_collation(&b.collation),
        )?;
        accumulate(ELEMS_INDEX, self.elems.compare_items(&b.elems))?;
        Ok(order)
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return Err(type_mismatch_error(self, other));
        };
        let tp_order = compare_field_type_codes(self.tp, b.tp)
            .map_err(|error| wrap_tuple_index_error(TYPE_INDEX, error))?;
        if self.tp == TypeNewDecimal {
            compare_singletons(self.flen, b.flen)
                .map_err(|error| wrap_tuple_index_error(LENGTH_INDEX, error))?;
            compare_singletons(self.decimal, b.decimal)
                .map_err(|error| wrap_tuple_index_error(DECIMAL_INDEX, error))?;
        }
        compare_singletons(self.flags, b.flags)
            .map_err(|error| wrap_tuple_index_error(FLAGS_INDEX, error))?;
        self.compare_default_flags(b)
            .map_err(|error| wrap_tuple_index_error(DEFAULT_INDEX, error))?;
        let default_flags = self.default_flags.or(b.default_flags);
        let collation = self
            .collation
            .join_collation(&b.collation)
            .map_err(|error| wrap_tuple_index_error(COLLATION_INDEX, error))?;
        let elems_order = self
            .elems
            .compare_items(&b.elems)
            .map_err(|error| wrap_tuple_index_error(ELEMS_INDEX, error))?;
        let anti_keys = self.anti_keys.max(b.anti_keys);
        if anti_keys == NOT_PART_OF_KEYS
            && default_flags.is_some_and(|flag| flag & AutoIncrementFlag as u64 != 0)
        {
            return Err(IncompatibleError::raw(ERR_MSG_AUTO_TYPE_WITHOUT_KEY));
        }

        Ok(Box::new(Self {
            tp: if tp_order < 0 { b.tp } else { self.tp },
            flen: self.flen.max(b.flen),
            decimal: self.decimal.max(b.decimal),
            flags: self.flags,
            nullable: self.nullable || b.nullable,
            anti_keys,
            default_flags,
            collation,
            elems: if elems_order <= 0 {
                b.elems.clone()
            } else {
                self.elems.clone()
            },
        }))
    }

    fn go_type_name(&self) -> &'static str {
        "schemacmp.typ"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clone_lattice(&self) -> Box<dyn Lattice> {
        Box::new(self.clone())
    }
}
