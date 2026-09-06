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

use std::any::Any;

use tidb_datatype::{FieldType, FieldTypeBuilder, FieldTypeCode, GoString};
use tidb_mysql::types::{
    AutoIncrementFlag, MultipleKeyFlag, NoDefaultValueFlag, NotNullFlag, PriKeyFlag, TypeDate,
    TypeDatetime, TypeDouble, TypeDuration, TypeEnum, TypeFloat, TypeInt24, TypeJSON, TypeLong,
    TypeLonglong, TypeNewDecimal, TypeShort, TypeString, TypeTiDBVectorFloat32, TypeTimestamp,
    TypeTiny, TypeYear, UniqueKeyFlag,
};

use crate::charset_collation::{charset, collation};
use crate::lattice::{
    field_tp, maybe, singleton, type_mismatch_error, Bool, Byte, IncompatibleError, Int, Lattice,
    StringList, Tuple, Value,
};

const FLAG_MASK_KEYS: u64 = (PriKeyFlag | UniqueKeyFlag | MultipleKeyFlag) as u64;
const FLAG_MASK_DEF_VAL: u64 = (AutoIncrementFlag | NoDefaultValueFlag) as u64;

/// Go `notPartOfKeys` (`^byte(0)`).
pub(crate) const NOT_PART_OF_KEYS: u8 = !0;

// Please ensure this list is synchronized with the order of `Tuple(...)` in
// `encode_field_type_to_lattice`.
pub(crate) const FIELD_TYPE_TUPLE_INDEX_TP: usize = 0;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_FLEN: usize = 1;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_DEC: usize = 2;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_FLAG_SINGLETON: usize = 3;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_FLAG_NULL: usize = 4;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_FLAG_ANTI_KEYS: usize = 5;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_FLAG_DEF_VAL: usize = 6;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_COLLATE: usize = 7;
pub(crate) const FIELD_TYPE_TUPLE_INDEX_ELEMS: usize = 8;

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

/// Go `encodeFieldTypeToLattice`.
fn encode_field_type_to_lattice(ft: &FieldType) -> Tuple {
    let flag = ft.raw_flags();
    let (flen, dec): (Box<dyn Lattice>, Box<dyn Lattice>) =
        if ft.code().mysql_type() == TypeNewDecimal {
            (
                singleton(Value::Int(ft.flen())),
                singleton(Value::Int(ft.decimal())),
            )
        } else {
            (Box::new(Int(ft.flen())), Box::new(Int(ft.decimal())))
        };

    let def_val = if flag & AutoIncrementFlag as u64 != 0 || flag & NoDefaultValueFlag as u64 == 0 {
        maybe(Some(singleton(Value::Uint(flag & FLAG_MASK_DEF_VAL))))
    } else {
        maybe(None)
    };

    Tuple(vec![
        field_tp(ft.code().mysql_type()),
        flen,
        dec,
        // TODO(from Go): recognize if the remaining flags can be merged or not.
        singleton(Value::Uint(
            flag & !(FLAG_MASK_DEF_VAL | NotNullFlag as u64 | FLAG_MASK_KEYS),
        )),
        Box::new(Bool(flag & NotNullFlag as u64 == 0)),
        Box::new(Byte(encode_anti_keys(flag))),
        def_val,
        Box::new(collation(ft.collation_name())),
        Box::new(StringList(ft.elems_snapshot())),
    ])
}

/// Go `decodeFieldTypeFromLattice`.
fn decode_field_type_from_lattice(tup: &Tuple) -> FieldType {
    let Value::List(lst) = tup.unwrap() else {
        unreachable!("a tuple unwraps to a list");
    };

    let &Value::Uint(mut flags) = &lst[FIELD_TYPE_TUPLE_INDEX_FLAG_SINGLETON] else {
        unreachable!("the flag singleton holds a Go uint");
    };
    let &Value::Byte(anti_keys) = &lst[FIELD_TYPE_TUPLE_INDEX_FLAG_ANTI_KEYS] else {
        unreachable!("the anti-keys entry holds a Go byte");
    };
    flags |= decode_anti_keys(anti_keys);
    if lst[FIELD_TYPE_TUPLE_INDEX_FLAG_NULL] != Value::Bool(true) {
        flags |= NotNullFlag as u64;
    }
    if let &Value::Uint(def_val) = &lst[FIELD_TYPE_TUPLE_INDEX_FLAG_DEF_VAL] {
        flags |= def_val;
    } else {
        flags |= NoDefaultValueFlag as u64;
    }

    let Value::Str(collate) = &lst[FIELD_TYPE_TUPLE_INDEX_COLLATE] else {
        unreachable!("the collate entry holds a Go string");
    };
    let collate = collate.to_utf8_lossy_go();
    let mut charset_name = collate
        .split_once('_')
        .map_or(collate.as_str(), |cut| cut.0);
    if charset_name.is_empty() {
        charset_name = collate.as_str();
    }
    let Value::Str(charset_name) = charset(charset_name).unwrap() else {
        unreachable!("a charset lattice unwraps to a Go string");
    };

    let (Value::Byte(tp), Value::Int(flen), Value::Int(dec)) = (
        &lst[FIELD_TYPE_TUPLE_INDEX_TP],
        &lst[FIELD_TYPE_TUPLE_INDEX_FLEN],
        &lst[FIELD_TYPE_TUPLE_INDEX_DEC],
    ) else {
        unreachable!("the type/flen/decimal entries hold byte and int values");
    };
    let Value::StringList(elems) = &lst[FIELD_TYPE_TUPLE_INDEX_ELEMS] else {
        unreachable!("the elems entry holds a Go string list");
    };

    FieldTypeBuilder::new()
        .with_code(FieldTypeCode::from_mysql_type(*tp))
        .flen_set(*flen)
        .decimal_set(*dec)
        .charset_set(charset_name.to_utf8_lossy_go())
        .collation_set(collate)
        .elems(elems.iter().cloned())
        .build()
        .with_raw_flags(flags)
}

/// Go's unexported `typ` struct (an embedded `Tuple`), returned by
/// [`Typ::new`].
#[derive(Clone, Debug)]
pub struct Typ {
    pub(crate) tuple: Tuple,
}

impl Typ {
    /// Go `Type`: creates the lattice of a column field type.
    #[must_use]
    pub fn new(ft: &FieldType) -> Self {
        Self {
            tuple: encode_field_type_to_lattice(ft),
        }
    }

    /// Go `hasDefault`.
    pub(crate) fn has_default(&self) -> bool {
        self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLAG_DEF_VAL].unwrap() != Value::Nil
    }

    /// Go `setFlagForMissingColumn`: adjusts the flags of the type for
    /// filling in a missing column. Returns whether the column had no default
    /// values.
    pub(crate) fn set_flag_for_missing_column(&mut self) -> bool {
        self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLAG_ANTI_KEYS] = Box::new(Byte(NOT_PART_OF_KEYS));
        let (def_val, ok) = match self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLAG_DEF_VAL].unwrap() {
            Value::Uint(def_val) => (def_val, true),
            _ => (0, false),
        };
        if !ok || def_val & NoDefaultValueFlag as u64 != 0 {
            self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLAG_DEF_VAL] = maybe(Some(singleton(
                Value::Uint(def_val & !(NoDefaultValueFlag as u64)),
            )));
            return true;
        }
        false
    }

    /// Go `isNotNull`.
    pub(crate) fn is_not_null(&self) -> bool {
        self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLAG_NULL].unwrap() != Value::Bool(true)
    }

    /// Go `inAutoIncrement`.
    pub(crate) fn in_auto_increment(&self) -> bool {
        matches!(
            self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLAG_DEF_VAL].unwrap(),
            Value::Uint(def_val) if def_val & AutoIncrementFlag as u64 != 0
        )
    }

    /// Go `setAntiKeyFlags`.
    pub(crate) fn set_anti_key_flags(&mut self, flag: u64) {
        self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLAG_ANTI_KEYS] =
            Box::new(Byte(encode_anti_keys(flag)));
    }

    /// Go `getStandardDefaultValue`.
    // The `mysql.Type*` constants keep their Go spellings in `tidb_mysql`.
    #[allow(non_upper_case_globals)]
    pub(crate) fn get_standard_default_value(&self) -> Value {
        let mut tail = String::new();
        if let Value::Int(dec) = self.tuple.0[FIELD_TYPE_TUPLE_INDEX_DEC].unwrap() {
            if dec > 0 {
                tail = format!(
                    ".{}",
                    "0".repeat(usize::try_from(dec).expect("a positive decimal count fits usize"))
                );
            }
        }

        let Value::Byte(tp) = self.tuple.0[FIELD_TYPE_TUPLE_INDEX_TP].unwrap() else {
            unreachable!("the type entry holds a Go byte");
        };
        let text: String = match tp {
            TypeTiny | TypeInt24 | TypeShort | TypeLong | TypeLonglong | TypeFloat | TypeDouble
            | TypeNewDecimal => "0".to_owned(),
            TypeTimestamp | TypeDatetime => format!("0000-00-00 00:00:00{tail}"),
            TypeDate => "0000-00-00".to_owned(),
            TypeDuration => format!("00:00:00{tail}"),
            TypeYear => "0000".to_owned(),
            TypeJSON => "null".to_owned(),
            TypeTiDBVectorFloat32 => "[]".to_owned(),
            TypeEnum => {
                let Value::StringList(elems) = self.tuple.0[FIELD_TYPE_TUPLE_INDEX_ELEMS].unwrap()
                else {
                    unreachable!("the elems entry holds a Go string list");
                };
                return Value::Str(elems[0].clone());
            }
            TypeString => {
                // ref https://github.com/pingcap/tidb/blob/66948b2fd9bec8ea11644770a2fa746c7eba1a1f/ddl/ddl_api.go#L3916
                // Go `charset.CollationBin` is "binary".
                if self.tuple.0[FIELD_TYPE_TUPLE_INDEX_COLLATE].unwrap()
                    == Value::Str(GoString::from("binary"))
                {
                    let Value::Int(flen) = self.tuple.0[FIELD_TYPE_TUPLE_INDEX_FLEN].unwrap()
                    else {
                        unreachable!("the flen entry holds a Go int");
                    };
                    let zeroes =
                        vec![0_u8; usize::try_from(flen).expect("a display width fits usize")];
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
        Value::FieldType(Box::new(decode_field_type_from_lattice(&self.tuple)))
    }

    fn compare(&self, other: &dyn Lattice) -> Result<i32, IncompatibleError> {
        if let Some(b) = other.as_any().downcast_ref::<Self>() {
            return self.tuple.compare(&b.tuple);
        }
        Err(type_mismatch_error(self, other))
    }

    fn join(&self, other: &dyn Lattice) -> Result<Box<dyn Lattice>, IncompatibleError> {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return Err(type_mismatch_error(self, other));
        };
        let gen_join = self.tuple.join(&b.tuple)?;
        let join = *gen_join
            .into_any()
            .downcast::<Tuple>()
            .expect("a tuple join returns a tuple");

        // Special check: we can't have an AUTO_INCREMENT column without being
        // a KEY.
        if let Value::Uint(def_val) = join.0[FIELD_TYPE_TUPLE_INDEX_FLAG_DEF_VAL].unwrap() {
            if def_val & AutoIncrementFlag as u64 != 0
                && join.0[FIELD_TYPE_TUPLE_INDEX_FLAG_ANTI_KEYS].unwrap()
                    == Value::Byte(NOT_PART_OF_KEYS)
            {
                return Err(IncompatibleError::raw(ERR_MSG_AUTO_TYPE_WITHOUT_KEY));
            }
        }

        Ok(Box::new(Self { tuple: join }))
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
