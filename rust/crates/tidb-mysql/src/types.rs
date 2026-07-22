// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! MySQL wire type codes, column flags, bounds, and flag predicates.

#![allow(non_upper_case_globals)]

macro_rules! constants {
    ($ty:ty; $($name:ident = $value:expr;)+) => {$(
        #[doc = concat!("Source-compatible `", stringify!($name), "` constant.")]
        pub const $name: $ty = $value;
    )+};
}

constants! { u8;
    TypeUnspecified = 0; TypeTiny = 1; TypeShort = 2; TypeLong = 3;
    TypeFloat = 4; TypeDouble = 5; TypeNull = 6; TypeTimestamp = 7;
    TypeLonglong = 8; TypeInt24 = 9; TypeDate = 10; TypeDuration = 11;
    TypeDatetime = 12; TypeYear = 13; TypeNewDate = 14; TypeVarchar = 15;
    TypeBit = 16; TypeTiDBVectorFloat32 = 0xe1; TypeJSON = 0xf5;
    TypeNewDecimal = 0xf6; TypeEnum = 0xf7; TypeSet = 0xf8;
    TypeTinyBlob = 0xf9; TypeMediumBlob = 0xfa; TypeLongBlob = 0xfb;
    TypeBlob = 0xfc; TypeVarString = 0xfd; TypeString = 0xfe;
    TypeGeometry = 0xff;
}

constants! { usize;
    NotNullFlag = 1 << 0; PriKeyFlag = 1 << 1; UniqueKeyFlag = 1 << 2;
    MultipleKeyFlag = 1 << 3; BlobFlag = 1 << 4; UnsignedFlag = 1 << 5;
    ZerofillFlag = 1 << 6; BinaryFlag = 1 << 7; EnumFlag = 1 << 8;
    AutoIncrementFlag = 1 << 9; TimestampFlag = 1 << 10; SetFlag = 1 << 11;
    NoDefaultValueFlag = 1 << 12; OnUpdateNowFlag = 1 << 13;
    PartKeyFlag = 1 << 14; NumFlag = 1 << 15; GroupFlag = 1 << 15;
    UniqueFlag = 1 << 16; BinCmpFlag = 1 << 17; ParseToJSONFlag = 1 << 18;
    IsBooleanFlag = 1 << 19; PreventNullInsertFlag = 1 << 20;
    EnumSetAsIntFlag = 1 << 21; DropColumnIndexFlag = 1 << 22;
    GeneratedColumnFlag = 1 << 23; UnderScoreCharsetFlag = 1 << 24;
}

/// Largest unsigned MEDIUMINT value.
pub const MaxUint24: u32 = (1 << 24) - 1;
/// Largest signed MEDIUMINT value.
pub const MaxInt24: i32 = (1 << 23) - 1;
/// Smallest signed MEDIUMINT value.
pub const MinInt24: i32 = -(1 << 23);

/// Returns whether every bit in `flag_item` is present in `flag`.
#[must_use]
pub const fn has_flag(flag: usize, flag_item: usize) -> bool {
    flag & flag_item != 0
}

macro_rules! predicate {
    ($($go:ident, $rust:ident, $flag:ident;)+) => {$(
        #[doc = concat!("Source-compatible `", stringify!($go), "` predicate.")]
        #[must_use]
        pub const fn $rust(value: usize) -> bool { has_flag(value, $flag) }
    )+};
}

predicate! {
    HasDropColumnWithIndexFlag, has_drop_column_with_index_flag, DropColumnIndexFlag;
    HasNotNullFlag, has_not_null_flag, NotNullFlag;
    HasNoDefaultValueFlag, has_no_default_value_flag, NoDefaultValueFlag;
    HasAutoIncrementFlag, has_auto_increment_flag, AutoIncrementFlag;
    HasUnsignedFlag, has_unsigned_flag, UnsignedFlag;
    HasZerofillFlag, has_zerofill_flag, ZerofillFlag;
    HasBinaryFlag, has_binary_flag, BinaryFlag;
    HasPriKeyFlag, has_pri_key_flag, PriKeyFlag;
    HasUniKeyFlag, has_uni_key_flag, UniqueKeyFlag;
    HasMultipleKeyFlag, has_multiple_key_flag, MultipleKeyFlag;
    HasTimestampFlag, has_timestamp_flag, TimestampFlag;
    HasOnUpdateNowFlag, has_on_update_now_flag, OnUpdateNowFlag;
    HasParseToJSONFlag, has_parse_to_json_flag, ParseToJSONFlag;
    HasIsBooleanFlag, has_is_boolean_flag, IsBooleanFlag;
    HasPreventNullInsertFlag, has_prevent_null_insert_flag, PreventNullInsertFlag;
    HasEnumSetAsIntFlag, has_enum_set_as_int_flag, EnumSetAsIntFlag;
}
