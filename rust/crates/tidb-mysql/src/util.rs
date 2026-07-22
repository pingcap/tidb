// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Default field dimensions and small MySQL type/authentication helpers.

#![allow(non_upper_case_globals)]

use crate::consts::{AuthCachingSha2Password, AuthNativePassword, AuthTiDBSM3Password};
use crate::types::*;

/// Returns whether a type is one of MySQL's five integer storage types.
#[must_use]
pub const fn is_integer_type(tp: u8) -> bool {
    matches!(
        tp,
        TypeTiny | TypeShort | TypeInt24 | TypeLong | TypeLonglong
    )
}

/// Returns the source default display length and decimal length.
#[must_use]
pub const fn default_field_length_and_decimal(tp: u8) -> (i64, i32) {
    match tp {
        TypeBit => (1, 0),
        TypeTiny => (4, 0),
        TypeShort => (6, 0),
        TypeInt24 => (9, 0),
        TypeLong => (11, 0),
        TypeLonglong => (20, 0),
        TypeDouble => (22, -1),
        TypeFloat => (12, -1),
        TypeNewDecimal => (10, 0),
        TypeDuration | TypeDate => (10, 0),
        TypeTimestamp | TypeDatetime => (19, 0),
        TypeYear => (4, 0),
        TypeString => (1, 0),
        TypeVarchar | TypeVarString => (5, 0),
        TypeTinyBlob => (255, 0),
        TypeBlob => (65_535, 0),
        TypeMediumBlob => (16_777_215, 0),
        TypeLongBlob | TypeJSON => (4_294_967_295, 0),
        TypeNull => (0, 0),
        TypeSet | TypeEnum => (-1, 0),
        _ => (-1, -1),
    }
}

/// Returns the source default dimensions for an unspecified CAST target.
#[must_use]
pub const fn default_field_length_and_decimal_for_cast(tp: u8) -> (i64, i32) {
    match tp {
        TypeString => (0, -1),
        TypeDate => (10, 0),
        TypeDatetime => (19, 0),
        TypeNewDecimal => (10, 0),
        TypeDuration => (10, 0),
        TypeLonglong => (22, 0),
        TypeDouble => (22, -1),
        TypeFloat => (12, -1),
        TypeJSON => (4_194_304, 0),
        _ => (-1, -1),
    }
}

/// Mirrors the intentionally historical Go helper: these three supported
/// plugins are classified as requiring access to the clear-text password.
#[must_use]
pub fn is_auth_plugin_clear_text(plugin: &str) -> bool {
    matches!(
        plugin,
        AuthNativePassword | AuthTiDBSM3Password | AuthCachingSha2Password
    )
}
