// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct message and redaction catalog from `pkg/parser/mysql/errname.go`.

#![allow(non_upper_case_globals)]

use super::errcode;
use crate::{CatalogEntry, ErrMessage};

mod catalog_1;
mod catalog_2;
mod catalog_3;
mod consts_1;
mod consts_2;
mod consts_3;

pub use consts_1::*;
pub use consts_2::*;
pub use consts_3::*;

const CATALOG_LEN: usize = 952;

/// Concatenates the catalog parts into one contiguous array, in the
/// same source order, entirely at compile time.
const fn build_catalog() -> [CatalogEntry; CATALOG_LEN] {
    const EMPTY_MESSAGE: ErrMessage = ErrMessage {
        raw: "",
        redact_arg_pos: &[],
    };
    let mut out = [CatalogEntry {
        name: "",
        code: 0,
        message: EMPTY_MESSAGE,
    }; CATALOG_LEN];
    let mut pos = 0;
    {
        let src = catalog_1::CATALOG_1;
        let mut i = 0;
        while i < src.len() {
            out[pos] = src[i];
            pos += 1;
            i += 1;
        }
    }
    {
        let src = catalog_2::CATALOG_2;
        let mut i = 0;
        while i < src.len() {
            out[pos] = src[i];
            pos += 1;
            i += 1;
        }
    }
    {
        let src = catalog_3::CATALOG_3;
        let mut i = 0;
        while i < src.len() {
            out[pos] = src[i];
            pos += 1;
            i += 1;
        }
    }
    out
}

static CATALOG_ARRAY: [CatalogEntry; CATALOG_LEN] = build_catalog();

/// Complete parser/MySQL message catalog in source order.
pub const CATALOG: &[CatalogEntry] = &CATALOG_ARRAY;

/// Finds the source entry registered for `code`.
#[must_use]
pub fn entry_by_code(code: u16) -> Option<&'static CatalogEntry> {
    CATALOG.iter().find(|entry| entry.code == code)
}

/// Finds the source message registered for `code`.
#[must_use]
pub fn message_by_code(code: u16) -> Option<&'static ErrMessage> {
    entry_by_code(code).map(|entry| &entry.message)
}
