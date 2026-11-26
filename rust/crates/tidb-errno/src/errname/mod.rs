// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct message and redaction catalog from `pkg/errno/errname.go`.

use tidb_mysql::errors::{CatalogEntry, ErrMessage};

// The messages and `CATALOG` are generated from Go's errname.go by
// rust/scripts/generate-errno.py.
mod catalog_generated;

pub use catalog_generated::*;

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
