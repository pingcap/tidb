// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Parser/MySQL error authority.

pub mod errcode;
pub mod errname;
mod error;
pub mod state;

pub use errname::{entry_by_code, message_by_code, CATALOG};
pub use error::{
    go_quote_bytes, go_quote_string, redaction_mode, set_redaction_mode, FormatArg, RedactionMode,
    SqlError, ERR_BAD_CONN, ERR_MALFORM_PACKET,
};
pub use state::{mysql_state, DEFAULT_MYSQL_STATE, MYSQL_STATES};

/// A source error message template and its zero-based sensitive arguments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrMessage {
    /// Go `fmt` template stored by the source catalog.
    pub raw: &'static str,
    /// Argument positions passed to `errors.RedactErrorArg`.
    pub redact_arg_pos: &'static [usize],
}

/// One named entry in a checked-in source error catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogEntry {
    /// Source Go identifier.
    pub name: &'static str,
    /// MySQL protocol error number.
    pub code: u16,
    /// Default message template and redaction metadata.
    pub message: ErrMessage,
}
