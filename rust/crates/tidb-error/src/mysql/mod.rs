// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Parser/MySQL error authority.

pub mod errcode;
pub mod errname;
mod error;
pub mod state;

pub use errname::{entry_by_code, message_by_code, CATALOG};
pub use error::{
    redaction_mode, set_redaction_mode, FormatArg, RedactionMode, SqlError, ERR_BAD_CONN,
    ERR_MALFORM_PACKET,
};
pub use state::{mysql_state, DEFAULT_MYSQL_STATE, MYSQL_STATES};
