// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! TiDB-extended error authority.

pub mod errcode;
pub mod errname;

pub use errname::{entry_by_code, message_by_code, CATALOG};
