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

//! Complete Rust transcreation of `pkg/parser/mysql` value contracts.
//!
//! Generated error codes, messages, SQLSTATEs, and formatting live in the
//! dependency-leaf `tidb-error` crate; all remaining package contracts live
//! here so the original Go package stays one atomic inventory and receipt.

pub mod charset;
pub mod consts;
pub mod locale;
pub mod privilege;
pub mod types;
pub mod util;

pub use charset::*;
pub use consts::*;
pub use locale::*;
pub use privilege::*;
pub use types::*;
pub use util::*;
