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
//! The error codes, messages, SQLSTATEs and `SQLError` formatting
//! (`errcode.go`, `errname.go`, `state.go`, `error.go`) are [`errors`].

pub mod charset;
pub mod consts;
pub mod errors;
pub mod locale;
pub mod privilege;
mod simple_case;
pub mod types;
mod unicode_category;
pub mod util;

pub use charset::*;
pub use consts::*;
pub use locale::*;
pub use privilege::*;
pub use simple_case::{
    equal_fold, lowercase_equal, simple_fold, to_lowercase, to_uppercase, GO_UNICODE_VERSION,
};
pub use types::*;
pub use unicode_category::{is_unicode_lowercase_letter, is_unicode_uppercase_letter};
pub use util::*;
