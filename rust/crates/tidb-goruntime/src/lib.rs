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
// See the License for the specific language governing permissions and
// limitations under the License.


//! The Go runtime and standard-library semantics the port reproduces exactly
//! wherever TiDB's Go code relies on them: shared slices and their growth
//! (`go_runtime`), Go strings as byte sequences (`go_string`) and `strconv`'s
//! number formatting (`go_strconv`).

pub mod go_runtime;
pub mod go_strconv;
mod go_string;

pub use go_string::{decode_go_utf8_lossy, go_chars, GoChars, GoString, GoStringSource};
