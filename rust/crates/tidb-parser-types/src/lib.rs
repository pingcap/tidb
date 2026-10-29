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


//! Go `pkg/parser/types`: the column type every layer above the parser
//! carries (`FieldType`), its evaluation class (`EvalType`) and the type
//! names. Go's `pkg/types` aliases these (`type FieldType = types.FieldType`)
//! and adds the functions over them.

mod eval_type;
pub mod parser_types_errors;
pub mod field_type;

#[cfg(test)]
mod eval_type_tests;

pub use eval_type::*;
pub use field_type::*;
