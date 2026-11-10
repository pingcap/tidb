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

//! Family modules for builtin scalar functions. Each family exposes
//! `dispatch(name, vals) -> Option<Result<Datum, EvalError>>`; `None` falls
//! through to the next family.
//!
//! These files are seed material until their complete upstream Go packages
//! are transcreated. Every builtin must cite the Go function it was read from
//! in `pkg/expression/builtin_*.go`.


pub(crate) mod cache;
pub(crate) mod compare2;
pub(crate) mod crypto;
pub(crate) mod info;
pub(crate) mod json;
pub(crate) mod misc;
pub(crate) mod regexp;
pub(crate) mod string2;
pub(crate) mod vec;

pub(crate) use cache::BuiltinFuncCache;
pub(crate) use compare2::{extremum_with_signature, interval_lazy, GlCmpStringMode, GlSignature};
pub(crate) use crypto::eval_aes_lazy;
pub(crate) use json::{
    cast_as_json, cast_as_json_typed, cast_as_json_value_typed,
    dispatch_typed as json_dispatch_typed, JsonSchemaCache,
};
pub(crate) use string2::{
    build_find_in_set_lookup, find_in_set_lookup, find_in_set_with_collation, FindInSetLookup,
};
