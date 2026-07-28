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
//! through to the next family and ultimately to `crate::func::eval_func`'s
//! `Unsupported` error.
//!
//! These files are seed material until their complete upstream Go packages
//! are transcreated. Every builtin must cite the Go function it was read from
//! in `pkg/expression/builtin_*.go`.

use crate::{Datum, EvalError};

pub(crate) mod compare2;
pub(crate) mod crypto;
pub(crate) mod info;
pub(crate) mod json;
pub(crate) mod json2;
pub(crate) mod misc;
pub(crate) mod regexp;
pub(crate) mod string2;

pub(crate) use json::{cast_as_json, cast_as_json_typed, dispatch_typed as json_dispatch_typed};

/// Tries each family in turn; `None` if no family implements `name`.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    string2::dispatch(name, vals)
        .or_else(|| crypto::dispatch(name, vals))
        .or_else(|| info::dispatch(name, vals))
        .or_else(|| json::dispatch(name, vals))
        .or_else(|| json2::dispatch(name, vals))
        .or_else(|| regexp::dispatch(name, vals))
        .or_else(|| compare2::dispatch(name, vals))
        .or_else(|| misc::dispatch(name, vals))
}
