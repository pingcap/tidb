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

//! Family extension modules for builtin scalar functions — the
//! parallel-work seam. Each family owns exactly ONE file here with its own
//! `dispatch(name, vals) -> Option<Result<Datum, EvalError>>`; a `None`
//! means "not mine", falling through to the next family and ultimately to
//! `crate::func::eval_func`'s honest `Unsupported` error. This lets several
//! agents add builtins concurrently with zero shared-file edits: a NEW
//! builtin goes in its family's file only, never in `func.rs`'s match and
//! never in this chain (which only changes when a whole new FAMILY is
//! added).
//!
//! Ownership (see `rust/PARALLEL.md`): `string2` also owns fixes in
//! `crate::string_fn`; the complete Go time and translated math families live
//! in the separate `crate::time_fn` and `crate::math_fn` source-owned
//! directories; `compare2` owns `crate::ops`. Every ported builtin
//! must cite the Go function it was read from (`pkg/expression/
//! builtin_*.go`) in its doc comment — the Go code is the source of truth.

use crate::{Datum, EvalError};

pub(crate) mod compare2;
pub(crate) mod crypto;
pub(crate) mod info;
pub(crate) mod json;
pub(crate) mod json2;
pub(crate) mod misc;
pub(crate) mod regexp;
pub(crate) mod string2;

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
