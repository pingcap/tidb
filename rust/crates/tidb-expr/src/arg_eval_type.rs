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

//! The readers a signature body uses on an argument Go's
//! `newBaseBuiltinFuncWithTp` (`pkg/expression/builtin.go`) has already
//! wrapped in the cast its declared eval type asks for
//! ([`crate::builtin::new_base_builtin_func_with_tp`]). Every
//! `evalInt`/`evalString` body downstream reads an argument that is ALREADY
//! of its declared type, so these readers refuse anything else.

use crate::{Datum, EvalError};

/// The `EvalString` a signature body reads out of an argument its build has
/// already cast: Go's `string`, which is a BYTE sequence and is never
/// validated as UTF-8. `None` is Go's NULL.
///
/// The four kinds below are exactly the ones
/// [`cast_arg_as_string`](crate::cast::cast_arg_as_string) leaves alone,
/// because they are the ones whose `EvalType()` is already `types.ETString`;
/// ENUM and SET read as their NAME, which is what Go's `EvalString` on a
/// hybrid column returns.
///
/// Reading BYTES here is the whole point of the rung for this cast kind. The
/// bodies routed through it used to funnel their argument through
/// `crate::coerce::coerce_str`, whose UTF-8 check turned every binary
/// argument into a hard evaluation error where TiDB answers a value:
/// captured, `hex(elt(1,v))` and `hex(ltrim(v))` over a `varbinary` holding
/// `0xFF` are `FF`, and `hex(quote(v))` is `27EFBFBD27`.
pub(crate) fn eval_string(v: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    match v {
        Datum::Null => Ok(None),
        Datum::String(value) => Ok(Some(value.bytes().to_vec())),
        Datum::Bytes(value) => Ok(Some(value.clone())),
        Datum::BinaryLiteral(value) => Ok(Some(value.as_bytes().to_vec())),
        Datum::Enum(value, _) => Ok(Some(value.name_bytes().to_vec())),
        Datum::Set(value, _) => Ok(Some(value.name_bytes().to_vec())),
        // Same contract as [`eval_int`]: the build declared this position
        // `types.ETString`, so an un-cast datum here means the signature was
        // reached without its build. Refuse loudly rather than re-deriving.
        _ => Err(EvalError::Unsupported("un-cast types.ETString argument")),
    }
}

/// The `EvalInt` a signature body reads out of an argument its build has
/// already cast: Go's `int64` carrier, whose 64 bits are the same ones an
/// UNSIGNED argument holds (`b.args[i].EvalInt` never re-reads the flag --
/// only `builtinTruncateIntSig` does, and it asks the TYPE). `None` is Go's
/// NULL.
pub(crate) fn eval_int(v: &Datum) -> Result<Option<i64>, EvalError> {
    match v {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(*value)),
        Datum::UInt(value) => Ok(Some(*value as i64)),
        // The build declared this position `types.ETInt`, so nothing else
        // can arrive unless the signature was reached without its build.
        // Refuse loudly instead of re-deriving the cast here: a silent NULL
        // would hide the uncovered entry.
        _ => Err(EvalError::Unsupported("un-cast types.ETInt argument")),
    }
}
