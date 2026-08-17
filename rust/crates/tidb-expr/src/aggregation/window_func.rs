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

//! Go `pkg/expression/aggregation/window_func.go`: the window-function
//! signature descriptor, which shares [`BaseFuncDesc`] with the aggregate one
//! and is what [`super::AggFuncDesc::new_for_window_func`] consumes.
//!
//! `WindowFuncToPBExpr` and `CanPushDownToTiFlash` are skipped; see [`super`].

use super::names;
use super::{AggDescError, BaseFuncDesc};
use crate::context::Columns;
use crate::expr_util::get_uint64_from_constant;
use crate::expression::Expression;
use tidb_datatype::FieldTypeFlags;

/// Go `WindowFuncDesc` (`window_func.go:27`).
#[derive(Clone, Debug)]
pub struct WindowFuncDesc {
    /// Go's embedded `baseFuncDesc`.
    pub base: BaseFuncDesc,
}

impl WindowFuncDesc {
    /// Go `NewWindowFuncDesc` (`window_func.go:32`).
    ///
    /// Go returns `(nil, nil)` -- no descriptor and no error -- when an
    /// argument check rejects the call; that is spelled `Ok(None)` here, which
    /// makes the "rejected" case impossible to confuse with "built".
    ///
    /// `skip_check_args` is Go's prepared-statement escape: parameters are not
    /// bound yet, so the constant checks cannot run.
    pub fn new(
        ctx: &impl Columns,
        name: &str,
        args: Vec<Expression>,
        skip_check_args: bool,
    ) -> Result<Option<Self>, AggDescError> {
        let lowered = name.to_ascii_lowercase();
        if !skip_check_args {
            match lowered.as_str() {
                // nth_value rejects `0` but allows NULL.
                names::NTH_VALUE => {
                    let Some(arg) = args.get(1) else {
                        return Ok(None);
                    };
                    match get_uint64_from_constant(arg, ctx) {
                        Some((value, is_null)) if value != 0 || is_null => {}
                        _ => return Ok(None),
                    }
                }
                // ntile rejects `0` but allows NULL.
                names::NTILE => {
                    let Some(arg) = args.first() else {
                        return Ok(None);
                    };
                    match get_uint64_from_constant(arg, ctx) {
                        Some((value, is_null)) if value != 0 || is_null => {}
                        _ => return Ok(None),
                    }
                }
                // Go `break`s out of the switch when there is no second
                // argument, leaving the call accepted.
                names::LEAD | names::LAG if args.len() >= 2 => {
                    match get_uint64_from_constant(&args[1], ctx) {
                        Some((_, false)) => {}
                        _ => return Ok(None),
                    }
                }
                _ => {}
            }
        }

        // Go captures `err` and applies the NOT-NULL fixups BEFORE returning
        // it, so a failed inference still runs the switch over a nil RetTp --
        // which would panic. Rust propagates the error first; the fixups are
        // unreachable on the failure path either way.
        let arg_flags: Vec<u32> = args
            .iter()
            .map(|arg| arg.static_type().map_or(0, tidb_datatype::FieldType::flags))
            .collect();
        let arg_count = args.len();
        let mut base = BaseFuncDesc::new(ctx, &lowered, args)?;

        // Some window functions' return type must be nullable, or must not be.
        match lowered.as_str() {
            names::ROW_NUMBER
            | names::RANK
            | names::DENSE_RANK
            | names::CUME_DIST
            | names::PERCENT_RANK
            | names::COUNT
            | names::APPROX_COUNT_DISTINCT
            | names::BIT_AND
            | names::BIT_OR
            | names::BIT_XOR => base.ret_type.set_flags(FieldTypeFlags::NOT_NULL),
            names::LEAD | names::LAG => {
                if arg_count == 3
                    && arg_flags[0] & FieldTypeFlags::NOT_NULL != 0
                    && arg_flags[2] & FieldTypeFlags::NOT_NULL != 0
                {
                    base.ret_type.set_flags(FieldTypeFlags::NOT_NULL);
                } else {
                    base.ret_type.del_flags(FieldTypeFlags::NOT_NULL);
                }
            }
            _ => base.ret_type.del_flags(FieldTypeFlags::NOT_NULL),
        }
        Ok(Some(WindowFuncDesc { base }))
    }
}

/// Go `noFrameWindowFuncs` (`window_func.go:85`): the functions that operate
/// on the whole partition and therefore admit no frame clause.
const NO_FRAME_WINDOW_FUNCS: &[&str] = &[
    names::CUME_DIST,
    names::DENSE_RANK,
    names::LAG,
    names::LEAD,
    names::NTILE,
    names::PERCENT_RANK,
    names::RANK,
    names::ROW_NUMBER,
];

/// The single entry of Go `useDefaultFrameWindowFuncs` (`window_func.go:97`):
/// `ROW_NUMBER`'s `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`.
///
/// The Go value is an `ast.FrameClause`; that AST node has no Rust
/// counterpart in this crate, so the frame is carried as the three facts the
/// clause states. A caller that owns an `ast.FrameClause` equivalent rebuilds
/// it from these.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowFrameDefault {
    /// Go `Type: ast.Rows`.
    pub rows: bool,
    /// Go `Extent.Start: ast.FrameBound{Type: ast.CurrentRow}`.
    pub start_is_current_row: bool,
    /// Go `Extent.End: ast.FrameBound{Type: ast.CurrentRow}`.
    pub end_is_current_row: bool,
}

/// Go `UseDefaultFrame` (`window_func.go:108`): whether the function's frame
/// is fixed by TiDB regardless of what the user wrote.
#[must_use]
pub fn use_default_frame(name: &str) -> Option<WindowFrameDefault> {
    if name.to_ascii_lowercase() == names::ROW_NUMBER {
        return Some(WindowFrameDefault {
            rows: true,
            start_is_current_row: true,
            end_is_current_row: true,
        });
    }
    None
}

/// Go `NeedFrame` (`window_func.go:113`).
#[must_use]
pub fn need_frame(name: &str) -> bool {
    !NO_FRAME_WINDOW_FUNCS.contains(&name.to_ascii_lowercase().as_str())
}
