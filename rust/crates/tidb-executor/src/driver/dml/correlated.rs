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

use super::*;

/// One assignment of an UPDATE: the planner's expression over the read's row
/// (Go `Update.OrderedList`), or, for a point update the fast plan reads,
/// an expression over the table's own row.
pub(super) enum UpdateExpression {
    Scalar(Expression),
    Physical(Expression),
}

impl UpdateExpression {
    pub(super) fn scalar(expression: Expression) -> Self {
        Self::Scalar(expression)
    }

    pub(super) fn physical(expression: Expression) -> Self {
        Self::Physical(expression)
    }

    pub(super) fn eval(
        &self,
        scalar_row: tidb_chunk::row::Row<'_>,
        physical_row: Option<tidb_chunk::row::Row<'_>>,
        ctx: &crate::StmtContext,
    ) -> Result<Datum, DriverError> {
        match self {
            Self::Scalar(expression) => expression
                .eval(ctx, scalar_row)
                .map_err(|error| DriverError::Exec(ExecError::Eval(error))),
            Self::Physical(expression) => expression
                .eval(
                    ctx,
                    physical_row.ok_or_else(|| {
                        DriverError::unsupported(
                            "a planned UPDATE expression has no physical input row",
                        )
                    })?,
                )
                .map_err(|error| DriverError::Exec(ExecError::Eval(error))),
        }
    }
}
