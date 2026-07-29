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

//! The aggregate-function RUNTIME: the per-kind partial states Go keeps in
//! `pkg/executor/aggfuncs`, and the tuple DISTINCT identity those states and
//! `SELECT DISTINCT` share.
//!
//! This is state, not an operator. The row-wise `GROUP BY`/`HAVING` planning
//! that used to live here belonged to the retired `Database` engine; the live
//! consumers are `tidb-executor`'s aggregation operators and the bounded
//! single-table node's `aggregate_result_set` / `distinct_result_set`.

use std::cmp::Ordering;

use tidb_datatype::Datum;

use crate::ExecError;

/// Tuple DISTINCT identity shared by the aggregate functions and the prepared
/// read's `SELECT DISTINCT` dedup.
pub mod aggregate_distinct {
    include!("aggregate_distinct.rs");
}
pub mod runtime;

/// Compares two non-NULL values from one resolved MAX/MIN evaluator domain.
///
/// Go selects one typed evaluator from the aggregate descriptor before values
/// reach the partial state. This seed executor does not carry that descriptor
/// yet, so rejecting mixed domains is safer than silently treating them as
/// equal. String payloads likewise must agree on their typed collation.
pub(super) fn value_cmp(a: &Datum, b: &Datum) -> Result<Ordering, ExecError> {
    match (a, b) {
        (Datum::Int(x), Datum::Int(y)) => Ok(x.cmp(y)),
        (Datum::UInt(x), Datum::UInt(y)) => Ok(x.cmp(y)),
        (Datum::String(x), Datum::String(y)) if x.collation() == y.collation() => {
            Ok(x.collation().compare(x.bytes(), y.bytes()))
        }
        (Datum::String(_), Datum::String(_)) => {
            Err(ExecError::Unsupported("MAX/MIN string collation mismatch"))
        }
        (Datum::Bytes(x), Datum::Bytes(y)) => Ok(x.cmp(y)),
        (Datum::Decimal(x), Datum::Decimal(y)) => Ok(x.cmp(y)),
        (Datum::Real(x), Datum::Real(y)) => x
            .partial_cmp(y)
            .ok_or(ExecError::Unsupported("MAX/MIN unordered real")),
        (Datum::Null, _) | (_, Datum::Null) => {
            Err(ExecError::Unsupported("MAX/MIN NULL comparison"))
        }
        (Datum::MinNotNull | Datum::MaxValue, _) | (_, Datum::MinNotNull | Datum::MaxValue) => {
            Err(ExecError::Unsupported("MAX/MIN range sentinel input"))
        }
        _ => Err(ExecError::Unsupported("MAX/MIN mixed value domains")),
    }
}
