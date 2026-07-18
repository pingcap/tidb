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

//! Go `HandParser.parseAlterPartition`'s terminal `PARTITION BY` branch.

use tidb_ast::AlterPartitionAction;

use crate::{PResult, Parser};

/// Parses the complete replacement partitioning payload without consuming a
/// named-partition action.  The caller owns ALTER TABLE's terminal ordering;
/// CREATE TABLE continues to share the exact `TablePartitioning` grammar.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterPartitionAction>> {
    if !(parser.is_kw("PARTITION") && parser.is_kw_at(1, "BY")) {
        return Ok(None);
    }
    Ok(Some(AlterPartitionAction::Repartition(Box::new(
        super::super::partition::parse_table_partitioning(parser)?,
    ))))
}
