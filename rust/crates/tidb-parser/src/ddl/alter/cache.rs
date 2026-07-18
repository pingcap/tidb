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

//! Go-shaped table-level `ALTER TABLE CACHE` and `NOCACHE` grammar.

use tidb_ast::{AlterTableAction, AlterTableCacheMode};

use crate::{PResult, Parser};

/// Parses only the `CACHE` and `NOCACHE` alternatives of Go's dedicated
/// cache/nocache alter-table branch. Sequence `CACHE` and SELECT `SQL_CACHE`
/// have separate grammar owners and intentionally do not enter here.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    let mode = if parser.is_kw("CACHE") {
        AlterTableCacheMode::Cache
    } else if parser.is_kw("NOCACHE") {
        AlterTableCacheMode::NoCache
    } else {
        return Ok(None);
    };
    parser.bump();
    Ok(Some(AlterTableAction::Cache(mode)))
}
