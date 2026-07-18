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

//! Go `HandParser.parseAlterTableOptions`' LOCK branch.

use tidb_ast::{AlterTableAction, AlterTableLock, AlterTableLockMode};

use crate::{PResult, Parser};

/// Parses only `LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}`.
///
/// The outer ALTER action loop owns commas and any following action. This
/// leaf consumes no token unless the first keyword is exactly `LOCK`.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("LOCK") {
        return Ok(None);
    }
    parser.bump();
    if parser.is_op("=") {
        parser.bump();
    }
    let mode = if parser.is_kw("DEFAULT") {
        parser.bump();
        AlterTableLockMode::Default
    } else if parser.is_kw("NONE") {
        parser.bump();
        AlterTableLockMode::None
    } else if parser.is_kw("SHARED") {
        parser.bump();
        AlterTableLockMode::Shared
    } else if parser.is_kw("EXCLUSIVE") {
        parser.bump();
        AlterTableLockMode::Exclusive
    } else {
        return Err(parser.err_here("expected ALTER TABLE LOCK type"));
    };
    Ok(Some(AlterTableAction::Lock(AlterTableLock { mode })))
}
