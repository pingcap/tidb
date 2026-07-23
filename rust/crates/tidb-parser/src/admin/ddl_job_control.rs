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

//! Go's shared `ADMIN {CANCEL|PAUSE|RESUME} DDL JOBS` parser leaf.

use tidb_ast::{AdminDdlJobControlKind, AdminDdlJobControlStmt};
use tidb_lexer::TokenKind;

use crate::{PResult, Parser};

pub(super) fn parse(parser: &mut Parser) -> PResult<Option<AdminDdlJobControlStmt>> {
    let kind = if parser.is_kw_at(1, "CANCEL") {
        AdminDdlJobControlKind::Cancel
    } else if parser.is_kw_at(1, "PAUSE") {
        AdminDdlJobControlKind::Pause
    } else if parser.is_kw_at(1, "RESUME") {
        AdminDdlJobControlKind::Resume
    } else {
        return Ok(None);
    };

    if !parser.is_kw_at(2, "DDL") {
        return Ok(None);
    }

    parser.expect_kw("ADMIN")?;
    parser.bump(); // CANCEL, PAUSE, or RESUME
    parser.expect_kw("DDL")?;

    // Keep Go's `parseAdminDDLJobs` transit exactly: it calls `next()` for
    // this token without checking its spelling, then restores it as `JOBS`.
    // EOF is the sole non-token case and must not become an implicit noun.
    if parser.peek().kind == TokenKind::Eof {
        return Err(parser.err_here("expected DDL job noun"));
    }
    parser.bump();

    let mut job_ids = vec![parse_job_id(parser)?];
    while parser.is_op(",") {
        parser.bump();
        if parser.peek().kind == TokenKind::IntLit {
            job_ids.push(parse_job_id(parser)?);
        } else {
            break;
        }
    }
    Ok(Some(AdminDdlJobControlStmt { kind, job_ids }))
}

fn parse_job_id(parser: &mut Parser) -> PResult<i64> {
    let token = parser.peek().clone();
    if token.kind != TokenKind::IntLit {
        return Err(parser.err_here("expected a DDL job ID"));
    }
    parser.bump();
    token
        .text
        .parse()
        .map_err(|_| parser.err_here("DDL job ID is out of signed 64-bit range"))
}
