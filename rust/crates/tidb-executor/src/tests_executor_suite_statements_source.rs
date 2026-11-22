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

//! The executor-internal halves of Go
//! `pkg/executor/test/executor/executor_test.go` (`TestAdapterStatement`'s
//! compiled origin text, `TestSessionRootTrackerDetach`'s tracker
//! lifecycle); its SQL tests run as TestKit SQL in tidb-session.

use crate::StmtContext;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// Go `executor_test.go:1439::TestAdapterStatement`: the compiled statement
/// keeps its ORIGIN text (`select 1`, the create-table text) and the GBK
/// client-charset statement renders `select '表1'` as text while keeping the
/// raw GBK bytes as origin.
///
/// The origin-text halves are pinned here through the AST the driver parses
/// (`tidb_ast`'s `NodeText::original_text`); the GBK client-charset arm
/// needs the charset handshake and stays a gap.
#[test]
fn adapter_statement_keeps_origin_text() {
    let stmt = ctx().parse("select 1").expect("parse");
    assert_eq!(stmt.node_text().original_text(), b"select 1");
    let stmt = ctx().parse("create table test.t (a int)").expect("parse");
    assert_eq!(
        stmt.node_text().original_text(),
        b"create table test.t (a int)"
    );
}

/// Go `executor_test.go:2275::TestSessionRootTrackerDetach`, ported at the
/// session/ statement tracker boundary (`SessionMemory`/`StatementMemory`,
/// Go's `ResetContextOfStmt` shape): while a statement is live the session
/// root's action chain carries a fallback (Go's `GetFallbackForTest` is
/// non-nil) and after the statement finishes — Go's `rs.Close()` — the
/// fallback is detached, so a fresh statement starts clean.
#[test]
fn session_root_tracker_fallback_detaches_after_the_statement() {
    // Go's quota=10 arm: an over-quota statement errors 8175 (pinned for the
    // write path by driver::tests::mem_quota; the fallback lifecycle is what
    // is pinned here).
    let session = crate::SessionMemory::new(10_000_000, crate::OomAction::Cancel, 1);
    let statement = session.statement();
    assert!(
        statement
            .session_tracker()
            .get_fallback_for_test(false)
            .is_some(),
        "Go: MemTracker.GetFallbackForTest(false) is non-nil while the result set is open"
    );
    statement.finish_statement();
    assert!(
        statement
            .session_tracker()
            .get_fallback_for_test(false)
            .is_none(),
        "Go: after rs.Close() GetFallbackForTest(false) is nil"
    );
}

