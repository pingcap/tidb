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

//! Go `pkg/planner/core/fts_resolve_index_test.go`: `FTS_MATCH_WORD()` over a
//! TiFlash fulltext index, against Go's recorded
//! `testdata/fts_resolve_index_suite_out.json`.

use super::*;

/// Go `setStarterDeployModeForFTSTest`.
fn set_starter_deploy_mode_for_fts_test() -> bool {
    if !tidb_config_kerneltype::is_next_gen() {
        return false;
    }
    tidb_config_deploymode::set(tidb_config_deploymode::Mode::Starter).unwrap();
    true
}

/// Go `setupTiFlashFTSMatchWordTable`.
fn setup_tiflash_fts_match_word_table(stack: &UnistoreClusterStack, session: &mut Session) {
    rows(session, "CREATE DATABASE IF NOT EXISTS test");
    rows(session, "USE test");
    rows(session, "set global tidb_redact_log=OFF");
    rows(
        session,
        "create table fts_t(id int primary key, title text, body text, fulltext key ft_title(title))",
    );
    rows(session, "alter table fts_t set tiflash replica 1");
    // Go `testkit.SetTiFlashReplica`.
    let table_id: i64 = displayed(rows(
        session,
        "select tidb_table_id from information_schema.tables where table_name = 'fts_t'",
    ))[0][0]
        .parse()
        .unwrap();
    stack
        .factory
        .ddl
        .update_table_replica_info(table_id, true,
            &tidb_sessionctx::StmtContext::for_query(),
        )
        .unwrap();
}

fn must_contain_err_msg(session: &mut Session, sql: &str, want: &str) {
    let (_, message) = query_error(session, sql);
    assert!(message.contains(want), "{sql}: {message}");
}

/// Go `TestFTSRequiresStarterMode`.
#[test]
fn fts_requires_starter_mode() {
    if tidb_config_kerneltype::is_next_gen() {
        tidb_config_deploymode::set(tidb_config_deploymode::Mode::Premium).unwrap();
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(4300)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    must_contain_err_msg(
        &mut session,
        "create table fts_blocked(id int primary key, title text, fulltext key ft_title(title))",
        "FULLTEXT index is only supported in starter deployment mode",
    );
    rows(&mut session, "create table fts_t(id int primary key, title text)");
    must_contain_err_msg(
        &mut session,
        "alter table fts_t add fulltext index ft_title(title)",
        "FULLTEXT index is only supported in starter deployment mode",
    );
    must_contain_err_msg(
        &mut session,
        "explain select * from fts_t where fts_match_word('hello', title)",
        "FTS_MATCH_WORD() is only supported in starter deployment mode",
    );
}

/// A table's FULLTEXT keys pass Go's `checkConstraintGrammar`,
/// `checkIndexOptions`, `checkIndexSpecs`, `buildFullTextInfoWithCheck` and
/// `checkColumnarIndexIfNeedTiFlashReplica`: each statement's outcome is the
/// one a Go testkit gave over two mock TiFlash stores, first in the premium
/// deployment and then in the starter one. The testkit prints `int(11)`
/// where a server prints `int`.
#[test]
fn create_table_checks_its_full_text_keys_like_go() {
    let stack = super::tiflash_partition::tiflash_stack(2);
    let mut session = stack.factory.open_session(session_context(4304)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let mut deployment = "classic";
    let mut starter_statements = 0;
    for line in include_str!("../../../../../testdata/fts_create_table_go.txt").lines() {
        let mut fields = line.splitn(3, " | ");
        let (label, sql, want) = (fields.next().unwrap(), fields.next().unwrap(), fields.next().unwrap());
        if label != deployment {
            if !tidb_config_kerneltype::is_next_gen() {
                break;
            }
            tidb_config_deploymode::set(tidb_config_deploymode::Mode::Starter).unwrap();
            deployment = "starter";
        }
        starter_statements += usize::from(deployment == "starter");
        let want = want.replace("int(11)", "int");
        let got = if let Some(error) = want.strip_prefix("ERR ") {
            let (code, message) = query_error(&mut session, sql);
            let class_end = error.find(']').unwrap();
            let want_code: u16 = error[error.find(':').unwrap() + 1..class_end].parse().unwrap();
            assert_eq!(code, want_code, "{deployment}: {sql}: {message}");
            format!("ERR {}{message}", &error[..=class_end])
        } else if want == "OK" {
            rows(&mut session, sql);
            "OK".to_owned()
        } else {
            let shown: Vec<String> = displayed(rows(&mut session, sql))
                .into_iter()
                .map(|row| row.concat().replace('\n', "\\n"))
                .collect();
            format!("ROWS {}", shown.join(" ;; "))
        };
        assert_eq!(got, want, "{deployment}: {sql}");
    }
    assert_eq!(starter_statements, if tidb_config_kerneltype::is_next_gen() { 23 } else { 0 });
}

/// Go `TestTiFlashFTSMatchWordPushDown`.
#[test]
fn tiflash_fts_match_word_push_down() {
    if !set_starter_deploy_mode_for_fts_test() {
        return;
    }
    let suites: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../../../pkg/planner/core/testdata/fts_resolve_index_suite_out.json"
    ))
    .unwrap();
    let cases = suites
        .as_array()
        .unwrap()
        .iter()
        .find(|suite| suite["Name"] == "TestTiFlashFTSMatchWordPushDown")
        .unwrap()["Cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 8);
    let stack = super::tiflash_partition::tiflash_stack(2);
    let mut session = stack.factory.open_session(session_context(4301)).unwrap();
    setup_tiflash_fts_match_word_table(&stack, &mut session);
    for case in cases {
        let sql = case["SQL"].as_str().unwrap();
        let redact_log = if case["RedactLog"].as_bool().unwrap() { "ON" } else { "OFF" };
        rows(&mut session, &format!("set global tidb_redact_log={redact_log}"));
        let got: Vec<String> = displayed(rows(&mut session, &format!("explain format = 'plan_tree' {sql}")))
            .into_iter()
            .map(|row| row.join(" "))
            .collect();
        let want: Vec<&str> = case["Plan"].as_array().unwrap().iter().map(|row| row.as_str().unwrap()).collect();
        assert_eq!(got, want, "{sql}");
    }
    rows(&mut session, "set global tidb_redact_log=OFF");

    for (sql, want) in [
        (
            "explain select fts_match_word('hello', title) from fts_t",
            "'FTS_MATCH_WORD()' in SELECT requires a matching 'FTS_MATCH_WORD()' in WHERE",
        ),
        (
            "explain select fts_match_word('hello', title) * 2 from fts_t where fts_match_word('hello', title)",
            "'FTS_MATCH_WORD()' in SELECT must not be wrapped in expressions",
        ),
        (
            "explain select fts_match_word('world', title) from fts_t where fts_match_word('hello', title)",
            "'FTS_MATCH_WORD()' in SELECT must match the one in WHERE",
        ),
        (
            "explain select * from fts_t where fts_match_word('hello', title) and fts_match_word('world', title)",
            "Currently 'FTS_MATCH_WORD()' must be used alone",
        ),
        (
            "explain select * from fts_t where fts_match_word('hello', body)",
            "Full text search can only be used with a matching fulltext index",
        ),
        (
            "explain select * from fts_t order by fts_match_word('hello', title)",
            "Currently 'FTS_MATCH_WORD()' in ORDER BY without a LIMIT clause is not supported",
        ),
    ] {
        must_contain_err_msg(&mut session, sql, want);
    }
}

/// Go `TestTiFlashFTSMatchWordPreparedPlanCache`.
#[test]
fn tiflash_fts_match_word_prepared_plan_cache() {
    if !set_starter_deploy_mode_for_fts_test() {
        return;
    }
    let stack = super::tiflash_partition::tiflash_stack(2);
    let mut session = stack.factory.open_session(session_context(4302)).unwrap();
    setup_tiflash_fts_match_word_table(&stack, &mut session);
    rows(&mut session, "set @@tidb_enable_prepared_plan_cache=1");
    rows(&mut session, "set @@tidb_enable_non_prepared_plan_cache=0");

    rows(
        &mut session,
        "prepare stmt from \"select * from fts_t where fts_match_word('hello', title)\"",
    );
    for _ in 0..2 {
        rows(&mut session, "execute stmt");
        assert_eq!(displayed(rows(&mut session, "select @@last_plan_from_cache")), [["0"]]);
    }
    rows(&mut session, "deallocate prepare stmt");

    must_contain_err_msg(
        &mut session,
        "prepare stmt_param from 'select * from fts_t where fts_match_word(?, title)'",
        "match against a non-constant string",
    );
}

/// Go `TestTiFlashFTSMatchWordDirtyTxn`.
#[test]
fn tiflash_fts_match_word_dirty_txn() {
    if !set_starter_deploy_mode_for_fts_test() {
        return;
    }
    let stack = super::tiflash_partition::tiflash_stack(2);
    let mut session = stack.factory.open_session(session_context(4303)).unwrap();
    setup_tiflash_fts_match_word_table(&stack, &mut session);

    assert!(rows(&mut session, "select * from fts_t where fts_match_word('hello', title)").is_empty());

    rows(&mut session, "begin");
    rows(&mut session, "insert into fts_t values (1, 'hello', 'dirty')");
    must_contain_err_msg(
        &mut session,
        "select * from fts_t where fts_match_word('hello', title)",
        "FTS_MATCH_WORD() cannot be used in a transaction with uncommitted changes",
    );
    rows(&mut session, "rollback");
}
