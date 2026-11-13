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

//! Go `executor.createMaskingPolicyWithInfo` after `doDDLJob2`: the policy
//! another session created while the job waited, pinned by a Go testkit run
//! whose `beforeRunOneJobStep` hook inserts the same row.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tidb_model::{ActionType, Job};

use super::Session;

fn exec(session: &mut Session, sql: &str) {
    if let Err(error) = session.run(sql) {
        panic!("{sql}: {:?}", error.to_mysql_error());
    }
}

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    crate::tests_support::row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

#[test]
fn a_policy_created_while_the_job_waited_is_if_not_exists_note_like_go() {
    let domain = crate::domain::mock_store::MockStoreDomain::create()
        .unwrap_or_else(|error| panic!("boot the mock store: {error}"));
    let mut session = domain.new_session().unwrap();
    let session = &mut session;
    let other = Arc::new(Mutex::new(domain.new_session().unwrap()));
    exec(session, "use test");
    // (the raced policy's column, the statement, its error, its warnings,
    // the policy left)
    let cases = [
        (
            "c",
            "create masking policy if not exists p on t0(c) as upper(c) enable",
            None,
            vec!["Note|8268|masking policy already exists"],
            "p|c|upper(c)",
        ),
        (
            "b",
            "create masking policy if not exists p on t1(c) as upper(c) enable",
            None,
            vec!["Note|8268|masking policy already exists"],
            "p|b|upper(b)",
        ),
        (
            "c",
            "create masking policy p on t2(c) as upper(c) enable",
            Some((8268, "masking policy already exists")),
            vec!["Error|8268|masking policy already exists"],
            "p|c|upper(c)",
        ),
        (
            "b",
            "create masking policy p on t3(c) as upper(c) enable",
            Some((8268, "masking policy already exists")),
            vec!["Error|8268|masking policy already exists"],
            "p|b|upper(b)",
        ),
        (
            "c",
            "create or replace masking policy p on t4(c) as upper(c) enable",
            None,
            vec![],
            "p|c|UPPER(`c`)",
        ),
    ];
    for (index, (column, sql, error, warnings, left)) in cases.into_iter().enumerate() {
        let table = format!("t{index}");
        exec(session, &format!("create table {table}(c varchar(20), b varchar(20))"));
        let table_id = rows(
            session,
            &format!("select tidb_table_id from information_schema.tables where table_name='{table}'"),
        )
        .remove(0);
        let column_id = if column == "c" { 1 } else { 2 };
        let insert = format!(
            "insert into mysql.tidb_masking_policy (policy_name, db_name, table_name, table_id, \
             column_name, column_id, expression, status, masking_type, created_at, updated_at) \
             values ('p','test','{table}',{table_id},'{column}',{column_id},'upper({column})',\
             'ENABLED','CUSTOM',now(6),now(6))"
        );
        let done = AtomicBool::new(false);
        let racer = other.clone();
        let _hook = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |job: &mut dyn Any| {
            let job = job.downcast_mut::<Job>().expect("the step's job");
            if job.type_ != ActionType::ACTION_CREATE_MASKING_POLICY || done.swap(true, Ordering::SeqCst) {
                return;
            }
            exec(&mut racer.lock().unwrap(), &insert);
        });
        let got = session.run(sql).err().map(|error| error.to_mysql_error());
        assert_eq!(
            got.as_ref().map(|error| (error.code, error.message.as_str())),
            error,
            "{sql}"
        );
        assert_eq!(rows(session, "show warnings"), warnings, "{sql}");
        assert_eq!(
            rows(
                session,
                &format!(
                    "select policy_name, column_name, expression from mysql.tidb_masking_policy \
                     where table_name='{table}'"
                )
            ),
            [left],
            "{sql}"
        );
    }
}
