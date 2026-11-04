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

use super::node_fixture::*;
use std::sync::atomic::Ordering;

fn completed(sql: &str) -> (i64, i64) {
    let (_, digest) = tidb_parser::normalize_digest(sql);
    tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP
        .summary_map_values()
        .iter()
        .fold((0, 0), |(executions, errors), record| {
            let record = record.lock().unwrap();
            if record.digest == digest.to_string() {
                (
                    executions + record.cumulative.exec_count,
                    errors + record.cumulative.sum_errors,
                )
            } else {
                (executions, errors)
            }
        })
}

#[test]
fn observation_batch_text_and_binary_writes_publish_after_commit_verdict() {
    let sql = "INSERT INTO t (id, v) VALUES (93, 903 + 0)";
    for binary in [false, true] {
        let (mut session, cluster) = open_session();
        let prepared = binary.then(|| session.node_conn().prepare_general(sql).unwrap());
        let before = completed(sql);
        cluster.fail_commit.store(true, Ordering::Release);
        let failed = match &prepared {
            Some(prepared) => session.node_conn().execute_general(prepared, &[]).map(|_| ()),
            None => session.node_conn().execute_write(sql).map(|_| ()),
        };
        assert!(failed.is_err());
        assert_eq!(cluster.rows(), 0);
        assert_eq!(
            completed(sql),
            (before.0 + 1, before.1 + 1),
            "binary={binary}: scratch success must be reported as durable failure once"
        );
        cluster.fail_commit.store(false, Ordering::Release);
        match &prepared {
            Some(prepared) => {
                session.node_conn().execute_general(prepared, &[]).unwrap();
            }
            None => {
                session.node_conn().execute_write(sql).unwrap();
            }
        }
        assert_eq!(cluster.rows(), 1);
        assert_eq!(
            completed(sql),
            (before.0 + 2, before.1 + 1),
            "binary={binary}: next successful write must be counted once"
        );
    }
}

/// Go `executor.Compile` counts an EXECUTE's prepared statement after its
/// `Optimize` planned it (`CountStmtNode`): an EXECUTE whose plan fails --
/// Go: `LIMIT ?` bound to -1 is refused while planning -- is not counted, a
/// planned one is counted once.
#[test]
fn a_binary_execute_whose_planning_fails_is_not_counted() {
    fn selects() -> f64 {
        tidb_metrics::executor::STATEMENT_TOTAL
            .with_label_values(&["", "default", "Select"])
            .get()
    }
    let (mut session, _cluster) = open_session();
    let prepared = session
        .node_conn()
        .prepare_general("SELECT id FROM t LIMIT ?")
        .unwrap();
    let before = selects();
    assert!(session
        .node_conn()
        .execute_general(&prepared, &[tidb_param::PreparedValue::SignedLongLong(-1)])
        .is_err());
    assert_eq!(selects() - before, 0.0);
    session
        .node_conn()
        .execute_general(&prepared, &[tidb_param::PreparedValue::SignedLongLong(1)])
        .unwrap();
    assert_eq!(selects() - before, 1.0);
}
