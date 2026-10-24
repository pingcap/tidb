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

use super::super::*;
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
        let prepared = binary.then(|| session.prepare_general(sql).unwrap());
        let before = completed(sql);
        cluster.fail_commit.store(true, Ordering::Release);
        let failed = match &prepared {
            Some(prepared) => session.execute_general(prepared, &[]).map(|_| ()),
            None => session.execute_write(sql).map(|_| ()),
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
                session.execute_general(prepared, &[]).unwrap();
            }
            None => {
                session.execute_write(sql).unwrap();
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
