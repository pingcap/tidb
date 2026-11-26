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

//! Go `SQLDigestTextRetriever` (`pkg/expression/util.go`).

use std::collections::HashMap;

use tidb_expropt::SqlExecutor;
use tidb_sqlexec::SqlExecError;

/// Go `SQLDigestTextRetriever`: finds the normalized SQL text of statement
/// digests in the statements summary tables. `TIDB_DECODE_SQL_DIGESTS` uses
/// it, and so do the memory tables that show a digest's text.
#[derive(Clone, Debug)]
pub struct SqlDigestTextRetriever {
    /// Go `SQLDigestsMap`: the requested digests, and the place their texts
    /// are put; a digest no summary knows keeps the empty text.
    pub sql_digests_map: HashMap<String, String>,
    /// Go `fetchAllLimit`: past this many digests the query fetches every row
    /// instead of listing the digests in a `WHERE IN`.
    fetch_all_limit: usize,
}

impl Default for SqlDigestTextRetriever {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlDigestTextRetriever {
    /// Go `NewSQLDigestTextRetriever`.
    #[must_use]
    pub fn new() -> Self {
        SqlDigestTextRetriever {
            sql_digests_map: HashMap::new(),
            fetch_all_limit: 512,
        }
    }

    /// Go `runFetchDigestQuery`: the normalized SQL texts from either the
    /// current or the history statement summary table (the `cluster_` one
    /// when `query_global`), only of `in_values` when given, else every row.
    fn run_fetch_digest_query(
        &self,
        exec: &dyn SqlExecutor,
        query_global: bool,
        history: bool,
        in_values: &[String],
    ) -> Result<HashMap<String, String>, SqlExecError> {
        let context = tikv_client::util::with_internal_source_type(
            &tikv_client::trace::TraceContext::new(),
            // Go `kv.InternalTxnOthers`, client-go's `util.InternalTxnOthers`.
            tikv_client::util::INTERNAL_TXN_OTHERS,
        );
        let mut table = if query_global {
            "information_schema.cluster_statements_summary".to_owned()
        } else {
            "information_schema.statements_summary".to_owned()
        };
        if history {
            table.push_str("_history");
        }
        // DISTINCT restores the deduplication the old UNION DISTINCT
        // provided: the same (digest, digest_text) pair appears once per
        // summary window, so a plain SELECT would materialize and size the
        // result by the total history row count.
        let mut stmt = format!("select distinct digest, digest_text from {table}");
        // Add the where clause when a digest batch is specified.
        if !in_values.is_empty() {
            stmt.push_str(" where digest in (");
            stmt.push_str(&vec!["%?"; in_values.len()].join(","));
            stmt.push(')');
        }
        let arguments: Vec<tidb_sqlescape::SqlArg<'_>> = in_values
            .iter()
            .map(|value| tidb_sqlescape::SqlArg::String(value.as_bytes()))
            .collect();
        let (rows, _) = exec.exec_restricted_sql(&context, &[], &stmt, &arguments)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let digest = row.first()?.as_raw_bytes()?;
                let text = row.get(1)?.as_raw_bytes()?;
                Some((
                    String::from_utf8_lossy(digest).into_owned(),
                    String::from_utf8_lossy(text).into_owned(),
                ))
            })
            .collect())
    }

    /// Go `updateDigestInfo`: a digest whose text is still unknown takes the
    /// query's nonempty one.
    fn update_digest_info(&mut self, query_result: &HashMap<String, String>) {
        for (digest, text) in &mut self.sql_digests_map {
            if !text.is_empty() {
                continue;
            }
            if let Some(sql_text) = query_result.get(digest) {
                if !sql_text.is_empty() {
                    text.clone_from(sql_text);
                }
            }
        }
    }

    /// Go `unresolvedDigests`: the digests still without a text, sorted.
    fn unresolved_digests(&self) -> Vec<String> {
        let mut digests: Vec<String> = self
            .sql_digests_map
            .iter()
            .filter(|(_, text)| text.is_empty())
            .map(|(digest, _)| digest.clone())
            .collect();
        digests.sort();
        digests
    }

    /// Go `hasUnresolvedDigests`.
    fn has_unresolved_digests(&self) -> bool {
        self.sql_digests_map.values().any(String::is_empty)
    }

    /// Go `retrieveDigests`: one query of the chosen table for the digests
    /// still unresolved.
    fn retrieve_digests(
        &mut self,
        exec: &dyn SqlExecutor,
        query_global: bool,
        history: bool,
    ) -> Result<(), SqlExecError> {
        let digests = self.unresolved_digests();
        if digests.is_empty() {
            return Ok(());
        }
        let batch_size = if self.fetch_all_limit == 0 { 512 } else { self.fetch_all_limit };
        // Reading the table once avoids a very long WHERE IN clause. It also
        // matters for the history table: every history query rescans all
        // summary files, so one query per digest batch would multiply the
        // scan cost.
        let in_values: &[String] = if digests.len() > batch_size { &[] } else { &digests };
        let query_result = self.run_fetch_digest_query(exec, query_global, history, in_values)?;
        self.update_digest_info(&query_result);
        Ok(())
    }

    /// Go `RetrieveLocal`: the texts this node's summaries know, the history
    /// read only for the digests the current summary does not.
    ///
    /// # Errors
    /// The summary query's failure.
    pub fn retrieve_local(&mut self, exec: &dyn SqlExecutor) -> Result<(), SqlExecError> {
        self.retrieve_digests(exec, false, false)?;
        self.retrieve_digests(exec, false, true)
    }

    /// Go `RetrieveGlobal`: this node's current summary, then the cluster's
    /// current summary before this node's history (a digest recorded on
    /// another node then costs no local history scan), then the cluster's
    /// history for what is still unknown.
    ///
    /// # Errors
    /// The summary query's failure.
    pub fn retrieve_global(&mut self, exec: &dyn SqlExecutor) -> Result<(), SqlExecError> {
        self.retrieve_digests(exec, false, false)?;
        // In some unit-test environments the cluster tables cannot be read,
        // and querying them blocks for tens of seconds.
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/expression/sqlDigestRetrieverSkipRetrieveGlobal").is_some() {
            return self.retrieve_digests(exec, false, true);
        }
        // Accepted trade-off (Go): a digest held only in local history pays
        // a remote query first.
        let global_current = self.retrieve_digests(exec, true, false);
        self.retrieve_digests(exec, false, true)?;
        if !self.has_unresolved_digests() {
            return Ok(());
        }
        global_current?;
        self.retrieve_digests(exec, true, true)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use tidb_datatype::Datum;
    use tidb_sqlescape::SqlArg;
    use tidb_ast::Stmt;
    use tidb_resolve::ResultFieldRef;
    use tidb_sqlexec::{ExecutionContext, OptionFuncAlias, RestrictedSqlExecutor};

    use super::SqlDigestTextRetriever;

    /// Go's `mockQuery`, answered through the restricted executor the
    /// retriever really calls: the current or history statements summary of
    /// this node, or of the cluster for the `cluster_` tables, filtered to
    /// the `WHERE IN` digests when the query names them. Each query is
    /// recorded as (global, history, digests).
    #[derive(Default)]
    struct SummaryTables {
        local: HashMap<String, String>,
        global: HashMap<String, String>,
        local_history: HashMap<String, String>,
        global_history: HashMap<String, String>,
        /// The cluster's current summary fails to answer.
        fail_global_current: bool,
        queries: Mutex<Vec<(bool, bool, Vec<String>)>>,
    }

    impl RestrictedSqlExecutor for SummaryTables {
        fn parse_with_params(&self, _: &dyn ExecutionContext, _: &str, _: &[SqlArg<'_>]) -> tidb_sqlexec::Result<Stmt> {
            unreachable!("the retriever runs ExecRestrictedSQL")
        }

        fn exec_restricted_stmt(
            &self,
            _: &dyn ExecutionContext,
            _: &Stmt,
            _: &[OptionFuncAlias],
        ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
            unreachable!("the retriever runs ExecRestrictedSQL")
        }

        fn exec_restricted_sql(
            &self,
            _: &dyn ExecutionContext,
            _: &[OptionFuncAlias],
            sql: &str,
            arguments: &[SqlArg<'_>],
        ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
            let (global, history) = match sql.split(" where ").next().unwrap() {
                "select distinct digest, digest_text from information_schema.statements_summary" => (false, false),
                "select distinct digest, digest_text from information_schema.statements_summary_history" => (false, true),
                "select distinct digest, digest_text from information_schema.cluster_statements_summary" => (true, false),
                "select distinct digest, digest_text from information_schema.cluster_statements_summary_history" => {
                    (true, true)
                }
                other => panic!("unexpected query {other}"),
            };
            assert_eq!(sql.contains(" where digest in ("), !arguments.is_empty(), "{sql}");
            let wanted: Vec<String> = arguments
                .iter()
                .map(|argument| match argument {
                    SqlArg::String(bytes) => String::from_utf8(bytes.to_vec()).unwrap(),
                    _ => panic!("a digest is a string argument"),
                })
                .collect();
            self.queries.lock().unwrap().push((global, history, wanted.clone()));
            if self.fail_global_current && global && !history {
                return Err("cluster summary unavailable".into());
            }
            let table = match (global, history) {
                (false, false) => &self.local,
                (false, true) => &self.local_history,
                (true, false) => &self.global,
                (true, true) => &self.global_history,
            };
            let rows = table
                .iter()
                .filter(|(digest, _)| wanted.is_empty() || wanted.contains(digest))
                .map(|(digest, text)| {
                    vec![
                        Datum::new_string_datum(digest.as_bytes().to_vec()),
                        Datum::new_string_datum(text.as_bytes().to_vec()),
                    ]
                })
                .collect();
            Ok((rows, Vec::new()))
        }
    }

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(digest, text)| ((*digest).to_owned(), (*text).to_owned())).collect()
    }

    fn digests(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    /// Go `TestSQLDigestTextRetriever`, with its mock data served by the
    /// summary tables the retriever queries.
    #[test]
    fn test_sql_digest_text_retriever() {
        let tables = SummaryTables {
            local: map(&[("digest1", "text1"), ("digest2", "text2"), ("digest6", "text6")]),
            global: map(&[("digest2", "text2"), ("digest3", "text3"), ("digest4", "text4"), ("digest7", "text7")]),
            global_history: map(&[("digest5", "text5")]),
            ..SummaryTables::default()
        };
        let requested = map(&[("digest1", ""), ("digest2", ""), ("digest3", ""), ("digest4", ""), ("digest5", "")]);
        let expected_local = map(&[("digest1", "text1"), ("digest2", "text2"), ("digest3", ""), ("digest4", ""), ("digest5", "")]);
        let expected_global = map(&[
            ("digest1", "text1"),
            ("digest2", "text2"),
            ("digest3", "text3"),
            ("digest4", "text4"),
            ("digest5", "text5"),
        ]);

        let mut r = SqlDigestTextRetriever::new();
        r.sql_digests_map = requested.clone();
        r.retrieve_local(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_local);
        r.sql_digests_map = requested.clone();
        r.retrieve_global(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_global);

        r.fetch_all_limit = 1;
        r.sql_digests_map = requested.clone();
        r.retrieve_local(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_local);
        r.sql_digests_map = requested;
        r.retrieve_global(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_global);
    }

    /// Go `TestSQLDigestTextRetriever/current summary hit skips history`.
    #[test]
    fn test_sql_digest_text_retriever_current_summary_hit_skips_history() {
        let tables = SummaryTables { local: map(&[("digest1", "text1")]), ..SummaryTables::default() };
        let mut r = SqlDigestTextRetriever::new();
        r.sql_digests_map = map(&[("digest1", "")]);
        r.retrieve_local(&tables).unwrap();
        assert!(tables.queries.lock().unwrap().iter().all(|(_, history, _)| !history));
        assert_eq!(r.sql_digests_map, map(&[("digest1", "text1")]));
    }

    /// Go `TestSQLDigestTextRetriever/cluster current hit precedes local history`.
    #[test]
    fn test_sql_digest_text_retriever_cluster_current_hit_precedes_local_history() {
        let tables = SummaryTables {
            local: map(&[("digest1", "text1")]),
            global: map(&[("digest3", "text3")]),
            local_history: map(&[("digest2", "text2")]),
            ..SummaryTables::default()
        };
        let mut r = SqlDigestTextRetriever::new();
        r.sql_digests_map = map(&[("digest1", ""), ("digest2", ""), ("digest3", "")]);
        r.retrieve_global(&tables).unwrap();
        assert_eq!(r.sql_digests_map, map(&[("digest1", "text1"), ("digest2", "text2"), ("digest3", "text3")]));
        assert_eq!(
            *tables.queries.lock().unwrap(),
            [
                (false, false, digests(&["digest1", "digest2", "digest3"])),
                (true, false, digests(&["digest2", "digest3"])),
                (false, true, digests(&["digest2"])),
            ]
        );
    }

    /// Go `RetrieveGlobal`: a failed cluster current lookup is no error once
    /// the local history resolves every digest, and is returned otherwise
    /// (before the cluster history is read).
    #[test]
    fn test_sql_digest_text_retriever_cluster_current_failure() {
        let tables = SummaryTables {
            local_history: map(&[("digest1", "text1")]),
            fail_global_current: true,
            ..SummaryTables::default()
        };
        let mut r = SqlDigestTextRetriever::new();
        r.sql_digests_map = map(&[("digest1", "")]);
        r.retrieve_global(&tables).unwrap();
        assert_eq!(r.sql_digests_map, map(&[("digest1", "text1")]));

        tables.queries.lock().unwrap().clear();
        r.sql_digests_map = map(&[("digest1", ""), ("digest2", "")]);
        let error = r.retrieve_global(&tables).unwrap_err();
        assert!(error.to_string().contains("cluster summary unavailable"), "{error}");
        assert!(!tables.queries.lock().unwrap().iter().any(|(global, history, _)| *global && *history));
    }

    /// Go `TestSQLDigestTextRetriever/unrequested results do not replace requested digests`.
    #[test]
    fn test_sql_digest_text_retriever_unrequested_results_do_not_replace_requested_digests() {
        struct Unrequested;
        impl RestrictedSqlExecutor for Unrequested {
            fn parse_with_params(&self, _: &dyn ExecutionContext, _: &str, _: &[SqlArg<'_>]) -> tidb_sqlexec::Result<Stmt> {
                unreachable!()
            }
            fn exec_restricted_stmt(
                &self,
                _: &dyn ExecutionContext,
                _: &Stmt,
                _: &[OptionFuncAlias],
            ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
                unreachable!()
            }
            fn exec_restricted_sql(
                &self,
                _: &dyn ExecutionContext,
                _: &[OptionFuncAlias],
                _: &str,
                _: &[SqlArg<'_>],
            ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
                Ok((
                    vec![vec![
                        Datum::new_string_datum(b"unrequested".to_vec()),
                        Datum::new_string_datum(b"some SQL".to_vec()),
                    ]],
                    Vec::new(),
                ))
            }
        }
        let mut r = SqlDigestTextRetriever::new();
        r.sql_digests_map = map(&[("requested", "")]);
        r.retrieve_local(&Unrequested).unwrap();
        assert_eq!(r.sql_digests_map, map(&[("requested", "")]));
    }

    /// Go `TestSQLDigestTextRetriever/large history lookup scans history once`.
    #[test]
    fn test_sql_digest_text_retriever_large_history_lookup_scans_history_once() {
        let tables = SummaryTables::default();
        let mut r = SqlDigestTextRetriever::new();
        r.fetch_all_limit = 2;
        r.sql_digests_map = map(&[("digest1", ""), ("digest2", ""), ("digest3", ""), ("digest4", ""), ("digest5", "")]);
        r.retrieve_local(&tables).unwrap();
        // Every history query rescans all summary files, so the number of
        // history queries equals the number of history scans: more digests
        // than the limit fetch all rows in one unfiltered query.
        let history_calls: Vec<usize> = tables
            .queries
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, history, _)| *history)
            .map(|(_, _, digests)| digests.len())
            .collect();
        assert_eq!(history_calls, [0]);
    }
}
