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

    /// Go `runFetchDigestQuery`: `statements_summary` and its history (the
    /// `cluster_` tables when `query_global`), unioned so a digest just moved
    /// to the history is still found, filtered to `in_values` when given.
    fn run_fetch_digest_query(
        &self,
        exec: &dyn SqlExecutor,
        query_global: bool,
        in_values: &[String],
    ) -> Result<HashMap<String, String>, SqlExecError> {
        let context = tikv_client::util::with_internal_source_type(
            &tikv_client::trace::TraceContext::new(),
            // Go `kv.InternalTxnOthers`, client-go's `util.InternalTxnOthers`.
            tikv_client::util::INTERNAL_TXN_OTHERS,
        );
        let mut stmt = if query_global {
            "select digest, digest_text from information_schema.cluster_statements_summary union distinct \
             select digest, digest_text from information_schema.cluster_statements_summary_history"
                .to_owned()
        } else {
            "select digest, digest_text from information_schema.statements_summary union distinct \
             select digest, digest_text from information_schema.statements_summary_history"
                .to_owned()
        };
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
    /// query's.
    fn update_digest_info(&mut self, query_result: &HashMap<String, String>) {
        for (digest, text) in &mut self.sql_digests_map {
            if !text.is_empty() {
                continue;
            }
            if let Some(sql_text) = query_result.get(digest) {
                text.clone_from(sql_text);
            }
        }
    }

    /// Go `RetrieveLocal`: the texts this node's summaries know.
    ///
    /// # Errors
    /// The summary query's failure.
    pub fn retrieve_local(&mut self, exec: &dyn SqlExecutor) -> Result<(), SqlExecError> {
        if self.sql_digests_map.is_empty() {
            return Ok(());
        }
        let query_result = if self.sql_digests_map.len() <= self.fetch_all_limit {
            let in_values: Vec<String> = self.sql_digests_map.keys().cloned().collect();
            let query_result = self.run_fetch_digest_query(exec, false, &in_values)?;
            if query_result.len() == self.sql_digests_map.len() {
                self.sql_digests_map = query_result;
                return Ok(());
            }
            query_result
        } else {
            self.run_fetch_digest_query(exec, false, &[])?
        };
        self.update_digest_info(&query_result);
        Ok(())
    }

    /// Go `RetrieveGlobal`: [`Self::retrieve_local`], then the whole
    /// cluster's summaries for the digests still unknown.
    ///
    /// # Errors
    /// The summary query's failure.
    pub fn retrieve_global(&mut self, exec: &dyn SqlExecutor) -> Result<(), SqlExecError> {
        self.retrieve_local(exec)?;
        // In some unit-test environments the cluster tables cannot be read,
        // and querying them blocks for tens of seconds.
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/expression/sqlDigestRetrieverSkipRetrieveGlobal").is_some() {
            return Ok(());
        }
        let unknown_digests: Vec<String> = self
            .sql_digests_map
            .iter()
            .filter(|(_, text)| text.is_empty())
            .map(|(digest, _)| digest.clone())
            .collect();
        if unknown_digests.is_empty() {
            return Ok(());
        }
        let query_result = if self.sql_digests_map.len() <= self.fetch_all_limit {
            self.run_fetch_digest_query(exec, true, &unknown_digests)?
        } else {
            self.run_fetch_digest_query(exec, true, &[])?
        };
        self.update_digest_info(&query_result);
        Ok(())
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

    /// Go's `mockLocalData` / `mockGlobalData`, answered through the
    /// restricted executor the retriever really calls: the statements
    /// summary of this node, or of the cluster for the `cluster_` tables,
    /// filtered to the `WHERE IN` digests when the query names them.
    struct SummaryTables {
        local: HashMap<String, String>,
        global: HashMap<String, String>,
        queries: Mutex<Vec<(bool, usize)>>,
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
            let global = sql.contains("cluster_statements_summary");
            assert_eq!(sql.contains(" where digest in ("), !arguments.is_empty(), "{sql}");
            self.queries.lock().unwrap().push((global, arguments.len()));
            let table = if global { &self.global } else { &self.local };
            let wanted: Option<Vec<String>> = (!arguments.is_empty()).then(|| {
                arguments
                    .iter()
                    .map(|argument| match argument {
                        SqlArg::String(bytes) => String::from_utf8(bytes.to_vec()).unwrap(),
                        _ => panic!("a digest is a string argument"),
                    })
                    .collect()
            });
            let rows = table
                .iter()
                .filter(|(digest, _)| wanted.as_ref().is_none_or(|wanted| wanted.contains(digest)))
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

    /// Go `TestSQLDigestTextRetriever`, with its mock data served by the
    /// summary tables the retriever queries.
    #[test]
    fn test_sql_digest_text_retriever() {
        let tables = SummaryTables {
            local: map(&[("digest1", "text1"), ("digest2", "text2"), ("digest6", "text6")]),
            global: map(&[("digest2", "text2"), ("digest3", "text3"), ("digest4", "text4"), ("digest7", "text7")]),
            queries: Mutex::default(),
        };
        let requested = map(&[("digest1", ""), ("digest2", ""), ("digest3", ""), ("digest4", ""), ("digest5", "")]);
        let expected_local = map(&[("digest1", "text1"), ("digest2", "text2"), ("digest3", ""), ("digest4", ""), ("digest5", "")]);
        let expected_global =
            map(&[("digest1", "text1"), ("digest2", "text2"), ("digest3", "text3"), ("digest4", "text4"), ("digest5", "")]);

        let mut r = SqlDigestTextRetriever::new();
        r.sql_digests_map = requested.clone();
        r.retrieve_local(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_local);
        r.sql_digests_map = requested.clone();
        r.retrieve_global(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_global);
        // Listed digests: the local query names all five, the global one the
        // three still unknown.
        assert_eq!(*tables.queries.lock().unwrap(), [(false, 5), (false, 5), (true, 3)]);

        tables.queries.lock().unwrap().clear();
        r.fetch_all_limit = 1;
        r.sql_digests_map = requested.clone();
        r.retrieve_local(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_local);
        r.sql_digests_map = requested;
        r.retrieve_global(&tables).unwrap();
        assert_eq!(r.sql_digests_map, expected_global);
        // Past `fetchAllLimit` every query fetches all rows.
        assert_eq!(*tables.queries.lock().unwrap(), [(false, 0), (false, 0), (true, 0)]);
    }
}
