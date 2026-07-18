use super::factory::table;
// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeSet;
use tidb_stats::auto_analyze_runtime::*;

struct Info(Option<TableMeta>);
impl InfoSchemaPort for Info {
    fn table_by_id(&self, _: i64) -> Option<TableMeta> {
        self.0.clone()
    }
}
struct Sql {
    executed: Vec<SqlStatement>,
    fail: bool,
}
impl SqlPort for Sql {
    fn query_optional_f64(&mut self, _: &SqlStatement) -> RuntimeResult<Option<f64>> {
        Ok(None)
    }
    fn query_optional_i64(&mut self, _: &SqlStatement) -> RuntimeResult<Option<i64>> {
        Ok(None)
    }
    fn execute(&mut self, statement: &SqlStatement) -> RuntimeResult<()> {
        if self.fail {
            Err(RuntimeError("execute".into()))
        } else {
            self.executed.push(statement.clone());
            Ok(())
        }
    }
}
struct Hooks {
    success: usize,
    retry: Vec<bool>,
}
impl JobHookPort for Hooks {
    fn success(&mut self, _: &AnalysisJobRuntime) {
        self.success += 1;
    }
    fn failure(&mut self, _: &AnalysisJobRuntime, retry: bool) {
        self.retry.push(retry);
    }
}
struct Stats;
impl StatisticsPort for Stats {
    fn stats_by_id(&self, _: i64) -> Option<TableStats> {
        None
    }
    fn locked_table_ids(&self) -> RuntimeResult<BTreeSet<i64>> {
        Ok(BTreeSet::new())
    }
    fn update_after_analyze(&mut self, _: i64) -> RuntimeResult<()> {
        Ok(())
    }
}

fn job(indexes: BTreeSet<i64>) -> AnalysisJobRuntime {
    AnalysisJobRuntime::NonPartitioned(NonPartitionedJob {
        table_id: 10,
        index_ids: indexes,
        table_stats_version: 2,
        need_version_rewrite_warning: false,
        indicators: JobIndicators::default(),
        weight: 0.0,
        schema_name: "s".into(),
        table_name: "t".into(),
        index_names: vec![],
    })
}

#[test]
fn table_sql_is_source_exact() {
    let AnalysisJobRuntime::NonPartitioned(j) = job(BTreeSet::new()) else {
        panic!()
    };
    assert_eq!(j.table_statement().sql, "analyze table %n.%n");
}
#[test]
fn index_sql_is_source_exact() {
    let AnalysisJobRuntime::NonPartitioned(j) = job(BTreeSet::new()) else {
        panic!()
    };
    assert_eq!(j.index_statement("i").sql, "analyze table %n.%n index %n");
}
#[test]
fn analyze_table_executes_and_publishes_success() {
    let j = job(BTreeSet::new());
    let mut sql = Sql {
        executed: vec![],
        fail: false,
    };
    let mut stats = Stats;
    let mut hooks = Hooks {
        success: 0,
        retry: vec![],
    };
    j.analyze(&mut sql, &mut stats, &mut hooks, 8).unwrap();
    assert_eq!(sql.executed.len(), 1);
    assert_eq!(hooks.success, 1);
}
#[test]
fn analyze_indexes_executes_only_first_index_for_version_two() {
    let mut j = job(BTreeSet::from([1]));
    let AnalysisJobRuntime::NonPartitioned(inner) = &mut j else {
        panic!()
    };
    inner.index_names = vec!["i1".into(), "i2".into()];
    let mut sql = Sql {
        executed: vec![],
        fail: false,
    };
    let mut stats = Stats;
    let mut hooks = Hooks {
        success: 0,
        retry: vec![],
    };
    j.analyze(&mut sql, &mut stats, &mut hooks, 8).unwrap();
    assert_eq!(sql.executed.len(), 1);
    assert!(sql.executed[0]
        .params
        .iter()
        .any(|p| p == &SqlValue::Identifier("i1".into())));
}
#[test]
fn validate_prepares_schema_table_and_index_names() {
    let mut j = job(BTreeSet::from([1]));
    let mut sql = Sql {
        executed: vec![],
        fail: false,
    };
    let mut hooks = Hooks {
        success: 0,
        retry: vec![],
    };
    assert!(
        j.validate_and_prepare(&Info(Some(table())), &mut sql, &mut hooks)
            .unwrap()
            .0
    );
    let AnalysisJobRuntime::NonPartitioned(inner) = j else {
        panic!()
    };
    assert_eq!(inner.index_names, vec!["i1"]);
}
#[test]
fn missing_table_is_nonretryable_and_failed_analysis_is_retryable() {
    let mut j = job(BTreeSet::new());
    let mut sql = Sql {
        executed: vec![],
        fail: false,
    };
    let mut hooks = Hooks {
        success: 0,
        retry: vec![],
    };
    assert!(
        !j.validate_and_prepare(&Info(None), &mut sql, &mut hooks)
            .unwrap()
            .0
    );
    assert_eq!(hooks.retry, vec![false]);
}
