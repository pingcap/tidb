use super::factory::table;
// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeSet;
use tidb_stats::auto_analyze_runtime::*;

struct Info(TableMeta);
impl InfoSchemaPort for Info {
    fn table_by_id(&self, _: i64) -> Option<TableMeta> {
        Some(self.0.clone())
    }
}
struct Sql {
    executed: Vec<SqlStatement>,
}
impl SqlPort for Sql {
    fn query_optional_f64(&mut self, _: &SqlStatement) -> RuntimeResult<Option<f64>> {
        Ok(None)
    }
    fn query_optional_i64(&mut self, _: &SqlStatement) -> RuntimeResult<Option<i64>> {
        Ok(None)
    }
    fn execute(&mut self, s: &SqlStatement) -> RuntimeResult<()> {
        self.executed.push(s.clone());
        Ok(())
    }
}
struct Hooks;
impl JobHookPort for Hooks {
    fn success(&mut self, _: &AnalysisJobRuntime) {}
    fn failure(&mut self, _: &AnalysisJobRuntime, _: bool) {}
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
    AnalysisJobRuntime::StaticPartitioned(StaticPartitionedJob {
        global_table_id: 10,
        partition_id: 11,
        index_ids: indexes,
        table_stats_version: 2,
        need_version_rewrite_warning: false,
        indicators: JobIndicators::default(),
        weight: 0.0,
        schema_name: "s".into(),
        table_name: "t".into(),
        partition_name: "p0".into(),
        index_names: vec![],
    })
}

#[test]
fn static_partition_sql_is_source_exact() {
    let AnalysisJobRuntime::StaticPartitioned(j) = job(BTreeSet::new()) else {
        panic!()
    };
    assert_eq!(
        j.partition_statement().sql,
        "analyze table %n.%n partition %n"
    );
}
#[test]
fn static_partition_index_sql_is_source_exact() {
    let AnalysisJobRuntime::StaticPartitioned(j) = job(BTreeSet::new()) else {
        panic!()
    };
    assert_eq!(
        j.index_statement("i").sql,
        "analyze table %n.%n partition %n index %n"
    );
}
#[test]
fn analyze_static_partition_executes_table_path() {
    let j = job(BTreeSet::new());
    let mut sql = Sql { executed: vec![] };
    let mut stats = Stats;
    let mut hooks = Hooks;
    j.analyze(&mut sql, &mut stats, &mut hooks, 8).unwrap();
    assert_eq!(sql.executed.len(), 1);
}
#[test]
fn analyze_static_indexes_executes_only_first() {
    let mut j = job(BTreeSet::from([1]));
    let AnalysisJobRuntime::StaticPartitioned(inner) = &mut j else {
        panic!()
    };
    inner.index_names = vec!["i1".into(), "i2".into()];
    let mut sql = Sql { executed: vec![] };
    let mut stats = Stats;
    let mut hooks = Hooks;
    j.analyze(&mut sql, &mut stats, &mut hooks, 8).unwrap();
    assert_eq!(sql.executed.len(), 1);
}
#[test]
fn static_validation_uses_partition_identity_and_rejects_missing_partition() {
    let mut j = job(BTreeSet::from([1]));
    let mut sql = Sql { executed: vec![] };
    let mut hooks = Hooks;
    assert!(
        j.validate_and_prepare(&Info(table()), &mut sql, &mut hooks)
            .unwrap()
            .0
    );
    assert_eq!(j.table_id(), 11);
    let mut absent = table();
    absent.partitions.clear();
    assert!(
        !j.validate_and_prepare(&Info(absent), &mut sql, &mut hooks)
            .unwrap()
            .0
    );
}
