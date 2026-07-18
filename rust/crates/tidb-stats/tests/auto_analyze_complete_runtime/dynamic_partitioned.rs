use super::factory::table;
// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use tidb_stats::auto_analyze_runtime::*;
struct Info;
impl InfoSchemaPort for Info {
    fn table_by_id(&self, _: i64) -> Option<TableMeta> {
        Some(table())
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
fn job(indexes: bool) -> AnalysisJobRuntime {
    AnalysisJobRuntime::DynamicPartitioned(DynamicPartitionedJob {
        global_table_id: 10,
        partition_ids: BTreeSet::from([11, 12]),
        partition_index_ids: if indexes {
            BTreeMap::from([(1, vec![11, 12])])
        } else {
            BTreeMap::new()
        },
        table_stats_version: 2,
        need_version_rewrite_warning: false,
        indicators: JobIndicators::default(),
        weight: 0.0,
        schema_name: "s".into(),
        table_name: "t".into(),
        partition_names: vec!["p0".into(), "p1".into()],
        partition_index_names: if indexes {
            BTreeMap::from([("i1".into(), vec!["p0".into(), "p1".into()])])
        } else {
            BTreeMap::new()
        },
    })
}
#[test]
fn dynamic_partition_analysis_batches_exact_placeholders() {
    let AnalysisJobRuntime::DynamicPartitioned(j) = job(false) else {
        panic!()
    };
    let mut sql = Sql { executed: vec![] };
    j.analyze(&mut sql, 1).unwrap();
    assert_eq!(sql.executed.len(), 2);
    assert_eq!(sql.executed[0].sql, "analyze table %n.%n partition %n");
}
#[test]
fn dynamic_index_analysis_stops_after_first_index_and_batches() {
    let AnalysisJobRuntime::DynamicPartitioned(j) = job(true) else {
        panic!()
    };
    let mut sql = Sql { executed: vec![] };
    j.analyze(&mut sql, 2).unwrap();
    assert_eq!(sql.executed.len(), 1);
    assert!(sql.executed[0].sql.ends_with(" index %n"));
}
#[test]
fn dynamic_validation_resolves_partition_and_index_names() {
    let mut j = job(true);
    let mut sql = Sql { executed: vec![] };
    let mut hooks = Hooks;
    assert!(
        j.validate_and_prepare(&Info, &mut sql, &mut hooks)
            .unwrap()
            .0
    );
    let AnalysisJobRuntime::DynamicPartitioned(inner) = j else {
        panic!()
    };
    assert_eq!(inner.partition_names, vec!["p0", "p1"]);
    assert_eq!(inner.partition_index_names["i1"], vec!["p0", "p1"]);
}
#[test]
fn dynamic_validation_work_is_linear_in_metadata_size() {
    let mut j = job(true);
    let mut sql = Sql { executed: vec![] };
    let mut hooks = Hooks;
    for _ in 0..100 {
        assert!(
            j.validate_and_prepare(&Info, &mut sql, &mut hooks)
                .unwrap()
                .0
        );
    }
    assert_eq!(j.table_id(), 10);
}
