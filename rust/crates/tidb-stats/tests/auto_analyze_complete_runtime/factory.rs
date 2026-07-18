// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeSet;
use tidb_stats::auto_analyze_runtime::*;

pub struct Session {
    pub version: i32,
    pub ratio: f64,
    pub enabled: bool,
    pub dynamic: bool,
}
impl SessionPort for Session {
    fn analyze_version(&self) -> i32 {
        self.version
    }
    fn auto_analyze_ratio(&self) -> f64 {
        self.ratio
    }
    fn auto_analyze_enabled(&self) -> bool {
        self.enabled
    }
    fn dynamic_partition_pruning(&self) -> bool {
        self.dynamic
    }
}
pub struct Clock(pub i64);
impl ClockPort for Clock {
    fn now_timestamp_nanos(&self) -> i64 {
        self.0
    }
}

pub fn table() -> TableMeta {
    TableMeta {
        id: 10,
        schema_name: "test".into(),
        table_name: "t".into(),
        indexes: vec![
            IndexMeta {
                id: 1,
                name: "i1".into(),
                public: true,
                columnar: false,
                special_global: false,
            },
            IndexMeta {
                id: 2,
                name: "vec".into(),
                public: true,
                columnar: true,
                special_global: false,
            },
            IndexMeta {
                id: 3,
                name: "global".into(),
                public: true,
                columnar: false,
                special_global: true,
            },
        ],
        partitions: vec![
            PartitionMeta {
                id: 11,
                name: "p0".into(),
            },
            PartitionMeta {
                id: 12,
                name: "p1".into(),
            },
        ],
    }
}

pub fn stats(id: i64) -> TableStats {
    TableStats {
        physical_id: id,
        eligible: true,
        analyzed: true,
        realtime_count: 100,
        modify_count: 60,
        analyze_row_count: 100,
        column_count: 2,
        last_analyze_timestamp_nanos: 1_000_000_000,
        analyze_version: 1,
        present_index_stats: BTreeSet::new(),
        analyzed_index_markers: BTreeSet::new(),
    }
}

#[test]
fn change_percentage_uses_analyzed_count_strict_threshold_and_zero_disable() {
    let clock = Clock(11_000_000_000);
    let mut session = Session {
        version: 2,
        ratio: 0.5,
        enabled: true,
        dynamic: true,
    };
    let factory = AnalysisJobFactory::new(&session, &clock);
    assert_eq!(factory.change_percentage(&stats(10)), 0.6);
    let mut exact = stats(10);
    exact.modify_count = 50;
    assert_eq!(factory.change_percentage(&exact), 0.0);
    let mut unanalyzed = exact.clone();
    unanalyzed.analyzed = false;
    assert_eq!(factory.change_percentage(&unanalyzed), 1.0);
    session.ratio = 0.0;
    assert_eq!(
        AnalysisJobFactory::new(&session, &clock).change_percentage(&exact),
        0.0
    );
}

#[test]
fn last_analyze_duration_and_table_size_match_factory_inputs() {
    let session = Session {
        version: 2,
        ratio: 0.5,
        enabled: true,
        dynamic: true,
    };
    let clock = Clock(11_000_000_000);
    let factory = AnalysisJobFactory::new(&session, &clock);
    assert_eq!(factory.last_analyze_duration(&stats(10)), 10_000_000_000);
    assert_eq!(
        AnalysisJobFactory::<Session, Clock>::table_size(&stats(10)),
        200.0
    );
    let mut value = stats(10);
    value.analyzed = false;
    assert_eq!(
        factory.last_analyze_duration(&value),
        30 * 60 * 1_000_000_000
    );
}

#[test]
fn nonpartitioned_index_selection_keeps_special_global_but_excludes_columnar() {
    let table = table();
    let mut value = stats(10);
    assert_eq!(
        AnalysisJobFactory::<Session, Clock>::indexes_needing_analyze(&table, &value),
        BTreeSet::from([1, 3])
    );
    value.analyzed_index_markers.insert(1);
    assert!(
        !AnalysisJobFactory::<Session, Clock>::indexes_needing_analyze(&table, &value).contains(&1)
    );
}

#[test]
fn requested_stats_version_is_carried_and_mismatch_warned() {
    let session = Session {
        version: 2,
        ratio: 0.5,
        enabled: true,
        dynamic: true,
    };
    let clock = Clock(11_000_000_000);
    let AnalysisJobRuntime::NonPartitioned(job) = AnalysisJobFactory::new(&session, &clock)
        .create_non_partitioned(&table(), &stats(10))
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(job.table_stats_version, 2);
    assert!(job.need_version_rewrite_warning);
}

#[test]
fn partition_indicators_average_only_threshold_partitions() {
    let session = Session {
        version: 2,
        ratio: 0.5,
        enabled: true,
        dynamic: true,
    };
    let clock = Clock(11_000_000_000);
    let mut low = stats(12);
    low.modify_count = 10;
    let parts = vec![
        PartitionStats {
            partition: table().partitions[0].clone(),
            stats: stats(11),
        },
        PartitionStats {
            partition: table().partitions[1].clone(),
            stats: low,
        },
    ];
    let (indicators, ids) =
        AnalysisJobFactory::new(&session, &clock).partition_indicators(&stats(10), &parts);
    assert_eq!(ids, BTreeSet::from([11]));
    assert_eq!(indicators.change_percentage, 0.6);
}

#[test]
fn partition_index_selection_skips_columnar_and_special_global_indexes() {
    let parts = vec![PartitionStats {
        partition: table().partitions[0].clone(),
        stats: stats(11),
    }];
    let indexes =
        AnalysisJobFactory::<Session, Clock>::partition_indexes_needing_analyze(&table(), &parts);
    assert_eq!(indexes.keys().copied().collect::<Vec<_>>(), vec![1]);
}

#[test]
fn daily_window_is_inclusive_crosses_midnight_and_rejects_unset() {
    assert!(AutoAnalysisTimeWindow::new(Some(60), Some(300)).contains(60));
    assert!(AutoAnalysisTimeWindow::new(Some(1320), Some(120)).contains(30));
    assert!(!AutoAnalysisTimeWindow::new(None, Some(120)).contains(30));
}
