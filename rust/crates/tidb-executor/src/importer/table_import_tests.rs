//! Go `pkg/executor/importer/table_import_test.go` and the
//! `TestVerifyChecksum` / `TestAdjustDiskQuota` cases.

use super::table_import::*;
use super::*;

fn controller(total_real_size: i64, max_engine_size: i64, cloud: &str, nodes: usize) -> LoadDataController {
    let plan = Plan {
        max_engine_size,
        cloud_storage_uri: cloud.to_owned(),
        in_import_into: true,
        format: "csv".to_owned(),
        path: "/x.csv".to_owned(),
        data_source_type: "file".to_owned(),
        fields_terminated_by: ",".to_owned(),
        ..Plan::default()
    };
    let table = tidb_table::tables::TableCommon::new(1, Vec::new());
    let mut c = LoadDataController::new(plan, table, ASTArgs::default()).unwrap();
    c.total_real_size = total_real_size;
    c.set_execute_node_cnt(nodes);
    c
}

/// Go `TestCalculateSubtaskCnt` and `TestLoadDataControllerGetAdjustedMaxEngineSize`.
#[test]
fn test_calculate_subtask_cnt() {
    for (total, max, nodes, url, cnt, adjusted) in [
        (1, 500, 0, "", 1, 1),
        (499, 500, 1, "", 1, 499),
        (500, 500, 2, "", 1, 500),
        (749, 500, 3, "", 1, 749),
        (750, 500, 4, "", 2, 375),
        (1249, 500, 5, "", 2, 625),
        (1250, 500, 6, "", 3, 417),
        (100, 30, 7, "", 3, 34),
        (1, 500, 0, "url", 1, 1),
        (499, 500, 1, "url", 1, 499),
        (500, 500, 2, "url", 2, 250),
        (749, 500, 3, "url", 3, 250),
        (750, 500, 4, "url", 4, 188),
        (1249, 500, 5, "url", 5, 250),
        (1250, 500, 6, "url", 6, 209),
        (100, 30, 2, "url", 4, 25),
        (400, 99, 3, "url", 6, 67),
        (500, 100, 5, "url", 5, 100),
        (500, 200, 5, "url", 5, 100),
    ] {
        let c = controller(total, max, url, nodes);
        assert_eq!(c.calculate_subtask_cnt(), cnt, "{total}/{max}/{nodes}/{url}");
        assert_eq!(c.get_adjusted_max_engine_size(), adjusted, "{total}/{max}/{nodes}/{url}");
    }
    assert_eq!(controller(500, 100, "url", 1).get_adjusted_max_engine_size(), 100);
}

/// Go `TestCalculateSubtaskCntUsesRealSizeNotFileSize`.
#[test]
fn test_calculate_subtask_cnt_uses_real_size() {
    let mut c = controller(1500, 500, "", 1);
    c.plan.total_file_size = 100;
    assert_eq!((c.calculate_subtask_cnt(), c.get_adjusted_max_engine_size()), (3, 500));
    c.plan.total_file_size = 1500;
    c.total_real_size = 100;
    assert_eq!((c.calculate_subtask_cnt(), c.get_adjusted_max_engine_size()), (1, 100));
}

/// Go `TestPrepareSortDir`.
#[test]
fn test_prepare_sort_dir() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = tidb_config::config_tree::config::Config::default();
    cfg.port = 4000;
    cfg.temp_dir = dir.path().to_str().unwrap().to_owned();
    let import_dir = dir.path().join("import-4000");
    let sort_dir = prepare_sort_dir("1", &cfg).unwrap();
    assert_eq!(sort_dir, import_dir.join("1"));
    assert!(import_dir.is_dir() && !sort_dir.exists());
    std::fs::remove_dir(&import_dir).unwrap();
    std::fs::File::create(&import_dir).unwrap();
    assert_eq!(prepare_sort_dir("2", &cfg).unwrap(), import_dir.join("2"));
    assert!(import_dir.is_dir());
    let sort_dir = prepare_sort_dir("3", &cfg).unwrap();
    assert_eq!(sort_dir, import_dir.join("3"));
    std::fs::create_dir(&sort_dir).unwrap();
    assert_eq!(prepare_sort_dir("3", &cfg).unwrap(), sort_dir);
    assert!(import_dir.is_dir() && !sort_dir.exists());
}

/// Go `TestVerifyChecksum`'s level handling.
#[test]
fn test_verify_checksum() {
    use tidb_ingestor_ingestctrl::checksum::RemoteChecksum;
    let local = tidb_lightning_verification::KvChecksum::make(1, 1, 1);
    let mut plan = Plan { checksum: PostOpLevel::Off, ..Plan::default() };
    verify_checksum(&plan, &local, || unreachable!("checksum off")).unwrap();
    plan.checksum = PostOpLevel::Required;
    let equal = || Ok(RemoteChecksum { checksum: 1, total_kvs: 1, total_bytes: 1, ..Default::default() });
    let unequal = || Ok(RemoteChecksum { checksum: 2, total_kvs: 2, total_bytes: 2, ..Default::default() });
    verify_checksum(&plan, &local, equal).unwrap();
    let error = crate::DriverError::from(verify_checksum(&plan, &local, unequal).unwrap_err()).to_mysql_error();
    assert_eq!(
        error.error_string(),
        "[Lighting:Restore:ErrChecksumMismatch]checksum mismatched remote vs local => (checksum: 2 vs 1) (total_kvs: 2 vs 1) (total_bytes:2 vs 1)"
    );
    let failed = || Err(plain("mock remote checksum error"));
    assert!(verify_checksum(&plan, &local, failed).is_err());
    plan.checksum = PostOpLevel::Optional;
    verify_checksum(&plan, &local, unequal).unwrap();
    verify_checksum(&plan, &local, failed).unwrap();
}

/// Go `TestGetBackoffWeight`'s floor at `DefaultBackoffWeight`.
#[test]
fn test_get_backoff_weight() {
    let mut plan = Plan::default();
    assert_eq!(get_backoff_weight(&plan), 30);
    plan.important_sys_vars.insert("tidb_backoff_weight".to_owned(), "2".to_owned());
    assert_eq!(get_backoff_weight(&plan), 30);
    plan.important_sys_vars.insert("tidb_backoff_weight".to_owned(), "100".to_owned());
    assert_eq!(get_backoff_weight(&plan), 100);
}
