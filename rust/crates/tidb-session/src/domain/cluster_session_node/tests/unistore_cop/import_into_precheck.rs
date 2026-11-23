// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/executor/importer/precheck_test.go`, over the embedded unistore
//! node: active jobs, empty file lists, non-empty tables, PiTR tasks and
//! changefeeds, and the global sort store's URI.

use super::*;
use std::collections::BTreeMap;
use std::sync::Mutex;
use tidb_executor_importer::{self as importer, job, precheck::PrecheckSession};

/// An in-memory etcd, standing in for Go's embedded etcd.
#[derive(Default)]
struct FakeEtcd(Mutex<BTreeMap<String, Vec<u8>>>);

impl tidb_domain_serverinfo::serverinfo_syncer::EtcdOps for FakeEtcd {
    fn lease_grant(&self, _: i64) -> Result<i64, String> {
        Ok(1)
    }
    fn lease_keep_alive_once(&self, _: i64) -> Result<(), String> {
        Ok(())
    }
    fn lease_revoke(&self, _: i64) -> Result<(), String> {
        Ok(())
    }
    fn put_with_lease(&self, key: &str, value: &[u8], _: i64) -> Result<(), String> {
        self.put(key, value)
    }
    fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        let map = self.0.lock().unwrap();
        Ok(map.iter().filter(|(k, _)| k.starts_with(prefix)).map(|(k, v)| (k.clone(), v.clone())).collect())
    }
    fn delete(&self, key: &str) -> Result<(), String> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
    fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.0.lock().unwrap().insert(key.to_owned(), value.to_vec());
        Ok(())
    }
    fn delete_prefix(&self, prefix: &str) -> Result<(), String> {
        self.0.lock().unwrap().retain(|k, _| !k.starts_with(prefix));
        Ok(())
    }
}

fn table(stack: &UnistoreClusterStack, name: &str) -> tidb_table::tables::TableCommon {
    let info = stack.factory.catalog.get_latest().v2().unwrap().table_by_name("test", name, None).unwrap().info.clone();
    let meta = info.read().clone();
    tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap()
}

fn controller(stack: &UnistoreClusterStack, path: &str, source_type: &str) -> importer::LoadDataController {
    let table = table(stack, "t");
    let plan = importer::Plan {
        db_name: "test".to_owned(),
        data_source_type: source_type.to_owned(),
        path: path.to_owned(),
        table_info: Some(table.meta().read().clone()),
        in_import_into: true,
        format: "csv".to_owned(),
        fields_terminated_by: ",".to_owned(),
        ..importer::Plan::default()
    };
    importer::LoadDataController::new(plan, table, importer::ASTArgs::default()).unwrap()
}

fn assert_pre_check_failed(result: Result<(), tidb_driver_error::DriverError>, contains: &str) {
    let error = result.unwrap_err();
    assert!(error.equal(&tidb_exeerrors::ERR_LOAD_DATA_PRE_CHECK_FAILED), "{error:?}");
    let text = error.to_mysql_error().error_string();
    assert!(text.contains(contains), "{text}");
}

/// Go `TestCheckRequirements`, but for the S3 and Azure access checks,
/// which need a fake object store.
#[test]
fn test_check_requirements() {
    let (stack, _users) = cop_backed_stack();
    let ctx = tidb_gocontext::Context::background();
    let tm = tidb_dxf_storage::TaskManager::new(stack.factory.dxf_session_pool());
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.csv");
    std::fs::write(&source, "1\n").unwrap();
    let source = source.to_str().unwrap().to_owned();
    let dir_text = dir.path().to_str().unwrap().to_owned();
    let etcd = FakeEtcd::default();
    tm.with_new_session(&mut |se| {
        let sql = |text: &str| tidb_dxf_storage::exec_sql(&ctx, se, text, &[]).map(|_| ());
        sql("create table test.t(id int primary key)").unwrap();
        let conn = se.sql_executor();
        let pre = PrecheckSession { ctx: &ctx, conn: conn.as_ref(), etcd: Some(&etcd) };
        let mut c = controller(&stack, &source, "file");
        let empty = job::ImportParameters::default();
        let table_id = c.table.meta().read().id;
        job::create_job(&ctx, conn.as_ref(), "test", "tttt", table_id, "root", "", &empty, 0).unwrap();
        let job_id = job::create_job(&ctx, conn.as_ref(), "test", "t", table_id, "root", "", &empty, 0).unwrap();
        let active = "there is active job on the target table already";
        assert_pre_check_failed(c.check_requirements(&ctx, &pre), active);
        assert_pre_check_failed(c.check_requirements_before_init_data_files(&ctx, &pre), active);
        job::cancel_job(&ctx, conn.as_ref(), job_id).unwrap();

        // The async-prepare path checks access only.
        c.plan.disable_precheck = true;
        c.check_requirements_before_init_data_files(&ctx, &pre).unwrap();
        c.plan.path = format!("{dir_text}/*.csv");
        c.check_requirements_before_init_data_files(&ctx, &pre).unwrap();
        c.plan.path = format!("{dir_text}/not-matched-*.csv");
        c.check_requirements_before_init_data_files(&ctx, &pre).unwrap();
        c.plan.path = format!("{dir_text}/missing.csv");
        let error = c.check_requirements_before_init_data_files(&ctx, &pre).unwrap_err();
        assert!(error.equal(&tidb_exeerrors::ERR_LOAD_DATA_CANT_READ));
        assert!(error.to_mysql_error().error_string().contains("Please check the file location is correct"));
        c.plan.path = source.clone();
        c.plan.disable_precheck = false;
        assert_pre_check_failed(c.check_requirements(&ctx, &pre), "No file matched");

        c.plan.total_file_size = 1;
        c.plan.thread_cnt = 1;
        c.plan.cloud_storage_uri.clear();
        sql("insert into test.t values(1)").unwrap();
        assert_pre_check_failed(c.check_requirements(&ctx, &pre), "target table is not empty");
        sql("drop table if exists test.t").unwrap();
        let error = c.check_requirements(&ctx, &pre).unwrap_err();
        assert!(error.to_mysql_error().error_string().contains("doesn't exist"));
        sql("create table test.t(id int primary key)").unwrap();

        let pitr_key = "/tidb/br-stream/info/test";
        tidb_domain_serverinfo::serverinfo_syncer::EtcdOps::put(&etcd, pitr_key, b"").unwrap();
        assert_pre_check_failed(c.check_requirements(&ctx, &pre), "found PiTR log streaming");
        c.plan.disable_precheck = true;
        c.check_requirements(&ctx, &pre).unwrap();
        c.plan.disable_precheck = false;
        tidb_domain_serverinfo::serverinfo_syncer::EtcdOps::delete(&etcd, pitr_key).unwrap();
        let cdc_key = "/tidb/cdc/testcluster/test_ns/changefeed/info/test_cf";
        tidb_domain_serverinfo::serverinfo_syncer::EtcdOps::put(&etcd, cdc_key, br#"{"state":"normal"}"#).unwrap();
        assert_pre_check_failed(c.check_requirements(&ctx, &pre), "found CDC changefeed");
        tidb_domain_serverinfo::serverinfo_syncer::EtcdOps::delete(&etcd, cdc_key).unwrap();
        c.check_requirements(&ctx, &pre).unwrap();

        // Global sort.
        c.plan.thread_cnt = 2;
        let invalid_uri = |c: &importer::LoadDataController| {
            let error = c.check_requirements(&ctx, &pre).unwrap_err();
            assert!(error.equal(&tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI), "{error:?}");
            error.to_mysql_error().error_string()
        };
        c.plan.cloud_storage_uri = ":".to_owned();
        invalid_uri(&c);
        c.plan.cloud_storage_uri =
            "s3:///path?access-key=secret-id&secret-access-key=secret-key&session-token=secret-token".to_owned();
        let text = invalid_uri(&c);
        assert!(
            text.contains("please specify the bucket for s3 in s3:///path?access-key=xxxxxx&secret-access-key=xxxxxx&session-token=xxxxxx"),
            "{text}"
        );
        assert!(!text.contains("secret-id") && !text.contains("secret-key") && !text.contains("secret-token"));
        c.plan.cloud_storage_uri = "sdsdsdsd://sdsdsdsd".to_owned();
        invalid_uri(&c);
        c.plan.cloud_storage_uri = "local:///tmp".to_owned();
        let error = c.check_requirements(&ctx, &pre).unwrap_err();
        assert!(error.to_mysql_error().error_string().contains("unsupported cloud storage uri scheme: local"));
        Ok(())
    })
    .unwrap();
}

/// Go `TestCheckRequirementsTTL`.
#[test]
fn test_check_requirements_ttl() {
    let (stack, _users) = cop_backed_stack();
    let ctx = tidb_gocontext::Context::background();
    let tm = tidb_dxf_storage::TaskManager::new(stack.factory.dxf_session_pool());
    tm.with_new_session(&mut |se| {
        let sql = |text: &str| tidb_dxf_storage::exec_sql(&ctx, se, text, &[]).map(|_| ());
        sql("create table test.t(id int primary key, created_at datetime) TTL = `created_at` + INTERVAL 1 DAY").unwrap();
        let conn = se.sql_executor();
        let pre = PrecheckSession { ctx: &ctx, conn: conn.as_ref(), etcd: None };
        let mut c = controller(&stack, "", "query");
        c.plan.disable_precheck = true;
        let ttl = "target table has TTL enabled, please disable TTL before IMPORT INTO";
        assert_pre_check_failed(c.check_requirements(&ctx, &pre), ttl);
        assert_pre_check_failed(c.check_requirements_before_init_data_files(&ctx, &pre), ttl);
        sql("alter table test.t ttl_enable = 'OFF'").unwrap();
        let mut c = controller(&stack, "", "query");
        c.plan.disable_precheck = true;
        c.check_requirements(&ctx, &pre).unwrap();
        c.check_requirements_before_init_data_files(&ctx, &pre).unwrap();
        Ok(())
    })
    .unwrap();
}
