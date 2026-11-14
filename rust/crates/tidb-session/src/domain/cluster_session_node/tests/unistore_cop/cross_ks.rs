// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use super::*;
use crate::domain::cluster_session_node::{create_cross_ks_session, CrossKsSessionResources};

/// Go `TestManager`: target SQL, no user tables, no DDL, and no attached Domain.
/// This pins session construction; target-store/etcd acquisition is tested separately.
#[test]
fn test_manager() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let (target, _users) = cop_backed_stack();
    let mut ordinary = target.factory.open_system_session().unwrap();
    rows(&mut ordinary, "create database if not exists test");
    rows(&mut ordinary, "create table test.t(id int)");
    // Load the target's real metadata, applying Go's system-database and
    // virtual-schema isolation. Named-codec transport is a separate gate.
    let mut snapshot = tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot::new(
        target
            .factory
            .transactions
            .open_snapshot("default")
            .unwrap(),
    );
    let loaded = tidb_infoschema_issyncer::cross_ks::load_info_schema(&mut snapshot).unwrap();
    let catalog = tidb_domain_core::cluster_session::cluster_session_catalog_for_cross_ks(
        &loaded,
        target.factory.auto_ids.as_ref(),
        None,
    )
    .catalog;
    let pins = Arc::new(tidb_ddl::schema_sync::SchemaPinRegistry::default());
    use tidb_infoschema_isvalidator::Validator as _;
    let validator = Arc::new(tidb_infoschema_isvalidator::SchemaValidator::new(
        Duration::from_secs(1),
    ));
    validator.set_mdl_enabled(true);
    validator.update(
        target.factory.transactions.fresh_timestamp().unwrap(),
        0,
        catalog.schema_meta_version(),
        None,
    );
    let resources = Arc::new(CrossKsSessionResources {
        keyspace: target.factory.store_keyspace().to_owned(),
        coordinator: Arc::default(),
        transactions: target.factory.transactions.clone(),
        meta_store: target.factory.meta_store.get().unwrap().clone(),
        info_cache: Arc::new(
            tidb_infoschema_issyncer::catalog_watch::InfoCache::new_for_cross_ks(catalog),
        ),
        schema_validator: validator,
        schema_pins: pins.clone(),
        kv_client: target.factory.kv_client.clone(),
        mpp_client: target.factory.mpp_client.clone(),
        global_vars: target.factory.global_vars.clone(),
    });
    assert!(create_cross_ks_session(&resources.keyspace, resources.clone(), 991).is_err());
    let mut session = create_cross_ks_session("other-keyspace", resources.clone(), 992).unwrap();
    assert!(session.is_cross_ks());
    assert!(session.domain_services().is_none());
    assert!(session.node().domain.is_none());
    rows(
        &mut session,
        r#"INSERT INTO mysql.tidb_import_jobs
      (table_schema,table_name,table_id,group_key,created_by,parameters,source_file_size,status,step)
      VALUES ('db','tbl',1,'','','{"file-location":"","format":""}',1,'pending','')"#,
    );
    assert_eq!(
        displayed(rows(
            &mut ordinary,
            "select count(1) from mysql.tidb_import_jobs"
        )),
        vec![vec!["1"]]
    );
    let error = session
        .node_conn()
        .execute("select * from test.t")
        .err()
        .unwrap();
    assert_eq!(error.code, 1146);
    let error = session
        .node_conn()
        .execute("select * from information_schema.tables")
        .err()
        .unwrap();
    assert_eq!(error.code, 1146);
    let error = session
        .node_conn()
        .execute("create table test.t2(id int)")
        .err()
        .unwrap();
    assert!(error
        .message
        .contains("DDL is not supported in cross keyspace session"));
    rows(&mut session, "begin");
    rows(&mut session, "select count(1) from mysql.tidb_import_jobs");
    let (_, table) = target
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("mysql", "tidb_import_jobs")
        .unwrap();
    assert!(pins.blocks(i64::MAX, &[table.read().id]));
    rows(&mut session, "commit");
    assert!(!pins.blocks(i64::MAX, &[table.read().id]));
    let pool = crate::domain::cluster_session_node::new_cross_ks_session_pool(
        "other-keyspace".into(),
        resources.clone(),
    );
    assert_eq!(pool.capacity(), 5);
    assert_eq!(resources.coordinator.internal_session_count(), 0);
    let borrowed = pool.get().unwrap();
    assert_eq!(resources.coordinator.internal_session_count(), 1);
    borrowed
        .with_session_context(|context| -> Result<(), tidb_syssession::SysSessionError> {
            let rows = tidb_domain_core::sys_session::exec_sql(
                context,
                "meta",
                "select count(1) from mysql.tidb_import_jobs",
                &[],
            )
            .map_err(tidb_syssession::SysSessionError::new)?;
            assert_eq!(rows[0][0], tidb_datatype::Datum::Int(1));
            assert!(context.contains_internal_session());
            Ok(())
        })
        .unwrap();
    pool.put(&borrowed);
    assert_eq!(resources.coordinator.internal_session_count(), 0);
    let borrowed = pool.get().unwrap();
    assert_eq!(resources.coordinator.internal_session_count(), 1);
    borrowed.close();
    assert_eq!(resources.coordinator.internal_session_count(), 0);
    pool.close();
    // Go TestManager uses beforeGetStore and skipCloseStore to give its
    // runtime an already bootstrapped mock store owned by the test Domain.
    let _skip_close = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
        |value| {
            *value.downcast_mut::<bool>().unwrap() = false;
        },
    );
    let info = target
        .factory
        .server_info
        .as_ref()
        .unwrap()
        .get_local_server_info();
    let runtime = crate::domain::cross_ks::SessionManager::create_session_manager(
        "other-keyspace",
        target.opener.clone(),
        None,
        info,
        Duration::from_secs(1),
        target.factory.kv_client.clone(),
        target.factory.mpp_client.clone(),
    )
    .unwrap();
    assert!(runtime.info_cache().is_cross_ks());
    assert!(runtime
        .info_cache()
        .get_latest()
        .schema_by_name("test")
        .is_none());
    rows(&mut ordinary, "create table test.t_after_runtime(id int)");
    let version = target.factory.catalog.get_latest().schema_meta_version();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while runtime.info_cache().get_latest().schema_meta_version() < version {
        assert!(
            std::time::Instant::now() < deadline,
            "cross-KS schema loop did not advance"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(runtime
        .info_cache()
        .get_latest()
        .schema_by_name("test")
        .is_none());
    runtime.close();
    assert_eq!(runtime.coordinator().internal_session_count(), 0);
    rows(&mut ordinary, "select count(1) from mysql.tidb_import_jobs");
}

/// Go `TestDomainAlterTableModeInKeyspaceSubmitOnly`: the target's existing
/// owner executes a job enqueued by the separate, domain-free target runtime.
#[test]
fn test_domain_alter_table_mode_in_keyspace_submit_only() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let (target, _users) = cop_backed_stack();
    let mut ordinary = target.factory.open_system_session().unwrap();
    rows(&mut ordinary, "create database if not exists test");
    rows(&mut ordinary, "create table test.t_mode(id int)");
    let catalog = target.factory.catalog.get_latest();
    let schema = catalog.schema_by_name("test").unwrap();
    let (_, table) = catalog.table_info_by_name("test", "t_mode").unwrap();
    let mut request = tidb_model::AlterTableModeTarget {
        schema_id: schema.read().id,
        schema_name: tidb_ast::CiString::new("test"),
        table_id: table.read().id,
        table_name: tidb_ast::CiString::new("t_mode"),
        target_mode: tidb_model::TableMode::IMPORT,
        ..Default::default()
    };
    let _skip_close = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
        |value| {
            *value.downcast_mut::<bool>().unwrap() = false;
        },
    );
    let runtime = crate::domain::cross_ks::SessionManager::create_session_manager(
        "other-keyspace",
        target.opener.clone(),
        None,
        target
            .factory
            .server_info
            .as_ref()
            .unwrap()
            .get_local_server_info(),
        Duration::from_secs(1),
        target.factory.kv_client.clone(),
        target.factory.mpp_client.clone(),
    )
    .unwrap();
    let (context, _cancel) = tidb_gocontext::Context::with_timeout(
        &tidb_gocontext::Context::background(),
        Duration::from_secs(30),
    );
    runtime
        .ddl_client
        .alter_table_mode(&context, &request)
        .unwrap();
    assert_eq!(
        runtime
            .ddl_client
            .resolve_alter_table_mode_target(&context, &request)
            .unwrap()
            .current_mode,
        tidb_model::TableMode::IMPORT
    );
    let global_id = || {
        let mut snapshot = tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot::new(
            target
                .factory
                .transactions
                .open_snapshot("default")
                .unwrap(),
        );
        use tidb_infoschema::cluster_catalog::MetaSnapshot as _;
        snapshot
            .get(&tidb_meta::key::next_global_id_kv_key())
            .unwrap()
    };
    let id = global_id();
    runtime
        .ddl_client
        .alter_table_mode(&context, &request)
        .unwrap();
    assert_eq!(global_id(), id, "Go's no-op does not allocate a job ID");
    request.schema_name = tidb_ast::CiString::new("renamed_test");
    assert!(runtime
        .ddl_client
        .alter_table_mode(&context, &request)
        .unwrap_err()
        .contains("expected schema name"));
    request.schema_name = tidb_ast::CiString::new("test");
    request.table_name = tidb_ast::CiString::new("renamed_t_mode");
    assert!(runtime
        .ddl_client
        .alter_table_mode(&context, &request)
        .unwrap_err()
        .contains("expected table name"));
    request.table_name = tidb_ast::CiString::new("t_mode");
    request.target_mode = tidb_model::TableMode::NORMAL;
    runtime
        .ddl_client
        .alter_table_mode(&context, &request)
        .unwrap();
    assert_eq!(
        runtime
            .ddl_client
            .resolve_alter_table_mode_target(&context, &request)
            .unwrap()
            .current_mode,
        tidb_model::TableMode::NORMAL
    );
    assert!(runtime.ddl_client.server_state.watch_chan().is_none());
    let (deadline, _cancel) = tidb_gocontext::Context::with_timeout(
        &tidb_gocontext::Context::background(),
        Duration::from_millis(20),
    );
    assert_eq!(
        runtime
            .ddl_client
            .wait_ddl_finished(&deadline, i64::MAX)
            .unwrap_err(),
        "context deadline exceeded"
    );
    runtime.close();
}

/// Go `TestDomainAcquireKSRuntimeHandle`: independently opened named stores,
/// Domain acquisition, and stable target pool identity across handles.
#[test]
fn test_domain_acquire_ks_runtime_handle() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let _skip_close = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
        |value| {
            *value.downcast_mut::<bool>().unwrap() = false;
        },
    );
    let serving =
        crate::domain::mock_store::MockStoreDomain::create_for_keyspace("SYSTEM").unwrap();
    let target =
        crate::domain::mock_store::MockStoreDomain::create_for_keyspace("ks-runtime-domain")
            .unwrap();
    assert_eq!(serving.factory().store_keyspace(), "SYSTEM");
    assert_eq!(target.factory().store_keyspace(), "ks-runtime-domain");
    let mut ordinary = target.new_session().unwrap();
    rows(&mut ordinary, "create database if not exists test");
    rows(&mut ordinary, "create table test.t(id int)");
    let creates = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    serving
        .factory()
        .install_cross_ks_factory(Arc::new({
            let target = target.clone();
            let creates = creates.clone();
            let mut info = serving
                .factory()
                .server_info
                .as_ref()
                .unwrap()
                .get_local_server_info();
            info.static_info.id = tidb_domain_serverinfo::serverinfo_syncer::new_node_id();
            info.static_info.server_id_getter = Some(Arc::new(|| 0));
            move |ks| {
                assert_eq!(ks, "ks-runtime-domain");
                creates.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                crate::domain::cross_ks::SessionManager::create_session_manager(
                    "SYSTEM",
                    target.transaction_opener(),
                    None,
                    info.clone(),
                    Duration::from_secs(1),
                    target.factory().kv_client.clone(),
                    target.factory().mpp_client.clone(),
                )
                .map(|runtime| {
                    runtime
                        as Arc<
                            dyn tidb_domain_crossks::SessionManager<
                                dyn tidb_dxf_storage::SessionContext,
                            >,
                        >
                })
            }
        }))
        .unwrap();
    use tidb_domain_sqlsvrapi::Server as _;
    let server = tidb_domain_core::domain::sqlsvr::DomainSqlServer(serving.factory().clone());
    let handle = server
        .acquire_ks_runtime("ks-runtime-domain", "test/domain-runtime-handle")
        .unwrap();
    assert_eq!(handle.store_keyspace(), "ks-runtime-domain");
    let second = server
        .acquire_ks_runtime("ks-runtime-domain", "test/second")
        .unwrap();
    assert!(Arc::ptr_eq(
        &handle.sys_session_pool(),
        &second.sys_session_pool()
    ));
    assert_eq!(creates.load(std::sync::atomic::Ordering::SeqCst), 1);
    let raw_store = serving.factory().get_ks_store("ks-runtime-domain").unwrap();
    assert!(Arc::ptr_eq(&raw_store, &handle.store()));
    assert!(Arc::ptr_eq(&raw_store, &second.store()));
    let raw_cache = serving
        .factory()
        .get_ks_info_cache("ks-runtime-domain")
        .unwrap();
    assert!(raw_cache.is_cross_ks());
    assert!(Arc::ptr_eq(
        &raw_cache,
        &serving
            .factory()
            .get_ks_info_cache("ks-runtime-domain")
            .unwrap()
    ));
    assert!(serving.factory().get_ks_store("SYSTEM").is_err());
    assert!(serving.factory().get_ks_info_cache("SYSTEM").is_err());
    assert!(server
        .acquire_ks_runtime("ks-runtime-domain", "test/second")
        .is_err());
    let pool = handle.sys_session_pool();
    let session = pool.get().unwrap();
    session
        .with_session_context(|context| -> tidb_syssession::Result<()> {
            assert_eq!(context.store_keyspace(), "ks-runtime-domain");
            tidb_domain_core::sys_session::exec_sql(
                context,
                "meta",
                "select count(1) from mysql.tidb_import_jobs",
                &[],
            )
            .map_err(tidb_syssession::SysSessionError::new)?;
            Ok(())
        })
        .unwrap();
    pool.put(&session);
    handle.release();
    second.release();
    serving.factory().get_cross_ks_mgr().unwrap().close();
    assert!(pool.get().is_err());
    rows(&mut ordinary, "select count(1) from mysql.tidb_import_jobs");
    // Go Domain.Init/loadSysKSInfoSchema and InitDistTaskLoop use the
    // SYSTEM runtime for a user keyspace, without starting a local DXF loop.
    target
        .factory()
        .install_cross_ks_factory(Arc::new({
            let opener = serving.transaction_opener();
            let kv = serving.factory().kv_client.clone();
            let mpp = serving.factory().mpp_client.clone();
            let mut info = target
                .factory()
                .server_info
                .as_ref()
                .unwrap()
                .get_local_server_info();
            info.static_info.id = tidb_domain_serverinfo::serverinfo_syncer::new_node_id();
            move |ks| {
                assert_eq!(ks, "SYSTEM");
                crate::domain::cross_ks::SessionManager::create_session_manager(
                    "ks-runtime-domain",
                    opener.clone(),
                    None,
                    info.clone(),
                    Duration::from_secs(1),
                    kv.clone(),
                    mpp.clone(),
                )
                .map(|runtime| {
                    runtime
                        as Arc<
                            dyn tidb_domain_crossks::SessionManager<
                                dyn tidb_dxf_storage::SessionContext,
                            >,
                        >
                })
            }
        }))
        .unwrap();
    // Go `CreateMockStoreAndDomainForKS` sets the process's serving keyspace.
    struct RestoreConfig(Arc<tidb_config::config_tree::config::Config>);
    impl Drop for RestoreConfig {
        /// Go `TestDomainAlterTableModeInKeyspaceSubmitOnly` deferred worker release.
        fn drop(&mut self) {
            tidb_config::config_tree::config::store_global_config(self.0.clone());
        }
    }
    let _config = RestoreConfig(tidb_config::config_tree::config::get_global_config());
    tidb_config::config_tree::config::update_global(|config| {
        config.keyspace_name = "ks-runtime-domain".into()
    });
    target.factory().load_sys_ks_info_schema().unwrap();
    let dxf = target.factory().init_dist_task_loop().unwrap();
    let service = tidb_dxf_storage::get_dxf_svc_task_mgr().unwrap();
    service.execute_sql_with_new_session(&tidb_gocontext::Context::background(),
        r#"INSERT INTO mysql.tidb_import_jobs (table_schema,table_name,table_id,group_key,created_by,parameters,source_file_size,status,step) VALUES ('db','tbl',1,'','','{}',1,'pending','')"#,&[]).unwrap();
    let mut system = serving.new_session().unwrap();
    assert_eq!(
        displayed(rows(
            &mut system,
            "select count(1) from mysql.tidb_import_jobs"
        )),
        vec![vec!["1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut ordinary,
            "select count(1) from mysql.tidb_import_jobs"
        )),
        vec![vec!["0"]]
    );
    drop(dxf);
    target.factory().get_cross_ks_mgr().unwrap().close();
}

/// Go `TestManager`: virtual registration, schema/MDL coordination and close.
#[test]
#[ignore = "requires O15_ETCD_ENDPOINT (an isolated real etcd fixture)"]
fn test_manager_etcd() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let endpoint = std::env::var("O15_ETCD_ENDPOINT").expect("etcd fixture endpoint");
    let namespace = format!(
        "/o15-{}/",
        tidb_domain_serverinfo::serverinfo_syncer::new_node_id()
    );
    let connect = || {
        let mut client =
            tidb_pd_client::EtcdClient::connect([endpoint.as_str()], Duration::from_secs(3))
                .unwrap();
        client.set_etcd_cli_by_namespace(&namespace);
        Arc::new(client)
    };
    let observer = connect();
    let target =
        crate::domain::mock_store::MockStoreDomain::create_for_keyspace("ks-etcd-runtime").unwrap();
    let opener = target.transaction_opener();
    let _skip_close = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
        |value| {
            *value.downcast_mut::<bool>().unwrap() = false;
        },
    );
    let skip_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _skip_refresh = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/domain/crossks/skipMinJobIDRefresher",
        {
            let called = skip_called.clone();
            move |value| {
                *value.downcast_mut::<bool>().unwrap() = false;
                called.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        },
    );
    let mut info = target
        .factory()
        .server_info
        .as_ref()
        .unwrap()
        .get_local_server_info();
    info.static_info.id = tidb_domain_serverinfo::serverinfo_syncer::new_node_id();
    let id = info.static_info.id.clone();
    let client = connect();
    let runtime = crate::domain::cross_ks::SessionManager::create_session_manager(
        "SYSTEM",
        opener.clone(),
        Some(client.clone()),
        info,
        Duration::from_millis(500),
        target.factory().kv_client.clone(),
        target.factory().mpp_client.clone(),
    )
    .unwrap();
    assert!(skip_called.load(std::sync::atomic::Ordering::SeqCst));
    let server_key = format!("/tidb/server/info/{id}");
    let registration = observer.get_prefix_metadata(server_key.as_bytes()).unwrap();
    assert_eq!(registration.len(), 1);
    assert!(String::from_utf8_lossy(&registration[0].value)
        .contains("\"assumed_keyspace\":\"ks-etcd-runtime\""));
    let server_lease = registration[0].lease;
    let schema_key = format!("/tidb/ddl/all_schema_versions/{id}");
    let schema_registration = observer.get_prefix_metadata(schema_key.as_bytes()).unwrap();
    assert_eq!(schema_registration.len(), 1);
    assert!(observer.get_prefix(b"/topology/tidb/").unwrap().is_empty());
    use tidb_domain_sqlsvrapi::Runtime as _;
    let pool = runtime.sys_session_pool();
    let session = pool.get().unwrap();
    let exec = |sql: &str| {
        session
            .with_session_context(|context| -> tidb_syssession::Result<()> {
                tidb_domain_core::sys_session::exec_sql(context, "ddl", sql, &[])
                    .map_err(tidb_syssession::SysSessionError::new)?;
                Ok(())
            })
            .unwrap()
    };
    exec("begin");
    exec("select count(1) from mysql.tidb_import_jobs");
    assert!(runtime.coordinator().internal_session_count() >= 1);
    let mut ordinary = target.new_session().unwrap();
    rows(&mut ordinary, "create database if not exists test");
    rows(&mut ordinary, "create table test.mdl_epoch(id int)");
    let latest = target.factory().catalog.get_latest();
    let version = latest.schema_meta_version();
    let (_, table) = latest
        .table_info_by_name("mysql", "tidb_import_jobs")
        .unwrap();
    rows(&mut ordinary,&format!("replace into mysql.tidb_mdl_info(job_id,version,table_ids) values(999999,{version},'{}')",table.read().id));
    let ack_key = format!("/tidb/ddl/all_schema_by_job_versions/999999/{id}");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while runtime.info_cache().get_latest().schema_meta_version() < version {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        observer.get(ack_key.as_bytes()).unwrap().is_none(),
        "old transaction must block the reserved-table MDL ack"
    );
    exec("commit");
    pool.put(&session);
    while observer.get(ack_key.as_bytes()).unwrap().is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "MDL ack did not follow commit"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(runtime.coordinator().internal_session_count(), 0);
    // Go TestDomainAlterTableModeInKeyspaceSubmitOnly: an uninitialized
    // watcher must refresh the target's etcd state before enqueuing DDL.
    rows(&mut ordinary, "create table test.t_mode_upgrade(id int)");
    let latest = target.factory().catalog.get_latest();
    let schema = latest.schema_by_name("test").unwrap();
    let (_, table) = latest.table_info_by_name("test", "t_mode_upgrade").unwrap();
    let request = tidb_model::AlterTableModeTarget {
        schema_id: schema.read().id,
        schema_name: tidb_ast::CiString::new("test"),
        table_id: table.read().id,
        table_name: tidb_ast::CiString::new("t_mode_upgrade"),
        target_mode: tidb_model::TableMode::IMPORT,
        ..Default::default()
    };
    struct Unblock(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
    impl Drop for Unblock {
        /// Go `TestDomainAlterTableModeInKeyspaceSubmitOnly` deferred worker release.
        fn drop(&mut self) {
            *self.0 .0.lock().unwrap() = true;
            self.0 .1.notify_all();
        }
    }
    let blocked = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs",
        {
            let blocked = blocked.clone();
            move |_| {
                let mut released = blocked.0.lock().unwrap();
                while !*released {
                    released = blocked.1.wait(released).unwrap();
                }
            }
        },
    );
    let unblock = Unblock(blocked);
    let state_ctx = tidb_timeutil::SleepContext::background();
    runtime
        .ddl_client
        .server_state
        .update_global_state(
            &state_ctx,
            &tidb_ddl_serverstate::StateInfo::new(tidb_ddl_serverstate::STATE_UPGRADING),
        )
        .unwrap();
    assert!(!runtime.ddl_client.server_state.is_upgrading_state());
    assert!(runtime.ddl_client.server_state.watch_chan().is_none());
    let (ctx, cancel) = tidb_gocontext::Context::with_timeout(
        &tidb_gocontext::Context::background(),
        Duration::from_secs(30),
    );
    let submit = std::thread::spawn({
        let runtime = runtime.clone();
        move || runtime.ddl_client.alter_table_mode(&ctx, &request)
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        // Go `GetAllDDLJobs` over the target's DDL session pool.
        let jobs = tidb_ddl::ddl_history::with_pool_session(runtime.ddl_client.ddl_pool.as_ref(), |se| {
            tidb_ddl::ddl_history::get_jobs_by_sql(se, "job_id >= 0 order by job_id")
        })
        .unwrap();
        if let Some(job) = jobs.iter().find(|job| job.table_id == table.read().id) {
            assert!(matches!(
                job.state,
                tidb_model::JobState::PAUSING | tidb_model::JobState::PAUSED
            ));
            assert_eq!(
                job.admin_operator,
                tidb_model::AdminCommandOperator::BY_SYSTEM
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cross-KS upgrade job was not submitted"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(runtime.ddl_client.server_state.is_upgrading_state());
    cancel.cancel();
    assert!(submit
        .join()
        .unwrap()
        .unwrap_err()
        .contains("context canceled"));
    runtime
        .ddl_client
        .server_state
        .update_global_state(
            &state_ctx,
            &tidb_ddl_serverstate::StateInfo::new(tidb_ddl_serverstate::STATE_NORMAL_RUNNING),
        )
        .unwrap();
    drop(unblock);
    drop(hook);
    runtime.close();
    assert!(pool.get().is_err());
    assert!(observer.get(server_key.as_bytes()).unwrap().is_none());
    assert!(observer.get(schema_key.as_bytes()).unwrap().is_none());
    assert!(observer
        .lease_keep_alive_once(server_lease)
        .map_or(true, |ttl| ttl <= 0));
    // Go schemaver.etcdSyncer.Close removes the self-version key; its lease
    // expires after etcd.Close stops keep-alive, rather than being revoked.
    assert_eq!(
        client.get(b"closed"),
        Err(tidb_pd_client::EtcdError::Closed)
    );
    assert!(!opener.store().store().is_closed());

    // Go TestManager's failed-bootstrap cleanup: no cached runtime,
    // registration/schema lease removed, client and target store closed.
    let empty = tidb_ddl::embedded_store::in_process_stack_from_image_for_keyspace(
        &tidb_pd_client::ClusterSecurity::plaintext(),
        None,
        Some(tikv_client::proto::keyspacepb::KeyspaceMeta {
            name: "ks-bootstrap".into(),
            keyspace: Some(tikv_client::proto::keyspacepb::keyspace_meta::Keyspace::Id(
                87,
            )),
            ..Default::default()
        }),
    )
    .unwrap();
    let mut info = target
        .factory()
        .server_info
        .as_ref()
        .unwrap()
        .get_local_server_info();
    info.static_info.id = tidb_domain_serverinfo::serverinfo_syncer::new_node_id();
    let id = info.static_info.id.clone();
    let client = connect();
    let failed = crate::domain::cross_ks::SessionManager::create_session_manager(
        "SYSTEM",
        empty.opener.clone(),
        Some(client.clone()),
        info,
        Duration::from_millis(500),
        None,
        None,
    );
    assert!(failed.is_err());
    assert!(observer
        .get(format!("/tidb/server/info/{id}").as_bytes())
        .unwrap()
        .is_none());
    assert!(observer
        .get(format!("/tidb/ddl/all_schema_versions/{id}").as_bytes())
        .unwrap()
        .is_none());
    assert!(empty.tikv_store.store().is_closed());
    assert_eq!(
        client.get(b"closed"),
        Err(tidb_pd_client::EtcdError::Closed)
    );
    observer.delete_prefix(b"").unwrap();
}

/// Go `zzprobe/o15_named_sql.TestNamedKeyspaceSQL`: named codec through SQL.
#[test]
fn test_named_keyspace_sql() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    for name in ["SYSTEM", "tenant-sql"] {
        let store = crate::domain::mock_store::MockStoreDomain::create_for_keyspace(name).unwrap();
        let mut session = store.new_session().unwrap();
        rows(&mut session, "create database if not exists test");
        rows(
            &mut session,
            "create table test.t_named(id int primary key, v int, key iv(v))",
        );
        rows(
            &mut session,
            "insert into test.t_named values (1,10),(2,20),(3,10)",
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                "select id,v from test.t_named ignore index(iv) order by id"
            )),
            vec![vec!["1", "10"], vec!["2", "20"], vec!["3", "10"]]
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                "select v from test.t_named force index(iv) where v=10 order by v"
            )),
            vec![vec!["10"], vec!["10"]]
        );
        rows(&mut session, "begin");
        rows(&mut session, "update test.t_named set v=30 where id=1");
        rows(&mut session, "commit");
        assert_eq!(
            displayed(rows(
                &mut session,
                "select id,v from test.t_named order by id"
            )),
            vec![vec!["1", "30"], vec!["2", "20"], vec!["3", "10"]]
        );
    }
}
