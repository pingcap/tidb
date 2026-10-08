use autoid::auto_id_alloc_server::{AutoIdAlloc, AutoIdAllocServer};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use tidb_exec::auto_id_client::{AutoIdClient, AutoIdLeader};
use tidb_exec::cluster_auto_id::AutoIdServiceAllocator;
use tidb_exec::cluster_auto_id::AutoIdServiceRpcError as Error;
use tidb_executor::kv_table::AutoIdCall as UnaryCallContext;
use tidb_pd_client::ClusterSecurity;
use tikv_client::proto::autoid;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    values: Mutex<HashMap<(i64, i64), i64>>,
    requests: Mutex<Vec<autoid::AutoIdRequest>>,
    rebase_requests: Mutex<Vec<autoid::RebaseRequest>>,
    fail: AtomicUsize,
}
#[derive(Clone)]
struct Service(Arc<State>);
#[tonic::async_trait]
impl AutoIdAlloc for Service {
    async fn alloc_auto_id(
        &self,
        request: Request<autoid::AutoIdRequest>,
    ) -> Result<Response<autoid::AutoIdResponse>, Status> {
        let request = request.into_inner();
        self.0.requests.lock().unwrap().push(request);
        if self.0.fail.swap(0, Ordering::SeqCst) > 0 {
            return Err(Status::unavailable("leader changed"));
        }
        if request.tbl_id == 99 {
            return Ok(Response::new(autoid::AutoIdResponse {
                errmsg: b"service rejected allocation".to_vec(),
                ..Default::default()
            }));
        }
        let mut values = self.0.values.lock().unwrap();
        let value = values.entry((request.db_id, request.tbl_id)).or_default();
        let min = *value;
        if request.n > 0 {
            let first = tidb_executor::kv_table::calc_needed_batch_size(
                *value as u64,
                request.n,
                request.increment as u64,
                request.offset as u64,
                request.is_unsigned,
            );
            *value = value.wrapping_add(first as i64);
        }
        Ok(Response::new(autoid::AutoIdResponse {
            min,
            max: *value,
            errmsg: vec![],
        }))
    }
    async fn rebase(
        &self,
        request: Request<autoid::RebaseRequest>,
    ) -> Result<Response<autoid::RebaseResponse>, Status> {
        let request = request.into_inner();
        self.0.rebase_requests.lock().unwrap().push(request);
        let mut values = self.0.values.lock().unwrap();
        let value = values.entry((request.db_id, request.tbl_id)).or_default();
        if request.force
            || tidb_executor::kv_table::exceeds(
                request.base as u64,
                *value as u64,
                request.is_unsigned,
            )
        {
            *value = request.base;
        }
        Ok(Response::new(autoid::RebaseResponse::default()))
    }
}
struct Server {
    address: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Server {
    fn start(state: Arc<State>) -> Self {
        let (ready, receive) = mpsc::channel();
        let (stop, done) = tokio::sync::oneshot::channel();
        let worker = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    ready
                        .send(listener.local_addr().unwrap().to_string())
                        .unwrap();
                    tonic::transport::Server::builder()
                        .add_service(AutoIdAllocServer::new(Service(state)))
                        .serve_with_incoming_shutdown(
                            tokio_stream::wrappers::TcpListenerStream::new(listener),
                            async {
                                let _ = done.await;
                            },
                        )
                        .await
                        .unwrap();
                });
        });
        Self {
            address: receive.recv().unwrap(),
            stop: Some(stop),
            worker: Some(worker),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.worker.take().unwrap().join().unwrap();
    }
}
struct Leader {
    addresses: Mutex<Vec<String>>,
    lookups: AtomicUsize,
}
impl AutoIdLeader for Leader {
    fn leader(&self) -> Result<Option<String>, Error> {
        let index = self.lookups.fetch_add(1, Ordering::SeqCst);
        let addresses = self.addresses.lock().unwrap();
        Ok(addresses
            .get(index.min(addresses.len().saturating_sub(1)))
            .cloned())
    }
}
fn client(addresses: Vec<String>) -> (Arc<AutoIdClient>, Arc<Leader>) {
    let leader = Arc::new(Leader {
        addresses: Mutex::new(addresses),
        lookups: AtomicUsize::new(0),
    });
    (
        Arc::new(AutoIdClient::new(leader.clone(), ClusterSecurity::plaintext()).unwrap()),
        leader,
    )
}
fn call() -> UnaryCallContext {
    UnaryCallContext::with_timeout(Duration::from_secs(2))
}

#[test]
fn auto_id_owner_two_nodes_share_service_and_all_table_operations() {
    let state = Arc::new(State::default());
    let server = Server::start(state.clone());
    let (first, lookups) = client(vec![server.address.clone()]);
    let (second, _) = client(vec![server.address.clone()]);
    let a = AutoIdServiceAllocator::new(first.clone(), 1, 2, false, u32::MAX);
    let b = AutoIdServiceAllocator::new(second, 1, 2, false, u32::MAX);
    assert_eq!(a.alloc(&call(), 1, 1, 1).unwrap(), (0, 1));
    assert_eq!(b.alloc(&call(), 1, 1, 1).unwrap(), (1, 2));
    a.rebase(&call(), 100, false).unwrap();
    assert_eq!(b.alloc(&call(), 2, 3, 2).unwrap(), (100, 104));
    assert_eq!(a.alloc(&call(), 0, 1, 1).unwrap(), (104, 104));
    a.rebase(&call(), 10, true).unwrap();
    assert_eq!(b.alloc(&call(), 1, 1, 1).unwrap(), (10, 11));
    let other = AutoIdServiceAllocator::new(first, 1, 3, true, u32::MAX);
    other.rebase(&call(), i64::MIN, false).unwrap();
    assert_eq!(other.alloc(&call(), 1, 1, 1).unwrap().1, i64::MIN + 1);
    assert_eq!(
        lookups.lookups.load(Ordering::SeqCst),
        1,
        "tables share discovery"
    );
    assert!(state
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.keyspace == Some(autoid::auto_id_request::Keyspace::KeyspaceId(u32::MAX))));
    assert!(state
        .rebase_requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.keyspace.is_none()));
}

#[test]
fn auto_id_owner_recovery_replaces_shared_generation_without_replaying_success() {
    let state = Arc::new(State::default());
    state.fail.store(1, Ordering::SeqCst);
    let old = Server::start(state.clone());
    let new = Server::start(state.clone());
    let (client, leader) = client(vec![old.address.clone(), new.address.clone()]);
    let a = AutoIdServiceAllocator::new(client.clone(), 1, 2, false, u32::MAX);
    let b = AutoIdServiceAllocator::new(client, 1, 3, false, u32::MAX);
    assert_eq!(a.alloc(&call(), 1, 1, 1).unwrap(), (0, 1));
    assert_eq!(b.alloc(&call(), 1, 1, 1).unwrap(), (0, 1));
    assert_eq!(leader.lookups.load(Ordering::SeqCst), 2);
    assert_eq!(state.requests.lock().unwrap().len(), 3);
}

#[test]
fn auto_id_owner_application_error_keeps_discovery_and_is_not_retried() {
    let state = Arc::new(State::default());
    let server = Server::start(state.clone());
    let (client, leader) = client(vec![server.address.clone()]);
    let refused = AutoIdServiceAllocator::new(client.clone(), 1, 99, false, u32::MAX);
    assert!(refused
        .alloc(&call(), 1, 1, 1)
        .unwrap_err()
        .to_string()
        .contains("service rejected allocation"));
    let accepted = AutoIdServiceAllocator::new(client, 1, 2, false, u32::MAX);
    assert_eq!(accepted.alloc(&call(), 1, 1, 1).unwrap(), (0, 1));
    assert_eq!(state.requests.lock().unwrap().len(), 2);
    assert_eq!(leader.lookups.load(Ordering::SeqCst), 1);
}

#[test]
fn auto_id_owner_discovery_deadline_and_cancellation_join() {
    let (client, _) = client(vec![]);
    let alloc = AutoIdServiceAllocator::new(client.clone(), 1, 2, false, u32::MAX);
    let short = UnaryCallContext::with_timeout(Duration::from_millis(25));
    assert!(alloc
        .alloc(&short, 1, 1, 1)
        .unwrap_err()
        .to_string()
        .contains("deadline"));
    let canceled = call();
    canceled.cancellation().cancel();
    assert!(alloc
        .alloc(&canceled, 1, 1, 1)
        .unwrap_err()
        .to_string()
        .contains("canceled"));
    drop(alloc);
    drop(client);
}
#[test]
fn auto_id_owner_sql_kill_interrupts_service_discovery() {
    let (client, leader) = client(vec![]);
    let alloc = Arc::new(AutoIdServiceAllocator::new(client, 1, 2, false, u32::MAX));
    let memory = tidb_executor::StatementMemory::default();
    let call = UnaryCallContext::statement(&memory);
    let worker = std::thread::spawn(move || alloc.alloc(&call, 1, 1, 1));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while leader.lookups.load(Ordering::SeqCst) == 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    memory
        .sql_killer()
        .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
    assert!(worker.join().unwrap().is_err());
}
#[test]
fn auto_id_owner_ddl_rebase_uses_service_instead_of_stale_metadata() {
    use tidb_exec::cluster_ddl::{plan_ddl_with_auto_ids, DdlPlan, DdlStatement};
    use tidb_meta::{key, value};
    let state = Arc::new(State::default());
    let server = Server::start(state.clone());
    let (client, _) = client(vec![server.address.clone()]);
    let mut store = crate::cluster_ddl_source::bootstrapped();
    let mut field = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    field.add_flags(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT);
    let table = tidb_model::TableInfo {
        id: 200,
        name: tidb_ast::CiString::new("t"),
        version: 5,
        auto_id_cache: 1,
        columns: vec![tidb_model::ColumnInfo::new(1, "id", field)].into(),
        state: tidb_model::SchemaState::PUBLIC,
        ..Default::default()
    };
    store.pairs.insert(
        key::table_kv_key(112, 200),
        value::serialize_table_info(&table).unwrap(),
    );
    store.pairs.insert(
        key::auto_increment_id_kv_key(112, 200),
        value::encode_int_value(4000),
    );
    // The service's consumed base differs from its persisted reserved end.
    state.values.lock().unwrap().insert((112, 200), 104);
    for (requested, force, expected) in [(5, false, 104), (8, true, 7)] {
        let plan = plan_ddl_with_auto_ids(
            &mut store,
            &DdlStatement::RebaseAutoIncrementId {
                schema: "u6".into(),
                table: "t".into(),
                new_base: requested,
                force,
            },
            100,
            true,
            Some(&client),
        )
        .unwrap();
        assert_eq!(state.values.lock().unwrap()[&(112, 200)], expected);
        let DdlPlan::Write(write) = plan else {
            panic!("DDL write")
        };
        assert!(
            !write
                .mutations
                .iter()
                .any(|m| m.key() == key::auto_increment_id_kv_key(112, 200)),
            "service owns IID writes"
        );
        assert_eq!(
            state.rebase_requests.lock().unwrap().last().unwrap().base,
            expected
        );
    }
    assert_eq!(
        state.requests.lock().unwrap().len(),
        1,
        "FORCE bypasses NextGlobalAutoID"
    );
    assert!(matches!(
        plan_ddl_with_auto_ids(
            &mut store,
            &DdlStatement::RebaseAutoIncrementId {
                schema: "u6".into(),
                table: "t".into(),
                new_base: 0,
                force: true
            },
            100,
            true,
            Some(&client)
        ),
        Err(tidb_exec::cluster_ddl::DdlPlanError::AutoIdReadFailed)
    ));
}
