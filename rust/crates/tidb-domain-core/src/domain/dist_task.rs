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

//! Go `domain.go`'s DXF half: `InitDistTaskLoop`, `calculateNodeResource`
//! and `distTaskFrameworkLoop`. The node's task executor manager always
//! runs; the scheduler manager runs while the node is the DDL owner. The
//! returned [`DistTaskLoop`] is the DXF half of Go's `Domain.Close`: dropped,
//! it stops the loop and waits for it.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_dxf::NodeResource;
use tidb_dxf_storage::TaskManager;
use tidb_gocontext::{CancelFunc, Context};
use tidb_log::{Field, Value};

use super::Domain;

fn str_field(name: &str, value: impl Into<String>) -> Field {
    Field::new(name, Value::Str(value.into()))
}

/// The DXF framework loop Go's `do.wg` runs, with Go's `do.exit` and the
/// executor manager's cancel (`do.cancelFns`).
pub struct DistTaskLoop {
    exit: Option<crossbeam_channel::Sender<()>>,
    manager_cancel: CancelFunc,
    handle: Option<JoinHandle<()>>,
    metering: Option<MeteringLoop>,
}

impl Drop for DistTaskLoop {
    fn drop(&mut self) {
        self.manager_cancel.cancel();
        // Go `close(do.exit)`.
        drop(self.exit.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Go `Domain.InitDistTaskLoop`'s metering worker, joined by `Domain.Close`.
struct MeteringLoop {
    cancel: CancelFunc,
    handle: Option<JoinHandle<()>>,
}
impl MeteringLoop {
    /// Go `InitDistTaskLoop`: parse the configured provider before starting its loop.
    fn start(uri: &str) -> Result<Option<Self>, String> {
        if uri.is_empty() {
            tidb_logutil::bg_logger().warn("metering storage uri is empty, metering will be disabled", &[]);
            return Ok(None);
        }
        let config = tidb_metering_sdk::config::new_from_uri(uri)
            .map_err(|error| format!("failed to parse metering storage uri: {error}"))?;
        let meter = tidb_dxf_metering::Meter::new_meter(&config)
            .map_err(|error| format!("failed to create metering: {error}"))?.map(Arc::new);
        tidb_dxf_metering::set_metering(meter.clone());
        let Some(meter) = meter else {return Ok(None)};
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let handle = std::thread::Builder::new().name("dxfMeteringFlushLoop".into()).spawn(move || {
            struct Reset;
            impl Drop for Reset {fn drop(&mut self) {tidb_dxf_metering::set_metering(None);}}
            let _reset = Reset;
            meter.start_flush_loop(&ctx);
        }).map_err(|error| {tidb_dxf_metering::set_metering(None); error.to_string()})?;
        Ok(Some(Self {cancel, handle: Some(handle)}))
    }
}
impl Drop for MeteringLoop {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {let _ = handle.join();}
    }
}

/// Go `calculateNodeResource`: the node's resource DXF may use.
///
/// # Errors
/// The memory total is unknown, or the CPU or memory is not positive.
pub fn calculate_node_resource() -> Result<NodeResource, String> {
    let logger = tidb_logutil::err_verbose_logger();
    // should not happen normally, as the server's main asserts that the
    // memory total is known.
    let total_mem = tidb_memory::mem_total().map_err(|err| err.to_string())?;
    let total_cpu = tidb_cpu::get_cpu_count() as isize;
    if total_cpu <= 0 || total_mem == 0 {
        return Err(format!("invalid cpu or memory, cpu: {total_cpu}, memory: {total_mem}"));
    }
    let cfg = tidb_config::config_tree::config::get_global_config();
    let total_disk = match tidb_lightning_common::get_storage_size(&cfg.temp_dir) {
        Ok(size) => size.capacity,
        Err(err) => {
            logger.warn(
                "get storage size failed, use tidb_ddl_disk_quota instead",
                &[str_field("error", err.to_string())],
            );
            tidb_vardef::DDL_DISK_QUOTA.load(std::sync::atomic::Ordering::SeqCst)
        }
    };
    let node_res = NodeResource::new(total_cpu, total_mem as i64, total_disk);
    let dxf_node_res = node_res.limit_dxf_resource(cfg.dxf_resource_limit as isize);
    logger.info(
        "initialize node resource",
        &[
            Field::new("total-cpu", Value::I64(total_cpu as i64)),
            str_field("total-mem", tidb_config_configtypes::bytes_size(total_mem as f64)),
            Field::new("dxf-resource-limit", Value::I64(cfg.dxf_resource_limit)),
            Field::new("dxf-usable-cpu", Value::I64(dxf_node_res.total_cpu as i64)),
            str_field("dxf-usable-mem", tidb_config_configtypes::bytes_size(dxf_node_res.total_mem as f64)),
            str_field("total-disk", tidb_config_configtypes::bytes_size(total_disk as f64)),
        ],
    );
    Ok(dxf_node_res)
}

impl Domain {
    /// Go `InitDistTaskLoop`: sets the process's task manager and starts the
    /// DXF framework loop.
    ///
    /// The SYSTEM keyspace installs its configured Meter. User-keyspace
    /// sessions use the crossks SYSTEM pool. Go's last step,
    /// `ingestctrl.InitializeRateLimiterParam`, comes with the ingest port
    /// (D02).
    ///
    /// # Errors
    /// A missing cross-keyspace pool; the node is not in the cluster's server list; the
    /// node's resource is unknown.
    pub fn init_dist_task_loop(self: &Arc<Self>) -> Result<DistTaskLoop, String> {
        let task_manager = Arc::new(TaskManager::new(self.dxf_session_pool()));
        tidb_dxf_storage::set_task_manager(Some(Arc::clone(&task_manager)));

        if tidb_config_kerneltype::is_next_gen() && self.store_keyspace() != "SYSTEM" {
            let pool=self.get_ks_sess_pool(tidb_keyspace::SYSTEM)?;
            tidb_dxf_storage::set_dxf_svc_task_mgr(Arc::new(TaskManager::new(pool)));
            tidb_logutil::bg_logger().info("skip running DXF in user keyspace", &[]);
            let (_,manager_cancel)=Context::with_cancel(&Context::background());
            return Ok(DistTaskLoop {exit:None,manager_cancel,handle:None,metering:None});
        }
        let metering = if tidb_config_kerneltype::is_next_gen() {
            MeteringLoop::start(&tidb_config::config_tree::config::get_global_config().metering_storage_uri)?
        } else {None};

        // Go `ddl.GetID()`: the DDL uuid the server info publishes.
        let ddl_id = self
            .server_info()
            .map(|syncer| syncer.get_local_server_info().static_info.id.clone())
            .or_else(|| self.ddl.owner_manager().map(|owner| owner.id().to_owned()))
            .unwrap_or_default();
        let server_id = self
            .server_info()
            .map(|syncer| tidb_util_disttask::generate_subtask_exec_id(&syncer, &ddl_id))
            .unwrap_or_default();
        if server_id.is_empty() {
            return Err(format!("TiDB node ID( = {ddl_id} ) not found in available TiDB nodes list"));
        }
        let (manager_ctx, manager_cancel) = Context::with_cancel(&Context::background());
        let node_res = calculate_node_resource()?;
        tidb_dxf_storage::set_node_resource(node_res);
        let executor_manager = tidb_dxf_taskexecutor::Manager::new(
            &manager_ctx,
            &server_id,
            Arc::clone(&task_manager) as Arc<dyn tidb_dxf_taskexecutor::TaskTable>,
            node_res,
        );

        // Go runs `InitMeta` on the bootstrap goroutine, whose stack grows: its
        // SQL outgrows a main thread's stack, so it runs on a SQL worker.
        let init_meta = {
            let executor_manager = Arc::clone(&executor_manager);
            tidb_worker_stack::sql_worker_thread("dxf-init-meta")
                .spawn(move || executor_manager.init_meta())
                .map_err(|err| err.to_string())?
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        };
        if let Err(err) = init_meta {
            // executor manager loop will try to recover meta repeatedly, so we can
            // just log the error here.
            tidb_logutil::bg_logger().warn("init task executor manager meta failed", &[str_field("error", err.to_string())]);
        }
        let (exit_tx, exit_rx) = crossbeam_channel::bounded::<()>(0);
        let domain = Arc::clone(self);
        let handle = tidb_worker_stack::sql_worker_thread("distTaskFrameworkLoop")
            .spawn(move || {
                domain.dist_task_framework_loop(&exit_rx, task_manager, &executor_manager, &server_id, node_res);
                tidb_dxf_storage::set_task_manager(None);
            })
            .map_err(|err| err.to_string())?;
        if let Err(error) = self.transactions.initialize_ingest_parameters() {
            tidb_logutil::bg_logger().error("initialize global max batch split ranges failed", &[str_field("error", error)]);
        }
        Ok(DistTaskLoop { exit: Some(exit_tx), manager_cancel, handle: Some(handle), metering })
    }

    /// Go `distTaskFrameworkLoop`.
    fn dist_task_framework_loop(
        &self,
        exit: &crossbeam_channel::Receiver<()>,
        task_manager: Arc<TaskManager>,
        executor_manager: &Arc<tidb_dxf_taskexecutor::Manager>,
        server_id: &str,
        node_res: NodeResource,
    ) {
        let logger = tidb_logutil::bg_logger();
        if let Err(err) = executor_manager.start() {
            logger.error("dist task executor manager start failed", &[str_field("error", err.to_string())]);
            return;
        }
        logger.info("dist task executor manager started", &[]);

        let ddl_id = || {
            self.server_info().map(|syncer| syncer.get_local_server_info().static_info.id.clone()).unwrap_or_default()
        };
        let mut scheduler_manager: Option<Arc<tidb_dxf_scheduler::Manager>> = None;
        let start_scheduler_mgr_if_needed = |scheduler_manager: &mut Option<Arc<tidb_dxf_scheduler::Manager>>| {
            if scheduler_manager.as_ref().is_some_and(|manager| manager.initialized()) {
                return;
            }
            let Some(server_info) = self.server_info() else {
                return;
            };
            let manager = tidb_dxf_scheduler::Manager::new(
                &Context::background(),
                Arc::clone(&task_manager) as Arc<dyn tidb_dxf_scheduler::TaskManager>,
                server_id,
                node_res,
                server_info as Arc<dyn tidb_dxf_scheduler::ServerInfoSource>,
            );
            manager.start();
            *scheduler_manager = Some(manager);
        };
        let stop_scheduler_mgr_if_needed = |scheduler_manager: &Option<Arc<tidb_dxf_scheduler::Manager>>| {
            if let Some(manager) = scheduler_manager.as_ref().filter(|manager| manager.initialized()) {
                logger.info(
                    "stopping dist task scheduler manager because the current node is not DDL owner anymore",
                    &[str_field("id", ddl_id())],
                );
                manager.stop();
                logger.info("dist task scheduler manager stopped", &[str_field("id", ddl_id())]);
            }
        };

        let ticker = crossbeam_channel::tick(Duration::from_secs(1));
        loop {
            crossbeam_channel::select! {
                recv(exit) -> _ => {
                    stop_scheduler_mgr_if_needed(&scheduler_manager);
                    break;
                }
                recv(ticker) -> _ => {
                    if self.ddl.is_owner() {
                        start_scheduler_mgr_if_needed(&mut scheduler_manager);
                    } else {
                        stop_scheduler_mgr_if_needed(&scheduler_manager);
                    }
                }
            }
        }

        logger.info("stopping dist task executor manager", &[]);
        executor_manager.stop();
        logger.info("dist task executor manager stopped", &[]);
    }
}

#[cfg(test)]
mod metering_provider_tests {
    use super::*;
    use std::io::Read;

    /// Go `zzprobe/o15_meter_live.TestMeterProductionS3Readback`, through
    /// Domain.InitDistTaskLoop's production provider installation and teardown.
    #[test]
    #[ignore = "requires O15_LIVE_CONFIG for an isolated MinIO fixture"]
    fn test_meter_production_s3_readback() {
        if tidb_config_kerneltype::is_classic() {return;}
        let fixture=std::fs::read(std::env::var("O15_LIVE_CONFIG").expect("MinIO fixture configuration")).unwrap();
        let fixture:serde_json::Value=serde_json::from_slice(&fixture).unwrap();
        let endpoint=format!("http://127.0.0.1:{}",fixture["minio"].as_u64().unwrap());
        let uri=format!("s3://o15-meter/rust-domain?region=us-east-1&endpoint={endpoint}&access-key={}&secret-access-key={}&force-path-style=true&shared-pool-id=pool",
            fixture["access_key"].as_str().unwrap(),fixture["secret_key"].as_str().unwrap());
        let config=tidb_metering_sdk::config::new_from_uri(&uri).unwrap();
        let provider=tidb_metering_sdk::storage::new_object_storage_provider(&config).unwrap();
        let ctx=Context::background();
        let path="metering/ru/120/dxf/pool/server_1-0.json.gz";
        let after_close="metering/ru/180/dxf/pool/server_2-0.json.gz";
        provider.delete(&ctx,path).unwrap();provider.delete(&ctx,after_close).unwrap();
        let worker=MeteringLoop::start(&uri).unwrap().expect("configured meter is installed");
        let items=[std::collections::BTreeMap::from([("row_count".into(),serde_json::json!(12)),("task_id".into(),serde_json::json!(7))])];
        tidb_dxf_metering::write_meter_data(&ctx,120,"server_1",&items).unwrap();
        let read=|provider:&dyn tidb_metering_sdk::storage::ObjectStorageProvider| {
            let bytes=provider.download(&ctx,path).unwrap();
            let mut json=String::new();flate2::read::GzDecoder::new(bytes.as_slice()).read_to_string(&mut json).unwrap();
            serde_json::from_str::<serde_json::Value>(&json).unwrap()
        };
        let actual=read(provider.as_ref());
        let mut go_config=config.clone();go_config.prefix="go-capture".into();
        let go_provider=tidb_metering_sdk::storage::new_object_storage_provider(&go_config).unwrap();
        assert_eq!(actual,read(go_provider.as_ref()));
        assert_eq!(actual["data"],serde_json::json!([{"row_count":12,"task_id":7}]));
        drop(worker);
        tidb_dxf_metering::write_meter_data(&ctx,180,"server_2",&items).unwrap();
        assert!(!provider.exists(&ctx,after_close).unwrap(),"Domain.Close uninstalls the meter");
    }
}
