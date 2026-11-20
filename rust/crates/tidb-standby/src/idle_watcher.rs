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

//! Go `pkg/standby/idle_watcher.go`.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;

use crate::{save_tidb_normal_restart_info, tidb_exit, ExitSignal, LoadKeyspaceController};

/// Go `mysql.ComSleep`.
const COM_SLEEP: u8 = 0;
/// Go `mysql.ServerStatusInTrans`.
const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
/// Go `mysql.ClientInteractive`.
const CLIENT_INTERACTIVE: u32 = 1 << 10;

/// The server as the idle watcher reads it (Go `*server.Server`).
pub trait IdleServer: Send + Sync {
    /// Go `Server.ConnectionCount`.
    fn connection_count(&self) -> usize;
    /// Go `Server.GetUserProcessList`: each process's `(Command, State)`.
    fn get_user_process_list(&self) -> Vec<(u8, u16)>;
    /// Go `Server.GetClientCapabilityList`.
    fn get_client_capability_list(&self) -> Vec<u32>;
    /// Go `Server.SetNeedRequestMgrFree`.
    fn set_need_request_mgr_free(&self);
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs() as i64)
}

impl LoadKeyspaceController {
    /// Go `OnConnActive`: makes sure `lastActive` is not less than the
    /// current time.
    pub fn on_conn_active(&self) {
        self.last_active.fetch_max(now_unix(), Ordering::SeqCst);
    }

    /// Go `OnServerCreated`: watches `lastActive` and exits the process if
    /// it is not updated for a long time.
    pub fn on_server_created(self: &Arc<Self>, svr: Arc<dyn IdleServer>) {
        let max_idle_secs = tidb_config::config_tree::config::get_global_config().standby.max_idle_seconds as i64;
        if max_idle_secs <= 0 {
            return;
        }
        self.on_conn_active();
        let controller = Arc::clone(self);
        let _ = std::thread::Builder::new().name("standby-idle-watcher".to_owned()).spawn(move || loop {
            std::thread::sleep(Duration::from_secs(10));
            controller.check_idle(svr.as_ref(), max_idle_secs);
        });
    }

    /// One tick of Go `OnServerCreated`'s watcher.
    pub(crate) fn check_idle(&self, svr: &dyn IdleServer, max_idle_secs: i64) {
        let last = self.last_active.load(Ordering::SeqCst);
        if now_unix() - last <= max_idle_secs {
            return;
        }
        let conn_count = svr.connection_count();
        let (mut process_count, mut in_trans_count) = (0, 0);
        for (command, state) in svr.get_user_process_list() {
            // ignore sleep sessions (waiting for client query).
            if command != COM_SLEEP {
                process_count += 1;
            }
            if state & SERVER_STATUS_IN_TRANS > 0 {
                in_trans_count += 1;
            }
        }
        let client_interactive_count =
            svr.get_client_capability_list().into_iter().filter(|capability| capability & CLIENT_INTERACTIVE > 0).count();
        bg_logger().info(
            "connection idle for too long",
            &[
                Field::new("max-idle-seconds", Value::I64(max_idle_secs)),
                Field::new("connection-count", Value::I64(conn_count as i64)),
                Field::new("process-count", Value::I64(i64::from(process_count))),
                Field::new("inTrans-count", Value::I64(i64::from(in_trans_count))),
                Field::new("client-interactive-count", Value::I64(client_interactive_count as i64)),
            ],
        );
        let idle = (conn_count == 0 || process_count == 0) && in_trans_count == 0;
        // if zero backend enabled. We shouldn't skip graceful shutdown to
        // wait for session migration. And clientInteractiveCount don't need
        // to be considered because session can be restored by gateway.
        if tidb_config_deploymode::is_starter()
            && tidb_config::config_tree::config::get_global_config().standby.enable_zero_backend
            && idle
        {
            save_tidb_normal_restart_info("connection idle for too long");
            svr.set_need_request_mgr_free();
            tidb_exit(ExitSignal::Term);
        } else if idle && client_interactive_count == 0 {
            save_tidb_normal_restart_info("connection idle for too long");
            tidb_exit(ExitSignal::Int);
        }
    }
}
