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

//! Go `pkg/ttl/client/command.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;
use tidb_pd_client::EtcdClient;

use crate::context::Context;

/// Go `ttlCmdKeyLeaseSeconds`.
pub const TTL_CMD_KEY_LEASE_SECONDS: i64 = 180;
/// Go `ttlCmdKeyRequestPrefix`.
pub const TTL_CMD_KEY_REQUEST_PREFIX: &str = "/tidb/ttl/cmd/req/";
/// Go `ttlCmdKeyResponsePrefix`.
pub const TTL_CMD_KEY_RESPONSE_PREFIX: &str = "/tidb/ttl/cmd/resp/";
/// Go `ttlCmdTypeTriggerTTLJob`.
pub const TTL_CMD_TYPE_TRIGGER_TTL_JOB: &str = "trigger_ttl_job";

/// Go `CmdRequest`: the request for a TTL command. `data` is Go's
/// `json.RawMessage`, kept as its JSON text.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CmdRequest {
    /// Go `RequestID`, JSON `request_id`.
    pub request_id: String,
    /// Go `CmdType`, JSON `cmd_type`.
    pub cmd_type: String,
    /// Go `Data`, JSON `data`.
    pub data: String,
}

/// Go's JSON text of a `json.RawMessage`: a nil message marshals as `null`.
fn raw_message(data: &str) -> &str {
    if data.is_empty() {
        "null"
    } else {
        data
    }
}

fn json_string(text: &str) -> String {
    serde_json::to_string(text).expect("a string always marshals")
}

impl CmdRequest {
    /// Go `json.Marshal(&CmdRequest{...})`.
    pub fn marshal(&self) -> String {
        format!(
            "{{\"request_id\":{},\"cmd_type\":{},\"data\":{}}}",
            json_string(&self.request_id),
            json_string(&self.cmd_type),
            raw_message(&self.data)
        )
    }

    /// Go `json.Unmarshal(payload, &request)`.
    pub fn unmarshal(payload: &[u8]) -> Result<Self, String> {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(default)]
            request_id: String,
            #[serde(default)]
            cmd_type: String,
            #[serde(default)]
            data: Option<serde_json::Value>,
        }
        let wire: Wire = serde_json::from_slice(payload).map_err(|error| error.to_string())?;
        Ok(Self {
            request_id: wire.request_id,
            cmd_type: wire.cmd_type,
            data: wire.data.map(|data| data.to_string()).unwrap_or_default(),
        })
    }

    /// Go `GetTriggerTTLJobRequest`: the `TriggerNewTTLJobRequest` of a
    /// `trigger_ttl_job` command, `None` for any other type or bad data.
    pub fn get_trigger_ttl_job_request(&self) -> Option<TriggerNewTTLJobRequest> {
        if self.cmd_type != TTL_CMD_TYPE_TRIGGER_TTL_JOB {
            return None;
        }
        serde_json::from_str(raw_message(&self.data)).ok()
    }
}

/// Go `cmdResponse`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct CmdResponse {
    request_id: String,
    error_message: String,
    data: String,
}

impl CmdResponse {
    fn marshal(&self) -> String {
        format!(
            "{{\"request_id\":{},\"error_message\":{},\"data\":{}}}",
            json_string(&self.request_id),
            json_string(&self.error_message),
            raw_message(&self.data)
        )
    }

    fn unmarshal(payload: &[u8]) -> Result<Self, String> {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(default)]
            request_id: String,
            #[serde(default)]
            error_message: String,
            #[serde(default)]
            data: Option<serde_json::Value>,
        }
        let wire: Wire = serde_json::from_slice(payload).map_err(|error| error.to_string())?;
        Ok(Self {
            request_id: wire.request_id,
            error_message: wire.error_message,
            data: wire.data.map(|data| data.to_string()).unwrap_or_default(),
        })
    }

    /// The response for Go's `obj any`: an error becomes `ErrorMessage`,
    /// anything else its JSON.
    fn of(req_id: &str, obj: &CommandResult) -> Self {
        match obj {
            Ok(data) => Self {
                request_id: req_id.to_owned(),
                error_message: String::new(),
                data: data.clone(),
            },
            Err(error) => Self {
                request_id: req_id.to_owned(),
                error_message: error.clone(),
                data: String::new(),
            },
        }
    }

    /// The tail of Go's `waitCmdResponse` / mock `Command`: the error message
    /// as an error, else the data for the caller to unmarshal.
    fn into_result(self) -> CommandResult {
        if !self.error_message.is_empty() {
            return Err(self.error_message);
        }
        Ok(self.data)
    }
}

/// Go `TriggerNewTTLJobRequest`: the command detail to trigger a TTL job.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TriggerNewTTLJobRequest {
    /// Go `DBName`.
    #[serde(default)]
    pub db_name: String,
    /// Go `TableName`.
    #[serde(default)]
    pub table_name: String,
}

/// Go `TriggerNewTTLJobTableResult`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TriggerNewTTLJobTableResult {
    /// Go `TableID`.
    #[serde(default)]
    pub table_id: i64,
    /// Go `DBName`.
    #[serde(default)]
    pub db_name: String,
    /// Go `TableName`.
    #[serde(default)]
    pub table_name: String,
    /// Go `PartitionName`, `omitempty`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub partition_name: String,
    /// Go `JobID`, `omitempty`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub job_id: String,
    /// Go `ErrorMessage`, `omitempty`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_message: String,
}

/// Go `TriggerNewTTLJobResponse`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TriggerNewTTLJobResponse {
    /// Go `TableResult`; Go's nil slice marshals as `null`.
    #[serde(default)]
    pub table_result: Option<Vec<TriggerNewTTLJobTableResult>>,
}

/// A command's outcome as it crosses the protocol: the response object's
/// JSON text, or the error message Go's `ResponseCommand(ctx, id, err)`
/// carries.
pub type CommandResult = Result<String, String>;

/// Go `CommandClient`: sends commands and responds to them.
pub trait CommandClient: Send + Sync {
    /// Go `Command`: sends a command and waits for its response. The first
    /// value is the request id, always set; the second is the response
    /// object's JSON for the caller to unmarshal, or the error.
    fn command(&self, ctx: &Context, cmd_type: &str, request: &str) -> (String, CommandResult);
    /// Go `WatchCommand`: the commands sent from now on (and, for the mock,
    /// the pending ones). The channel closes when `ctx` ends.
    fn watch_command(&self, ctx: &Context) -> Receiver<CmdRequest>;
    /// Go `TakeCommand`: whether this caller won the command and must
    /// respond to it.
    fn take_command(&self, ctx: &Context, req_id: &str) -> Result<bool, String>;
    /// Go `ResponseCommand`; `obj` is Go's `obj any`, an error or a value.
    fn response_command(&self, ctx: &Context, req_id: &str, obj: CommandResult) -> Result<(), String>;
}

/// Go `TriggerNewTTLJob`.
pub fn trigger_new_ttl_job(
    ctx: &Context,
    cli: &dyn CommandClient,
    db_name: &str,
    table_name: &str,
) -> Result<TriggerNewTTLJobResponse, String> {
    let request = serde_json::to_string(&TriggerNewTTLJobRequest {
        db_name: db_name.to_owned(),
        table_name: table_name.to_owned(),
    })
    .map_err(|error| error.to_string())?;
    let (_, response) = cli.command(ctx, TTL_CMD_TYPE_TRIGGER_TTL_JOB, &request);
    let data = response?;
    serde_json::from_str(raw_message(&data)).map_err(|error| error.to_string())
}

/// Go `uuid.New().String()`.
fn new_request_id() -> String {
    let hex = tidb_timer::uuid::new_uuid_hex();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

/// Go `etcdClient`: the command and notification clients over etcd.
pub struct EtcdCommandClient {
    pub(crate) etcd_cli: Arc<EtcdClient>,
}

/// Go `NewCommandClient`.
pub fn new_command_client(etcd_cli: Arc<EtcdClient>) -> Arc<dyn CommandClient> {
    Arc::new(EtcdCommandClient { etcd_cli })
}

impl EtcdCommandClient {
    /// Go `(*etcdClient).sendCmd`.
    fn send_cmd(&self, cmd_type: &str, request: &str) -> (String, Result<(), String>) {
        let req_id = new_request_id();
        let request_json = CmdRequest {
            request_id: req_id.clone(),
            cmd_type: cmd_type.to_owned(),
            data: request.to_owned(),
        }
        .marshal();
        let result = self
            .etcd_cli
            .lease_grant(TTL_CMD_KEY_LEASE_SECONDS)
            .map_err(|error| error.to_string())
            .and_then(|(lease, _)| {
                self.etcd_cli
                    .put_with_lease(
                        format!("{TTL_CMD_KEY_REQUEST_PREFIX}{req_id}").as_bytes(),
                        request_json.as_bytes(),
                        lease,
                    )
                    .map_err(|error| error.to_string())
            });
        (req_id, result)
    }

    /// Go `(*etcdClient).waitCmdResponse`: watches the response key and
    /// polls it every second, for at most the command lease.
    fn wait_cmd_response(&self, ctx: &Context, req_id: &str) -> CommandResult {
        let (ctx, _cancel) =
            Context::with_timeout(ctx, Duration::from_secs(TTL_CMD_KEY_LEASE_SECONDS as u64));
        let key = format!("{TTL_CMD_KEY_RESPONSE_PREFIX}{req_id}");
        let (watch_tx, watch_rx) = crossbeam_channel::unbounded::<Vec<u8>>();
        // A failed watch leaves only the polling, as Go's closed watcher does.
        let _watcher = self
            .etcd_cli
            .watch_key(key.clone(), 0, move |event| {
                if !event.deleted {
                    let _ = watch_tx.send(event.value.clone());
                }
            })
            .ok();
        let ticker = crossbeam_channel::tick(Duration::from_secs(1));
        let resp_data = loop {
            crossbeam_channel::select! {
                recv(ctx.done()) -> _ => {
                    return Err(ctx.err().map(|error| error.to_string()).unwrap_or_default());
                }
                recv(ticker) -> _ => {
                    match self.etcd_cli.get(key.as_bytes()) {
                        Ok(Some(value)) => break value,
                        Ok(None) => {}
                        Err(error) => return Err(error.to_string()),
                    }
                }
                recv(watch_rx) -> value => {
                    if let Ok(value) = value {
                        break value;
                    }
                }
            }
        };
        CmdResponse::unmarshal(&resp_data)?.into_result()
    }
}

impl CommandClient for EtcdCommandClient {
    fn command(&self, ctx: &Context, cmd_type: &str, request: &str) -> (String, CommandResult) {
        let (request_id, sent) = self.send_cmd(cmd_type, request);
        if let Err(error) = sent {
            return (request_id, Err(error));
        }
        let response = self.wait_cmd_response(ctx, &request_id);
        (request_id, response)
    }

    fn watch_command(&self, ctx: &Context) -> Receiver<CmdRequest> {
        let (ch_tx, ch_rx) = crossbeam_channel::bounded::<CmdRequest>(0);
        let (ctx, cancel) = Context::with_cancel(ctx);
        let done = ctx.done();
        let watcher = {
            let ctx = ctx.clone();
            self.etcd_cli.watch_prefix_responses(
                TTL_CMD_KEY_REQUEST_PREFIX,
                0,
                {
                    let ctx = ctx.clone();
                    move || ctx.err().is_some()
                },
                move |response| {
                    for event in &response.events {
                        if event.deleted {
                            continue;
                        }
                        let request = match CmdRequest::unmarshal(&event.value) {
                            Ok(request) => request,
                            Err(error) => {
                                bg_logger().error(
                                    "failed to parse ttl cmd payload",
                                    &[
                                        Field::new("error", Value::Str(error)),
                                        Field::new("key", Value::ByteString(event.key.clone())),
                                        Field::new("value", Value::ByteString(event.value.clone())),
                                    ],
                                );
                                // Go sends the zero request it failed to fill.
                                CmdRequest::default()
                            }
                        };
                        crossbeam_channel::select! {
                            send(ch_tx, request) -> _ => {}
                            recv(ctx.done()) -> _ => return,
                        }
                    }
                },
            )
        };
        match watcher {
            Ok(watcher) => {
                // Go's goroutine: the channel closes when the context ends,
                // which is when the watcher (and the sender it owns) drops.
                std::thread::Builder::new()
                    .name("ttl-cmd-watch".to_owned())
                    .spawn(move || {
                        let _ = done.recv();
                        drop(watcher);
                        drop(cancel);
                    })
                    .expect("ttl command watch thread spawns");
            }
            // Go's `etcdCh` closes at once, and so does the channel.
            Err(_) => cancel.cancel(),
        }
        ch_rx
    }

    fn take_command(&self, _ctx: &Context, req_id: &str) -> Result<bool, String> {
        self.etcd_cli
            .delete_count(format!("{TTL_CMD_KEY_REQUEST_PREFIX}{req_id}").as_bytes())
            .map(|deleted| deleted > 0)
            .map_err(|error| error.to_string())
    }

    fn response_command(&self, _ctx: &Context, req_id: &str, obj: CommandResult) -> Result<(), String> {
        let resp_json = CmdResponse::of(req_id, &obj).marshal();
        let (lease, _) = self
            .etcd_cli
            .lease_grant(TTL_CMD_KEY_LEASE_SECONDS)
            .map_err(|error| error.to_string())?;
        self.etcd_cli
            .put_with_lease(
                format!("{TTL_CMD_KEY_RESPONSE_PREFIX}{req_id}").as_bytes(),
                resp_json.as_bytes(),
                lease,
            )
            .map_err(|error| error.to_string())
    }
}

/// One value of Go's `mockClient.store map[string]any`.
enum MockStoreValue {
    Request(CmdRequest),
    Response(Sender<CmdResponse>),
}

#[derive(Default)]
pub(crate) struct MockState {
    store: HashMap<String, MockStoreValue>,
    command_watchers: Vec<Sender<CmdRequest>>,
    pub(crate) notification_watchers: HashMap<String, Vec<Sender<()>>>,
}

/// Go `mockClient`: the in-memory command and notification client.
#[derive(Clone, Default)]
pub struct MockClient {
    pub(crate) state: Arc<Mutex<MockState>>,
}

impl MockClient {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `(*mockClient).sendCmd`.
    fn send_cmd(
        &self,
        ctx: &Context,
        cmd_type: &str,
        request: &str,
    ) -> (String, Result<Receiver<CmdResponse>, String>) {
        let req_id = new_request_id();
        let req = CmdRequest {
            request_id: req_id.clone(),
            cmd_type: cmd_type.to_owned(),
            data: request.to_owned(),
        };
        let mut state = self.lock();
        state
            .store
            .insert(format!("{TTL_CMD_KEY_REQUEST_PREFIX}{req_id}"), MockStoreValue::Request(req.clone()));
        let (resp_tx, resp_rx) = crossbeam_channel::bounded(1);
        state
            .store
            .insert(format!("{TTL_CMD_KEY_RESPONSE_PREFIX}{req_id}"), MockStoreValue::Response(resp_tx));
        for watcher in &state.command_watchers {
            if let Some(error) = ctx.err() {
                return (req_id, Err(error.to_string()));
            }
            // A watcher whose reader is gone still has Go's buffer to fill;
            // only a full buffer blocks.
            if let Err(crossbeam_channel::TrySendError::Full(_)) = watcher.try_send(req.clone()) {
                debug_assert!(false, "watcher channel should not be blocked");
                return (req_id, Err("watcher channel is blocked".to_owned()));
            }
        }
        (req_id, Ok(resp_rx))
    }
}

/// Go `NewMockCommandClient`.
pub fn new_mock_command_client() -> Arc<dyn CommandClient> {
    Arc::new(MockClient::default())
}

impl CommandClient for MockClient {
    fn command(&self, ctx: &Context, cmd_type: &str, request: &str) -> (String, CommandResult) {
        let (ctx, _cancel) =
            Context::with_timeout(ctx, Duration::from_secs(TTL_CMD_KEY_LEASE_SECONDS as u64));
        let (req_id, resp_rx) = self.send_cmd(&ctx, cmd_type, request);
        let resp_rx = match resp_rx {
            Ok(resp_rx) => resp_rx,
            Err(error) => return (req_id, Err(error)),
        };
        crossbeam_channel::select! {
            recv(resp_rx) -> res => match res {
                Ok(res) => {
                    let result = res.into_result();
                    (req_id, result)
                }
                Err(_) => {
                    debug_assert!(false, "response channel should not be closed");
                    (String::new(), Err("response channel is closed".to_owned()))
                }
            },
            recv(ctx.done()) -> _ => {
                let error = ctx.err().map(|error| error.to_string()).unwrap_or_default();
                (req_id, Err(error))
            }
        }
    }

    fn watch_command(&self, ctx: &Context) -> Receiver<CmdRequest> {
        let mut state = self.lock();
        let (ch_tx, ch_rx) = crossbeam_channel::bounded(16 + state.store.len());
        for (key, value) in &state.store {
            if key.starts_with(TTL_CMD_KEY_REQUEST_PREFIX) {
                if let MockStoreValue::Request(req) = value {
                    let _ = ch_tx.try_send(req.clone());
                }
            }
        }
        state.command_watchers.push(ch_tx.clone());
        drop(state);
        let client = self.clone();
        let done = ctx.done();
        std::thread::Builder::new()
            .name("ttl-mock-cmd-watch".to_owned())
            .spawn(move || {
                let _ = done.recv();
                let mut state = client.lock();
                if let Some(index) = state
                    .command_watchers
                    .iter()
                    .position(|watcher| watcher.same_channel(&ch_tx))
                {
                    state.command_watchers.remove(index);
                }
                // Go's `close(ch)`: the last sender goes with this thread.
            })
            .expect("ttl mock command watch thread spawns");
        ch_rx
    }

    fn take_command(&self, _ctx: &Context, req_id: &str) -> Result<bool, String> {
        let mut state = self.lock();
        let key = format!("{TTL_CMD_KEY_REQUEST_PREFIX}{req_id}");
        Ok(state.store.remove(&key).is_some())
    }

    fn response_command(&self, _ctx: &Context, req_id: &str, obj: CommandResult) -> Result<(), String> {
        let mut state = self.lock();
        let resp = CmdResponse::of(req_id, &obj);
        let resp_key = format!("{TTL_CMD_KEY_RESPONSE_PREFIX}{req_id}");
        let Some(item) = state.store.remove(&resp_key) else {
            return Err(format!("response key not found for: {req_id}"));
        };
        if let MockStoreValue::Response(resp_ch) = item {
            if resp_ch.try_send(resp).is_err() {
                debug_assert!(false, "response channel should not be blocked");
            }
            // Go's `close(respCh)`: `resp_ch` drops here.
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestCommandClient` over the mock client.
    #[test]
    fn mock_command_round_trips_a_trigger_job_request() {
        let cli = new_mock_command_client();
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let watcher = cli.watch_command(&ctx);

        let responder = {
            let cli = Arc::clone(&cli);
            std::thread::spawn(move || {
                let cmd = watcher.recv().expect("a command arrives");
                let request = cmd.get_trigger_ttl_job_request().expect("a trigger request");
                assert_eq!(request.db_name, "test");
                assert_eq!(request.table_name, "t1");
                assert!(cli.take_command(&Context::background(), &cmd.request_id).unwrap());
                assert!(!cli.take_command(&Context::background(), &cmd.request_id).unwrap());
                let response = TriggerNewTTLJobResponse {
                    table_result: Some(vec![TriggerNewTTLJobTableResult {
                        table_id: 1,
                        db_name: "test".to_owned(),
                        table_name: "t1".to_owned(),
                        partition_name: "p1".to_owned(),
                        ..Default::default()
                    }]),
                };
                cli.response_command(
                    &Context::background(),
                    &cmd.request_id,
                    Ok(serde_json::to_string(&response).unwrap()),
                )
                .unwrap();
            })
        };

        let response = trigger_new_ttl_job(&Context::background(), cli.as_ref(), "test", "t1").unwrap();
        responder.join().unwrap();
        let results = response.table_result.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table_id, 1);
        assert_eq!(results[0].partition_name, "p1");

        // An error response is the command's error.
        let watcher = cli.watch_command(&ctx);
        let responder = {
            let cli = Arc::clone(&cli);
            std::thread::spawn(move || {
                let cmd = watcher.recv().unwrap();
                cli.take_command(&Context::background(), &cmd.request_id).unwrap();
                cli.response_command(&Context::background(), &cmd.request_id, Err("mockErr".to_owned()))
                    .unwrap();
            })
        };
        let err = trigger_new_ttl_job(&Context::background(), cli.as_ref(), "test", "t2").unwrap_err();
        responder.join().unwrap();
        assert_eq!(err, "mockErr");
        cancel.cancel();
    }

    #[test]
    fn cmd_request_json_is_gos() {
        let request = CmdRequest {
            request_id: "id".to_owned(),
            cmd_type: TTL_CMD_TYPE_TRIGGER_TTL_JOB.to_owned(),
            data: r#"{"db_name":"test","table_name":"t"}"#.to_owned(),
        };
        let text = request.marshal();
        assert_eq!(
            text,
            r#"{"request_id":"id","cmd_type":"trigger_ttl_job","data":{"db_name":"test","table_name":"t"}}"#
        );
        assert_eq!(CmdRequest::unmarshal(text.as_bytes()).unwrap(), request);
        assert!(CmdRequest {
            cmd_type: "other".to_owned(),
            ..request.clone()
        }
        .get_trigger_ttl_job_request()
        .is_none());
        let result = TriggerNewTTLJobTableResult {
            table_id: 7,
            db_name: "d".to_owned(),
            table_name: "t".to_owned(),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&result).unwrap(),
            r#"{"table_id":7,"db_name":"d","table_name":"t"}"#
        );
    }
}
