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

//! Go `pkg/timer/tablestore/notifier.go`: the etcd-backed timer event
//! notifier. The wire document and key namespace intentionally stay compatible
//! with Go so a Rust and Go timer store can watch one another's mutations.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tidb_pd_client::{EtcdClient, EtcdWatcher};
use tidb_util::logutil::bg_logger;

use crate::store::{
    Context, TimerWatchEventNotifier, WatchTimerChan, WatchTimerEvent, WatchTimerEventType,
    WatchTimerResponse,
};
use crate::table_store::json::{parse, write_json_string, JsonValue};
use crate::uuid::new_uuid_hex;

const NOTIFY_TIMEOUT: Duration = Duration::from_secs(20);
const MIN_NOTIFY_INTERVAL: Duration = Duration::from_secs(1);
const ETCD_NOTIFY_KEY_TTL_SECONDS: i64 = 60;
const WATCH_TIMER_EVENT_CREATE: &str = "create";
const WATCH_TIMER_EVENT_UPDATE: &str = "update";
const WATCH_TIMER_EVENT_DELETE: &str = "delete";

struct NotifyEvent {
    tp: String,
    timer_id: String,
    timestamp: i64,
}

struct NotifyState {
    events: Mutex<Vec<NotifyEvent>>,
    changed: Condvar,
}

struct WatchHandle {
    watcher: EtcdWatcher,
    cancel: Arc<AtomicBool>,
    monitor: JoinHandle<()>,
}

/// The etcd-backed implementation of Go's `etcdNotifier`.
pub struct EtcdTimerNotifier {
    etcd: Arc<EtcdClient>,
    key_prefix: String,
    closed: Arc<AtomicBool>,
    notify_state: Arc<NotifyState>,
    worker: Mutex<Option<JoinHandle<()>>>,
    watchers: Mutex<Vec<WatchHandle>>,
}

/// Constructs a notifier using Go's `/tidb/timer/cluster/<id>/notify/` key
/// namespace. The etcd client is lazy, so construction itself does not dial.
pub fn new_etcd_timer_watch_event_notifier(
    cluster_id: u64,
    etcd: Arc<EtcdClient>,
) -> Arc<EtcdTimerNotifier> {
    let key_prefix = format!("/tidb/timer/cluster/{cluster_id}/notify/");
    let key = format!("{}{}", key_prefix, new_uuid_hex());
    let closed = Arc::new(AtomicBool::new(false));
    let notify_state = Arc::new(NotifyState {
        events: Mutex::new(Vec::with_capacity(8)),
        changed: Condvar::new(),
    });

    let worker_closed = Arc::clone(&closed);
    let worker_state = Arc::clone(&notify_state);
    let worker_etcd = Arc::clone(&etcd);
    let worker_key = key.clone();
    let worker = thread::Builder::new()
        .name("timer-etcd-notify".to_owned())
        .spawn(move || notify_loop(worker_etcd, worker_key, worker_closed, worker_state))
        .expect("timer etcd notifier worker thread can be created");

    Arc::new(EtcdTimerNotifier {
        etcd,
        key_prefix,
        closed,
        notify_state,
        worker: Mutex::new(Some(worker)),
        watchers: Mutex::new(Vec::new()),
    })
}

impl TimerWatchEventNotifier for EtcdTimerNotifier {
    fn watch(&self, ctx: &Context) -> WatchTimerChan {
        let (sender, receiver) = sync_channel(0);
        if self.closed.load(Ordering::Acquire) || ctx.is_done() {
            return receiver;
        }

        let cancel = Arc::new(AtomicBool::new(false));
        let callback_cancel = Arc::clone(&cancel);
        let callback_closed = Arc::clone(&self.closed);
        let callback_ctx = ctx.clone();
        let callback_sender = sender.clone();
        let watch_result = self.etcd.watch_prefix_responses(
            self.key_prefix.as_bytes().to_vec(),
            0,
            {
                let cancel = Arc::clone(&cancel);
                let closed = Arc::clone(&self.closed);
                let ctx = ctx.clone();
                move || {
                    cancel.load(Ordering::Acquire)
                        || closed.load(Ordering::Acquire)
                        || ctx.is_done()
                }
            },
            move |watch_response| {
                for event in &watch_response.events {
                    if event.deleted {
                        continue;
                    }
                    let Some(response) = decode_notify_message(&event.value) else {
                        continue;
                    };
                    if !deliver_response(
                        &callback_sender,
                        response,
                        &callback_cancel,
                        &callback_closed,
                        &callback_ctx,
                    ) {
                        return;
                    }
                }
            },
        );

        let Ok(watcher) = watch_result else {
            return receiver;
        };

        let monitor_cancel = Arc::clone(&cancel);
        let monitor_closed = Arc::clone(&self.closed);
        let monitor_ctx = ctx.clone();
        let monitor = thread::Builder::new()
            .name("timer-etcd-watch-monitor".to_owned())
            .spawn(move || {
                while !monitor_closed.load(Ordering::Acquire) && !monitor_ctx.is_done() {
                    if monitor_ctx.wait_done(Duration::from_millis(100)) {
                        break;
                    }
                }
                monitor_cancel.store(true, Ordering::Release);
            })
            .expect("timer etcd watch monitor thread can be created");

        let mut watchers = self.watchers.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            cancel.store(true, Ordering::Release);
            drop(watchers);
            let mut watcher = watcher;
            watcher.shutdown();
            let _ = monitor.join();
        } else {
            watchers.push(WatchHandle {
                watcher,
                cancel,
                monitor,
            });
        }
        receiver
    }

    fn notify(&self, tp: WatchTimerEventType, timer_id: &str) {
        let Some(tp) = event_type_to_wire(tp) else {
            return;
        };
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let mut events = self.notify_state.events.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        events.push(NotifyEvent {
            tp: tp.to_owned(),
            timer_id: timer_id.to_owned(),
            timestamp: unix_timestamp(),
        });
        self.notify_state.changed.notify_one();
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.notify_state.changed.notify_all();

        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }

        let watchers = std::mem::take(&mut *self.watchers.lock().unwrap());
        for mut handle in watchers {
            handle.cancel.store(true, Ordering::Release);
            handle.watcher.shutdown();
            let _ = handle.monitor.join();
        }
    }
}

impl Drop for EtcdTimerNotifier {
    fn drop(&mut self) {
        self.close();
    }
}

fn notify_loop(
    etcd: Arc<EtcdClient>,
    key: String,
    closed: Arc<AtomicBool>,
    state: Arc<NotifyState>,
) {
    let mut lease_id = 0_i64;
    let mut last_notify = Instant::now() - MIN_NOTIFY_INTERVAL;
    loop {
        let mut events = state.events.lock().unwrap();
        let wait_timeout = if lease_id == 0 {
            Duration::from_secs(3600)
        } else {
            Duration::from_secs(20)
        };
        let (guard, timeout) = state
            .changed
            .wait_timeout_while(events, wait_timeout, |events| {
                events.is_empty() && !closed.load(Ordering::Acquire)
            })
            .unwrap();
        events = guard;
        if closed.load(Ordering::Acquire) {
            return;
        }
        if timeout.timed_out() && events.is_empty() && lease_id != 0 {
            drop(events);
            if etcd.lease_keep_alive_once(lease_id).is_err() {
                lease_id = 0;
            }
            continue;
        }
        if events.is_empty() {
            continue;
        }
        let elapsed = last_notify.elapsed();
        if elapsed < MIN_NOTIFY_INTERVAL {
            let _ = state
                .changed
                .wait_timeout(events, MIN_NOTIFY_INTERVAL - elapsed);
            continue;
        }
        if lease_id == 0 {
            drop(events);
            match etcd.lease_grant(ETCD_NOTIFY_KEY_TTL_SECONDS) {
                Ok((id, _)) => lease_id = id,
                Err(error) => {
                    bg_logger().warn(
                        "create timer notifier lease failed",
                        &[tidb_log::Field::new(
                            "error",
                            tidb_log::Value::Str(error.to_string()),
                        )],
                    );
                    continue;
                }
            }
            events = state.events.lock().unwrap();
            if closed.load(Ordering::Acquire) {
                return;
            }
            if events.is_empty() {
                continue;
            }
        }
        let pending = std::mem::take(&mut *events);
        drop(events);
        last_notify = Instant::now();
        let payload = encode_notify_message(&pending);
        if let Err(error) = etcd.put_with_lease_with_timeout(
            key.as_bytes(),
            payload.as_bytes(),
            lease_id,
            NOTIFY_TIMEOUT,
        ) {
            bg_logger().warn(
                "put timer notifier event failed",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(error.to_string()),
                )],
            );
        }
    }
}

fn deliver_response(
    sender: &SyncSender<WatchTimerResponse>,
    mut response: WatchTimerResponse,
    cancel: &Arc<AtomicBool>,
    closed: &Arc<AtomicBool>,
    ctx: &Context,
) -> bool {
    loop {
        match sender.try_send(response) {
            Ok(()) => return true,
            Err(TrySendError::Disconnected(_)) => {
                cancel.store(true, Ordering::Release);
                return false;
            }
            Err(TrySendError::Full(returned)) => {
                response = returned;
                if cancel.load(Ordering::Acquire) || closed.load(Ordering::Acquire) || ctx.is_done()
                {
                    return false;
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn event_type_to_wire(tp: WatchTimerEventType) -> Option<&'static str> {
    match tp {
        WatchTimerEventType::Create => Some(WATCH_TIMER_EVENT_CREATE),
        WatchTimerEventType::Update => Some(WATCH_TIMER_EVENT_UPDATE),
        WatchTimerEventType::Delete => Some(WATCH_TIMER_EVENT_DELETE),
    }
}

fn wire_to_event_type(tp: &str) -> Option<WatchTimerEventType> {
    match tp {
        WATCH_TIMER_EVENT_CREATE => Some(WatchTimerEventType::Create),
        WATCH_TIMER_EVENT_UPDATE => Some(WatchTimerEventType::Update),
        WATCH_TIMER_EVENT_DELETE => Some(WatchTimerEventType::Delete),
        _ => None,
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn encode_notify_message(events: &[NotifyEvent]) -> String {
    let mut output = String::from(r#"{"events":["#);
    for (index, event) in events.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push('{');
        output.push_str(r#""tp":"#);
        write_json_string(&mut output, &event.tp);
        output.push_str(r#","timer_id":"#);
        write_json_string(&mut output, &event.timer_id);
        output.push_str(r#","timestamp":"#);
        output.push_str(&event.timestamp.to_string());
        output.push('}');
    }
    output.push_str("]}");
    output
}

fn decode_notify_message(payload: &[u8]) -> Option<WatchTimerResponse> {
    let text = std::str::from_utf8(payload).ok()?;
    let document = parse(text).ok()?;
    let JsonValue::Object(members) = document else {
        return None;
    };
    let events_value = members
        .iter()
        .find(|(key, _)| key == "events")
        .map(|(_, value)| value);
    let Some(events_value) = events_value else {
        return Some(WatchTimerResponse { events: Vec::new() });
    };
    if matches!(events_value, JsonValue::Null) {
        return Some(WatchTimerResponse { events: Vec::new() });
    }
    let JsonValue::Array(events) = events_value else {
        return None;
    };
    let mut response = WatchTimerResponse {
        events: Vec::with_capacity(events.len()),
    };
    for event in events {
        let JsonValue::Object(members) = event else {
            continue;
        };
        let tp = members
            .iter()
            .find(|(key, _)| key == "tp")
            .and_then(|(_, value)| value.as_str())
            .and_then(wire_to_event_type);
        let timer_id = members
            .iter()
            .find(|(key, _)| key == "timer_id")
            .and_then(|(_, value)| value.as_str());
        let (Some(tp), Some(timer_id)) = (tp, timer_id) else {
            continue;
        };
        if timer_id.is_empty() {
            continue;
        }
        response.events.push(WatchTimerEvent {
            tp,
            timer_id: timer_id.to_owned(),
        });
    }
    Some(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_message_matches_go_json_and_round_trips() {
        let events = vec![
            NotifyEvent {
                tp: WATCH_TIMER_EVENT_CREATE.to_owned(),
                timer_id: "timer<1>\"".to_owned(),
                timestamp: 123,
            },
            NotifyEvent {
                tp: WATCH_TIMER_EVENT_DELETE.to_owned(),
                timer_id: "timer-2".to_owned(),
                timestamp: 456,
            },
        ];
        let encoded = encode_notify_message(&events);
        assert_eq!(
            encoded,
            r#"{"events":[{"tp":"create","timer_id":"timer\u003c1\u003e\"","timestamp":123},{"tp":"delete","timer_id":"timer-2","timestamp":456}]}"#
        );

        let response = decode_notify_message(encoded.as_bytes()).expect("valid Go message");
        assert_eq!(
            response.events,
            vec![
                WatchTimerEvent {
                    tp: WatchTimerEventType::Create,
                    timer_id: "timer<1>\"".to_owned(),
                },
                WatchTimerEvent {
                    tp: WatchTimerEventType::Delete,
                    timer_id: "timer-2".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn decode_skips_invalid_events_like_go() {
        let payload = br#"{"events":[{"tp":"unknown","timer_id":"1"},{"tp":"update","timer_id":""},{"tp":"update","timer_id":"2"}]}"#;
        let response = decode_notify_message(payload).expect("valid JSON document");
        assert_eq!(
            response.events,
            vec![WatchTimerEvent {
                tp: WatchTimerEventType::Update,
                timer_id: "2".to_owned(),
            }]
        );
    }
}
