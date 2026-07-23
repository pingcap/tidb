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

//! Source-complete tests for the testless `pkg/util/trxevents` package.

use std::sync::{Arc, Mutex};

use tidb_proto::KvrpcLockInfo;
use tidb_txnkv::{
    wrap_cop_meet_lock, ClientSendOption, CopMeetLock, EventCallback, EventType,
    TransactionEvent, EVENT_TYPE_COP_MEET_LOCK,
};

#[test]
fn event_type_keeps_go_int_width_and_iota_value() {
    assert_eq!(std::mem::size_of::<EventType>(), std::mem::size_of::<isize>());
    assert_eq!(EVENT_TYPE_COP_MEET_LOCK, 0);
}

#[test]
fn wrapped_lock_preserves_the_exact_kvproto_message() {
    let lock = KvrpcLockInfo {
        key: b"locked-key".to_vec(),
        primary_lock: b"primary".to_vec(),
        lock_version: 42,
        ..KvrpcLockInfo::default()
    };
    let event = wrap_cop_meet_lock(Some(CopMeetLock {
        lock_info: Some(lock.clone()),
    }));
    assert_eq!(
        event
            .get_cop_meet_lock()
            .and_then(|event| event.lock_info.as_ref()),
        Some(&lock)
    );
}

#[test]
fn wrapped_nil_cop_event_is_distinct_from_an_event_with_nil_lock_info() {
    assert_eq!(wrap_cop_meet_lock(None).get_cop_meet_lock(), None);

    let event = wrap_cop_meet_lock(Some(CopMeetLock { lock_info: None }));
    assert_eq!(
        event.get_cop_meet_lock(),
        Some(&CopMeetLock { lock_info: None })
    );
}

#[test]
#[should_panic(expected = "interface conversion: interface is nil")]
fn default_event_preserves_the_go_nil_interface_assertion_panic() {
    let _ = TransactionEvent::default().get_cop_meet_lock();
}

#[test]
fn client_send_option_uses_the_typed_concurrent_callback() {
    let observed = Arc::new(Mutex::new(None));
    let callback_observation = Arc::clone(&observed);
    let callback: EventCallback = Arc::new(move |event| {
        *callback_observation.lock().expect("lock callback observation") = event
            .get_cop_meet_lock()
            .and_then(|event| event.lock_info.clone());
    });
    let option = ClientSendOption::<()> {
        event_callback: Some(Arc::clone(&callback)),
        ..ClientSendOption::default()
    };

    option.event_callback.as_ref().expect("typed callback")(
        wrap_cop_meet_lock(Some(CopMeetLock {
            lock_info: Some(KvrpcLockInfo {
                key: b"k".to_vec(),
                ..KvrpcLockInfo::default()
            }),
        })),
    );
    assert_eq!(
        observed
            .lock()
            .expect("lock callback observation")
            .as_ref()
            .map(|lock| lock.key.as_slice()),
        Some(b"k".as_slice())
    );
}
