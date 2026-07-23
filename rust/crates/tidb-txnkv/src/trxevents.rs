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

//! Complete transcreation of `pkg/util/trxevents`.
//!
//! The Go package keeps the event tag and erased payload private. Its only
//! public constructor stores a typed `*CopMeetLock`, including a nil pointer.
//! A zero-value `TransactionEvent`, however, has a nil interface and panics
//! when `GetCopMeetLock` performs its type assertion. The private inner enum
//! preserves that otherwise easy-to-collapse distinction without exposing
//! invalid event construction to callers.
//!
//! The source package has no Go tests or support artifacts. Its sole Bazel
//! dependency on kvproto's `kvrpcpb` package is the `tidb-proto` dependency
//! used below.

use std::sync::Arc;

use tidb_proto::KvrpcLockInfo;

/// Source-width transaction event discriminant (`type EventType = int`).
pub type EventType = isize;

/// The coprocessor encountered a lock while reading.
pub const EVENT_TYPE_COP_MEET_LOCK: EventType = 0;

/// Coprocessor-read lock observation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CopMeetLock {
    /// Exact kvproto lock information, or Go's nil `*kvrpcpb.LockInfo`.
    pub lock_info: Option<KvrpcLockInfo>,
}

#[derive(Clone, Debug, Default, PartialEq)]
enum TransactionEventInner {
    #[default]
    Unset,
    CopMeetLock(Option<CopMeetLock>),
}

/// A transaction event with the source package's private tag and payload.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransactionEvent {
    inner: TransactionEventInner,
    event_type: EventType,
}

impl TransactionEvent {
    /// Extracts a coprocessor-lock event.
    ///
    /// A wrapped nil pointer returns `None`. The default value panics exactly
    /// like Go's failed `e.inner.(*CopMeetLock)` assertion on a nil interface.
    #[must_use]
    pub fn get_cop_meet_lock(&self) -> Option<&CopMeetLock> {
        if self.event_type != EVENT_TYPE_COP_MEET_LOCK {
            return None;
        }
        match &self.inner {
            TransactionEventInner::CopMeetLock(event) => event.as_ref(),
            TransactionEventInner::Unset => {
                panic!("interface conversion: interface is nil, not *trxevents.CopMeetLock")
            }
        }
    }
}

/// Wraps a coprocessor-lock event, preserving a typed nil pointer.
#[must_use]
pub const fn wrap_cop_meet_lock(event: Option<CopMeetLock>) -> TransactionEvent {
    TransactionEvent {
        inner: TransactionEventInner::CopMeetLock(event),
        event_type: EVENT_TYPE_COP_MEET_LOCK,
    }
}

/// Concurrent callback invoked for transaction events.
///
/// Go function values are shareable and the DistSQL consumer explicitly warns
/// that invocation may occur from another goroutine. `Arc<dyn Fn + Send +
/// Sync>` preserves those semantics without mutable callback aliasing.
pub type EventCallback = Arc<dyn Fn(TransactionEvent) + Send + Sync + 'static>;
