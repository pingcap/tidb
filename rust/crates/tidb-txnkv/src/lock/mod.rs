// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded optimistic read-lock recovery.

mod model;
mod pessimistic;
mod resolver;

pub use model::{
    decode_blocking_lock_observation, decode_lock_observation, BlockingLock, LockAdmissionError,
    OptimisticLock, PessimisticLock,
};
pub use pessimistic::{resolve_blocking_locks, SKIP_RESOLVE_THRESHOLD_MS};
pub use resolver::{
    resolve_optimistic_locks, FixedTimestampSource, LockRecoveryClient, LockRecoveryError,
    LockRecoveryResult, ResolvedTxnStatus, TimestampSource,
};
