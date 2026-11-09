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

//! Go `pkg/ttl/client` -- complete package:
//! - [`command`] <- `command.go`: the TTL command protocol (`CmdRequest`,
//!   `TriggerNewTTLJob*`), the `CommandClient` interface, its etcd client
//!   and its in-memory mock.
//! - [`notification`] <- `notification.go`: the `NotificationClient`
//!   interface, its etcd client and its in-memory mock.
//!
//! [`context`] is Go's standard `context` package as `pkg/ttl` selects on it
//! (`Done()`, `Err()`, `WithCancel`, `WithTimeout`); it is a standard-library
//! port, not part of the TTL package.
//!
//! Go's `json.RawMessage` fields carry the raw JSON text as a `String`, and
//! the `any` request/response objects travel as their JSON text: every Go
//! caller marshals or unmarshals them with `encoding/json`, which is what the
//! typed helpers ([`command::trigger_new_ttl_job`]) do.

pub mod command;
pub use tidb_gocontext as context;
pub mod notification;

pub use command::{
    trigger_new_ttl_job, CmdRequest, CommandClient, TriggerNewTTLJobRequest,
    TriggerNewTTLJobResponse, TriggerNewTTLJobTableResult,
};
pub use context::{CancelFunc, Context, CtxError};
pub use notification::NotificationClient;
