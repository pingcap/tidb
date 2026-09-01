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

//! The etcd v3 KV/Watch surface PD serves on its own client port.
//!
//! TiDB reaches etcd through exactly these addresses: `pkg/store/etcd.go`
//! `NewEtcdCli` builds its `clientv3` from `store.EtcdAddrs()`, which are the
//! PD endpoints this crate already dials for `pdpb.PD`. PD embeds a real etcd
//! server, so `etcdserverpb.KV` and `etcdserverpb.Watch` answer on the same
//! channel — no second port, no second discovery.
//!
//! Two shapes live here because the two uses have opposite lifetimes:
//!
//! * [`EtcdClient`] is the bounded foreground client. Like [`crate::PdClient`]
//!   it owns one worker thread with one current-thread Tokio runtime, so the
//!   synchronous callers (a DDL commit path) never nest a runtime inside one
//!   they already own. Each call tries the configured endpoints in order and
//!   drops a channel that failed, so a restarted PD is picked up on the next
//!   call rather than poisoning the client.
//! * [`EtcdWatcher`] is a long-lived stream. It gets its own thread and
//!   runtime rather than sharing the foreground worker, because a bidi stream
//!   that blocks would otherwise stall every unary call queued behind it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use etcd_client::{
    Client as RawEtcdClient, Compare, CompareOp, ConnectOptions, DeleteOptions,
    Error as RawEtcdError, EventType, GetOptions, PutOptions, SortOrder, SortTarget, Txn, TxnOp,
    TxnOpResponse,
};
use tokio::sync::watch;

use crate::client::normalize_endpoints;
use crate::{ClusterSecurity, PdClientError};

/// Exact generated method path for the schema-version PUT.
pub const ETCD_PUT_PATH: &str = "/etcdserverpb.KV/Put";
/// Exact generated method path for reading one key back.
pub const ETCD_RANGE_PATH: &str = "/etcdserverpb.KV/Range";
/// Exact generated method path for the watch stream.
pub const ETCD_WATCH_PATH: &str = "/etcdserverpb.Watch/Watch";

/// The etcd key TiDB publishes the cluster's schema version under.
///
/// Source of truth: `pkg/ddl/util/util.go` `DDLGlobalSchemaVersion`.
pub const DDL_GLOBAL_SCHEMA_VERSION_KEY: &str = "/tidb/ddl/global_schema_version";

/// The etcd key TiDB notifies privilege changes on.
///
/// Source of truth: `pkg/domain/domain.go` `privilegeKey`, watched by
/// `Domain.LoadPrivilegeLoop` and written by `Domain.notifyUpdatePrivilege`.
/// Unlike the schema-version key this one carries no state -- the value is a
/// `PrivilegeEvent` message and every reader reloads from `mysql.*` itself --
/// so a node that misses an event loses nothing but time.
pub const PRIVILEGE_UPDATE_KEY: &str = "/tidb/privilege";

/// The `PrivilegeEvent` body that asks every reader to reload every account.
///
/// Go's `PrivilegeEvent` is `{All bool, ServerID uint64, UserList []string}`
/// (`pkg/domain/domain.go`). A reader skips an event whose `ServerID` equals
/// its own, so `0` -- the ID no running TiDB reports, and a value its check
/// ignores anyway -- is what keeps a real TiDB from mistaking this node's
/// announcement for its own. `All` rather than a `UserList` because every
/// reader of this key reloads its whole account table regardless, and a
/// partial list that missed a row would be a silently stale grant.
const PRIVILEGE_UPDATE_ALL_EVENT: &str = r#"{"All":true,"ServerID":0,"UserList":null}"#;

/// The etcd key TiDB notifies `SET GLOBAL` changes on.
///
/// Source of truth: `pkg/domain/domain.go` `sysVarCacheKey` ("/tidb/sysvars"),
/// watched by `Domain.LoadSysVarCacheLoop` and written by
/// `Domain.NotifyUpdateSysVarCache` -- which Go's own `SetGlobalSysVar`
/// (`pkg/session/session.go`) calls right after it `REPLACE INTO
/// mysql.global_variables`s the row, in the same call path rather than
/// leaving it to the loop's own ticker. Like the privilege key, the value
/// carries no state; every reader reloads the whole table from `mysql.*`
/// on any write to this key, and `LoadSysVarCacheLoop`'s 30-second ticker is
/// the fallback for a watch event a reader missed.
pub const SYSVAR_UPDATE_KEY: &str = "/tidb/sysvars";

/// Why an etcd call or watch could not be completed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EtcdError {
    /// A configured endpoint is not a plaintext URI this client can dial.
    InvalidEndpoint {
        /// The endpoint as configured.
        endpoint: String,
        /// Why it was refused.
        message: String,
    },
    /// No endpoint was configured at all.
    NoEndpoint,
    /// The dedicated runtime or thread could not be created.
    Runtime(String),
    /// The worker is gone; the client was shut down.
    Closed,
    /// Every configured endpoint failed, with the last failure retained.
    Unreachable {
        /// The endpoint whose failure is reported.
        endpoint: String,
        /// The gRPC status code identity, or `timeout`.
        code: String,
        /// The failure detail.
        message: String,
    },
    /// etcd answered, but not with the shape this key's contract requires.
    UnexpectedResponse(String),
}

impl std::fmt::Display for EtcdError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEndpoint { endpoint, message } => {
                write!(formatter, "invalid etcd endpoint {endpoint}: {message}")
            }
            Self::NoEndpoint => formatter.write_str("no etcd endpoint was configured"),
            Self::Runtime(message) => write!(formatter, "etcd client runtime failed: {message}"),
            Self::Closed => formatter.write_str("the etcd client is closed"),
            Self::Unreachable {
                endpoint,
                code,
                message,
            } => write!(formatter, "etcd {endpoint} unreachable ({code}): {message}"),
            Self::UnexpectedResponse(message) => {
                write!(formatter, "unexpected etcd response: {message}")
            }
        }
    }
}

impl std::error::Error for EtcdError {}

impl From<PdClientError> for EtcdError {
    fn from(error: PdClientError) -> Self {
        match error {
            PdClientError::InvalidEndpoint { endpoint, message } => {
                Self::InvalidEndpoint { endpoint, message }
            }
            other => Self::Runtime(other.to_string()),
        }
    }
}

enum EtcdCommand {
    /// `KV.Range` over the whole `[prefix, prefix+1)` interval:
    /// `clientv3.WithPrefix()`. Answers `(key, value)` pairs.
    GetPrefix {
        prefix: Vec<u8>,
        reply: mpsc::Sender<Result<Vec<(Vec<u8>, Vec<u8>)>, EtcdError>>,
    },
    GetPrefixMetadata {
        prefix: Vec<u8>,
        reply: mpsc::Sender<Result<(Vec<EtcdKeyValue>, i64), EtcdError>>,
    },
    CreateWithLease {
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        reply: mpsc::Sender<Result<bool, EtcdError>>,
    },
    CreateOrGetWithLease {
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        reply: mpsc::Sender<Result<EtcdCreateOrGet, EtcdError>>,
    },
    Create {
        key: Vec<u8>,
        value: Vec<u8>,
        reply: mpsc::Sender<Result<bool, EtcdError>>,
    },
    CompareValueAndPut {
        key: Vec<u8>,
        expected_value: Vec<u8>,
        value: Vec<u8>,
        reply: mpsc::Sender<Result<bool, EtcdError>>,
    },
    CompareAndPutWithLease {
        key: Vec<u8>,
        expected_mod_revision: i64,
        value: Vec<u8>,
        lease: i64,
        reply: mpsc::Sender<Result<bool, EtcdError>>,
    },
    CompareAndPut {
        key: Vec<u8>,
        expected_mod_revision: i64,
        value: Vec<u8>,
        reply: mpsc::Sender<Result<bool, EtcdError>>,
    },
    DeleteIfModRevision {
        key: Vec<u8>,
        expected_mod_revision: i64,
        reply: mpsc::Sender<Result<bool, EtcdError>>,
    },
    DeleteKeysAndPutWithLease {
        delete_keys: Vec<Vec<u8>>,
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        reply: mpsc::Sender<Result<(), EtcdError>>,
    },
    /// `KV.DeleteRange` of ONE key -- Go's `DeleteKeyFromEtcd`.
    Delete {
        key: Vec<u8>,
        reply: mpsc::Sender<Result<(), EtcdError>>,
    },
    /// `KV.DeleteRange` over `[prefix, prefix+1)` -- Go's
    /// `DeleteKeysWithPrefixFromEtcd`.
    DeletePrefix {
        prefix: Vec<u8>,
        reply: mpsc::Sender<Result<(), EtcdError>>,
    },
    /// One PUT with a lease attached -- the serverinfo key's spelling.
    PutWithLease {
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        reply: mpsc::Sender<Result<(), EtcdError>>,
    },
    /// `Lease.LeaseGrant`: `(lease id, server-chosen TTL seconds)`.
    LeaseGrant {
        ttl_seconds: i64,
        timeout: Duration,
        reply: mpsc::Sender<Result<(i64, i64), EtcdError>>,
    },
    /// `Lease.LeaseRevoke`: every key under the lease expires now.
    LeaseRevoke {
        id: i64,
        reply: mpsc::Sender<Result<(), EtcdError>>,
    },
    /// One round of `Lease.LeaseKeepAlive`: Go's `KeepAliveOnce`. Answers
    /// the refreshed TTL.
    LeaseKeepAliveOnce {
        id: i64,
        reply: mpsc::Sender<Result<i64, EtcdError>>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        timeout: Duration,
        reply: mpsc::Sender<Result<(), EtcdError>>,
    },
    Get {
        key: Vec<u8>,
        timeout: Duration,
        reply: mpsc::Sender<Result<Option<Vec<u8>>, EtcdError>>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

/// One etcd key/value together with the MVCC fields election algorithms use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EtcdKeyValue {
    /// Key bytes.
    pub key: Vec<u8>,
    /// Value bytes.
    pub value: Vec<u8>,
    /// Revision that first created this key.
    pub create_revision: i64,
    /// Revision of the most recent mutation.
    pub mod_revision: i64,
    /// Attached lease ID, or zero when unleased.
    pub lease: i64,
}

/// Result of an atomic create-if-absent transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EtcdCreateOrGet {
    /// Whether the transaction created the requested key.
    pub created: bool,
    /// The existing key observed by the transaction when `created` is false.
    pub existing: Option<EtcdKeyValue>,
}

struct EtcdClientShared {
    endpoints: Vec<String>,
    timeout: Duration,
    security: Arc<ClusterSecurity>,
    commands: mpsc::Sender<EtcdCommand>,
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// A bounded synchronous etcd KV client over the PD endpoints.
///
/// Cloning shares the worker; the last handle dropped stops it. Unlike
/// [`crate::PdClient`] there is no owner/handle distinction, because nothing
/// here holds a stream whose ownership has to be resolved at shutdown.
#[derive(Clone)]
pub struct EtcdClient {
    shared: Arc<EtcdClientShared>,
}

impl std::fmt::Debug for EtcdClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EtcdClient")
            .field("endpoints", &self.shared.endpoints)
            .field("timeout", &self.shared.timeout)
            .finish_non_exhaustive()
    }
}

impl EtcdClient {
    /// Starts the worker over the PD endpoints, in the caller's order.
    ///
    /// Connecting is lazy: this does not prove etcd is reachable, because the
    /// notification path must not make a node's startup depend on a surface
    /// whose failure it deliberately tolerates.
    pub fn connect<I, S>(endpoints: I, timeout: Duration) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::connect_with_security(endpoints, timeout, Arc::new(ClusterSecurity::plaintext()))
    }

    /// Starts the worker, securing every etcd channel with the given cluster
    /// TLS material. Plaintext security keeps [`Self::connect`]'s `http://`
    /// behavior.
    pub fn connect_with_security<I, S>(
        endpoints: I,
        timeout: Duration,
        security: Arc<ClusterSecurity>,
    ) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let endpoints = normalize_endpoints(endpoints, false)?;
        if endpoints.is_empty() {
            return Err(EtcdError::NoEndpoint);
        }
        let (commands, receiver) = mpsc::channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let worker_endpoints = endpoints.clone();
        let worker_security = Arc::clone(&security);
        let worker = std::thread::Builder::new()
            .name("etcd-kv".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    // Every queued and future command answers `Closed` when the
                    // receiver drops with the thread, which is the same
                    // observable outcome as a shut-down worker.
                    return;
                };
                run_kv_worker(
                    &runtime,
                    &worker_endpoints,
                    timeout,
                    &worker_security,
                    &receiver,
                    &shutdown_rx,
                );
            })
            .map_err(|error| EtcdError::Runtime(error.to_string()))?;
        Ok(Self {
            shared: Arc::new(EtcdClientShared {
                endpoints,
                timeout,
                security,
                commands,
                shutdown,
                worker: Mutex::new(Some(worker)),
            }),
        })
    }

    /// The endpoints this client dials, normalized.
    #[must_use]
    pub fn endpoints(&self) -> &[String] {
        &self.shared.endpoints
    }

    /// The ordinary per-operation timeout configured for this client.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.shared.timeout
    }

    /// Starts a reconnecting single-key watch from `start_revision`.
    pub fn watch_key(
        &self,
        key: impl Into<Vec<u8>>,
        start_revision: i64,
        on_event: impl Fn(&EtcdWatchEvent) + Send + 'static,
    ) -> Result<EtcdWatcher, EtcdError> {
        EtcdWatcher::spawn_from_revision(
            self.shared.endpoints.clone(),
            self.shared.timeout,
            Arc::clone(&self.shared.security),
            key,
            start_revision,
            on_event,
        )
    }

    /// Starts the same reconnecting watch while preserving each etcd watch
    /// response as one callback and binding its lifetime to the caller's
    /// cancellation predicate.
    pub fn watch_key_responses(
        &self,
        key: impl Into<Vec<u8>>,
        start_revision: i64,
        is_cancelled: impl Fn() -> bool + Send + 'static,
        on_response: impl Fn(&EtcdWatchResponse) + Send + 'static,
    ) -> Result<EtcdWatcher, EtcdError> {
        EtcdWatcher::spawn_responses_from_revision(
            self.shared.endpoints.clone(),
            self.shared.timeout,
            Arc::clone(&self.shared.security),
            key,
            start_revision,
            false,
            is_cancelled,
            on_response,
        )
    }

    /// Starts a prefix watch from `start_revision`, preserving etcd watch
    /// responses and binding its lifetime to the caller's cancellation
    /// predicate.
    pub fn watch_prefix_responses(
        &self,
        prefix: impl Into<Vec<u8>>,
        start_revision: i64,
        is_cancelled: impl Fn() -> bool + Send + 'static,
        on_response: impl Fn(&EtcdWatchResponse) + Send + 'static,
    ) -> Result<EtcdWatcher, EtcdError> {
        EtcdWatcher::spawn_responses_from_revision(
            self.shared.endpoints.clone(),
            self.shared.timeout,
            Arc::clone(&self.shared.security),
            prefix,
            start_revision,
            true,
            is_cancelled,
            on_response,
        )
    }

    /// Puts one key with no lease attached, exactly as
    /// `OwnerUpdateGlobalVersion` does.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), EtcdError> {
        self.put_with_timeout(key, value, self.shared.timeout)
    }

    /// Puts one key with the caller's exact per-operation deadline.
    ///
    /// Go derives this deadline with `context.WithTimeout` at the call site;
    /// the ordinary [`Self::put`] retains the timeout configured on the
    /// client.
    pub fn put_with_timeout(
        &self,
        key: &[u8],
        value: &[u8],
        timeout: Duration,
    ) -> Result<(), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                timeout,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Puts one key under a lease: the key expires with the lease -- the
    /// spelling `pkg/domain/serverinfo` stores `/tidb/server/info/<id>` with.
    pub fn put_with_lease(&self, key: &[u8], value: &[u8], lease: i64) -> Result<(), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::PutWithLease {
                key: key.to_vec(),
                value: value.to_vec(),
                lease,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Grants a lease of `ttl_seconds`; answers `(lease id, the TTL the
    /// server actually chose)`.
    pub fn lease_grant(&self, ttl_seconds: i64) -> Result<(i64, i64), EtcdError> {
        self.lease_grant_with_timeout(ttl_seconds, self.shared.timeout)
    }

    /// Grants a lease with the caller's exact per-operation deadline.
    pub fn lease_grant_with_timeout(
        &self,
        ttl_seconds: i64,
        timeout: Duration,
    ) -> Result<(i64, i64), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::LeaseGrant {
                ttl_seconds,
                timeout,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Grants and continuously refreshes a lease until the supplied context
    /// predicate fires or the lease can no longer be refreshed.
    pub fn lease_session(
        &self,
        ttl_seconds: i64,
        operation_timeout: Duration,
        is_cancelled: impl Fn() -> bool + Send + 'static,
    ) -> Result<EtcdLeaseSession, EtcdError> {
        let (lease, granted_ttl) = self.lease_grant_with_timeout(ttl_seconds, operation_timeout)?;
        EtcdLeaseSession::spawn(
            self.shared.endpoints.clone(),
            self.shared.timeout,
            Arc::clone(&self.shared.security),
            lease,
            granted_ttl.max(1),
            is_cancelled,
        )
    }

    /// Revokes a lease; every key stored under it expires immediately --
    /// the graceful-shutdown half of the serverinfo session.
    pub fn lease_revoke(&self, id: i64) -> Result<(), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::LeaseRevoke { id, reply })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// One keepalive round for the lease -- Go's `KeepAliveOnce`. Answers
    /// the refreshed TTL in seconds.
    pub fn lease_keep_alive_once(&self, id: i64) -> Result<i64, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::LeaseKeepAliveOnce { id, reply })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Reads every key under the prefix -- `clientv3.WithPrefix()`.
    pub fn get_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::GetPrefix {
                prefix: prefix.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Reads every key under `prefix`, ordered by creation revision, and
    /// retains the MVCC fields used by etcd's concurrency recipes.
    pub fn get_prefix_metadata(&self, prefix: &[u8]) -> Result<Vec<EtcdKeyValue>, EtcdError> {
        self.get_prefix_metadata_with_revision(prefix)
            .map(|(entries, _revision)| entries)
    }

    /// Reads every key under `prefix` and also returns the range response's
    /// header revision, for a race-free range-then-watch handoff.
    pub fn get_prefix_metadata_with_revision(
        &self,
        prefix: &[u8],
    ) -> Result<(Vec<EtcdKeyValue>, i64), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::GetPrefixMetadata {
                prefix: prefix.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Creates `key` under `lease` iff it does not exist.
    pub fn create_with_lease(
        &self,
        key: &[u8],
        value: &[u8],
        lease: i64,
    ) -> Result<bool, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::CreateWithLease {
                key: key.to_vec(),
                value: value.to_vec(),
                lease,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Atomically creates `key` under `lease`, or returns the existing key's
    /// value and MVCC metadata when another owner already holds it.
    pub fn create_or_get_with_lease(
        &self,
        key: &[u8],
        value: &[u8],
        lease: i64,
    ) -> Result<EtcdCreateOrGet, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::CreateOrGetWithLease {
                key: key.to_vec(),
                value: value.to_vec(),
                lease,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Creates `key` without a lease iff it does not exist.
    pub fn create(&self, key: &[u8], value: &[u8]) -> Result<bool, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::Create {
                key: key.to_vec(),
                value: value.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Replaces `key` iff its current value equals `expected_value`.
    pub fn compare_value_and_put(
        &self,
        key: &[u8],
        expected_value: &[u8],
        value: &[u8],
    ) -> Result<bool, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::CompareValueAndPut {
                key: key.to_vec(),
                expected_value: expected_value.to_vec(),
                value: value.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Replaces `key` under `lease` iff its modification revision still
    /// equals `expected_mod_revision`.
    pub fn compare_and_put_with_lease(
        &self,
        key: &[u8],
        expected_mod_revision: i64,
        value: &[u8],
        lease: i64,
    ) -> Result<bool, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::CompareAndPutWithLease {
                key: key.to_vec(),
                expected_mod_revision,
                value: value.to_vec(),
                lease,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Replaces `key` iff its modification revision still equals
    /// `expected_mod_revision`.
    pub fn compare_and_put(
        &self,
        key: &[u8],
        expected_mod_revision: i64,
        value: &[u8],
    ) -> Result<bool, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::CompareAndPut {
                key: key.to_vec(),
                expected_mod_revision,
                value: value.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Deletes `key` iff its modification revision still equals the expected
    /// revision. A false result means another writer changed or removed it.
    pub fn delete_if_mod_revision(
        &self,
        key: &[u8],
        expected_mod_revision: i64,
    ) -> Result<bool, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::DeleteIfModRevision {
                key: key.to_vec(),
                expected_mod_revision,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Atomically deletes the listed keys and writes one leased key.
    pub fn delete_keys_and_put_with_lease(
        &self,
        delete_keys: Vec<Vec<u8>>,
        key: &[u8],
        value: &[u8],
        lease: i64,
    ) -> Result<(), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::DeleteKeysAndPutWithLease {
                delete_keys,
                key: key.to_vec(),
                value: value.to_vec(),
                lease,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Deletes one key -- Go's `DeleteKeyFromEtcd`.
    pub fn delete(&self, key: &[u8]) -> Result<(), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::Delete {
                key: key.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Deletes every key under the prefix -- Go's
    /// `DeleteKeysWithPrefixFromEtcd`.
    pub fn delete_prefix(&self, prefix: &[u8]) -> Result<(), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::DeletePrefix {
                prefix: prefix.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Reads one key. `None` means the key is absent.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EtcdError> {
        self.get_with_timeout(key, self.shared.timeout)
    }

    /// Reads one key with the caller's exact per-operation deadline.
    ///
    /// This is the synchronous equivalent of Go wrapping one `Get` in a
    /// child context. The deadline participates in the cached-client key, so
    /// a client configured for a broader control-plane timeout cannot leak
    /// that broader deadline into this operation.
    pub fn get_with_timeout(
        &self,
        key: &[u8],
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::Get {
                key: key.to_vec(),
                timeout,
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Publishes a schema version under the key TiDB's own owner writes.
    ///
    /// The value is the decimal ASCII text of the `int64`
    /// (`strconv.FormatInt(version, 10)` in `OwnerUpdateGlobalVersion`), and
    /// the PUT carries no lease, so the key outlives the writer's session.
    pub fn put_global_schema_version(&self, version: i64) -> Result<(), EtcdError> {
        self.put(
            DDL_GLOBAL_SCHEMA_VERSION_KEY.as_bytes(),
            version.to_string().as_bytes(),
        )
    }

    /// Announces that this node changed the cluster's accounts, so every
    /// peer's privilege watch reloads instead of waiting out its own tick.
    pub fn notify_privilege_update(&self) -> Result<(), EtcdError> {
        self.put(
            PRIVILEGE_UPDATE_KEY.as_bytes(),
            PRIVILEGE_UPDATE_ALL_EVENT.as_bytes(),
        )
    }

    /// Announces that this node changed a `SET GLOBAL` value, so every
    /// peer's sysvar watch reloads instead of waiting out its own tick. The
    /// PUT carries no payload, matching Go's own empty-value write to this
    /// key (see [`SYSVAR_UPDATE_KEY`]).
    pub fn notify_sysvar_update(&self) -> Result<(), EtcdError> {
        self.put(SYSVAR_UPDATE_KEY.as_bytes(), b"")
    }

    /// Reads back whatever schema version the cluster last published.
    pub fn global_schema_version(&self) -> Result<Option<i64>, EtcdError> {
        let Some(value) = self.get(DDL_GLOBAL_SCHEMA_VERSION_KEY.as_bytes())? else {
            return Ok(None);
        };
        parse_global_schema_version(&value).map(Some)
    }
}

impl Drop for EtcdClient {
    fn drop(&mut self) {
        // Only the last handle stops the worker: an in-flight call on another
        // clone must not lose its runtime under it.
        if Arc::strong_count(&self.shared) > 1 {
            return;
        }
        let _ = self.shared.shutdown.send(true);
        let (reply, response) = mpsc::channel();
        if self
            .shared
            .commands
            .send(EtcdCommand::Close { reply })
            .is_ok()
        {
            let _ = response.recv();
        }
        let worker = match self.shared.worker.lock() {
            Ok(mut worker) => worker.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }
}

/// The decimal ASCII int64 contract of the global schema version value.
fn parse_global_schema_version(value: &[u8]) -> Result<i64, EtcdError> {
    let text = std::str::from_utf8(value)
        .map_err(|_| EtcdError::UnexpectedResponse(format!("non-UTF-8 version value {value:?}")))?;
    text.trim().parse::<i64>().map_err(|error| {
        EtcdError::UnexpectedResponse(format!("version value {text:?} is not an int64: {error}"))
    })
}

fn run_kv_worker(
    runtime: &tokio::runtime::Runtime,
    endpoints: &[String],
    timeout: Duration,
    security: &ClusterSecurity,
    receiver: &mpsc::Receiver<EtcdCommand>,
    shutdown: &watch::Receiver<bool>,
) {
    let mut clients: HashMap<(String, Duration), RawEtcdClient> = HashMap::new();
    while let Ok(command) = receiver.recv() {
        match command {
            EtcdCommand::Close { reply } => {
                let _ = reply.send(());
                return;
            }
            _ if *shutdown.borrow() => match command {
                EtcdCommand::Put { reply, .. } | EtcdCommand::PutWithLease { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::Get { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::GetPrefix { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::GetPrefixMetadata { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::CreateWithLease { reply, .. }
                | EtcdCommand::Create { reply, .. }
                | EtcdCommand::CompareValueAndPut { reply, .. }
                | EtcdCommand::CompareAndPut { reply, .. }
                | EtcdCommand::CompareAndPutWithLease { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::DeleteIfModRevision { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::CreateOrGetWithLease { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::DeleteKeysAndPutWithLease { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::Delete { reply, .. } | EtcdCommand::DeletePrefix { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::LeaseGrant { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::LeaseRevoke { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::LeaseKeepAliveOnce { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::Close { .. } => unreachable!("handled above"),
            },
            EtcdCommand::Put {
                key,
                value,
                timeout: operation_timeout,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    operation_timeout,
                    security,
                    |runtime, mut client| {
                        runtime
                            .block_on(client.put(key.clone(), value.clone(), None))
                            .map(|_| ())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::PutWithLease {
                key,
                value,
                lease,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let options = PutOptions::new().with_lease(lease);
                        runtime
                            .block_on(client.put(key.clone(), value.clone(), Some(options)))
                            .map(|_| ())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::LeaseGrant {
                ttl_seconds,
                timeout: operation_timeout,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    operation_timeout,
                    security,
                    |runtime, mut client| {
                        runtime
                            .block_on(client.lease_grant(ttl_seconds, None))
                            .map(|response| (response.id(), response.ttl()))
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::LeaseRevoke { id, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| runtime.block_on(client.lease_revoke(id)).map(|_| ()),
                );
                let _ = reply.send(result);
            }
            EtcdCommand::LeaseKeepAliveOnce { id, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        // `Client::lease_keep_alive` itself sends the first
                        // request and validates the first response
                        // (`ttl > 0`) internally, but does not hand that
                        // response's TTL back to the caller. Go's
                        // `KeepAliveOnce` is a single request/response
                        // round trip; the closest equivalent on this crate's
                        // public API sends one more request on the same
                        // stream and reads its response for the TTL, which
                        // is behaviorally equivalent (a freshly refreshed
                        // lease TTL is returned) at the cost of one extra
                        // round trip on this uncommon, non-hot-path call.
                        runtime.block_on(async {
                            let (mut keeper, mut stream) = client.lease_keep_alive(id).await?;
                            keeper.keep_alive().await?;
                            match stream.message().await? {
                                Some(response) => Ok(response.ttl()),
                                None => Err(RawEtcdError::LeaseKeepAliveError(
                                    "keepalive stream closed without a response".to_owned(),
                                )),
                            }
                        })
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::GetPrefix { prefix, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let options = GetOptions::new().with_prefix();
                        runtime
                            .block_on(client.get(prefix.clone(), Some(options)))
                            .map(|mut response| {
                                response
                                    .take_kvs()
                                    .into_iter()
                                    .map(|kv| kv.into_key_value())
                                    .collect()
                            })
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::GetPrefixMetadata { prefix, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let options = GetOptions::new()
                            .with_prefix()
                            .with_sort(SortTarget::Create, SortOrder::Ascend);
                        runtime
                            .block_on(client.get(prefix.clone(), Some(options)))
                            .map(|response| {
                                let revision =
                                    response.header().map_or(0, |header| header.revision());
                                let entries = response
                                    .kvs()
                                    .iter()
                                    .map(|kv| EtcdKeyValue {
                                        key: kv.key().to_vec(),
                                        value: kv.value().to_vec(),
                                        create_revision: kv.create_revision(),
                                        mod_revision: kv.mod_revision(),
                                        lease: kv.lease(),
                                    })
                                    .collect();
                                (entries, revision)
                            })
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::CreateWithLease {
                key,
                value,
                lease,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let put = TxnOp::put(
                            key.clone(),
                            value.clone(),
                            Some(PutOptions::new().with_lease(lease)),
                        );
                        let txn = Txn::new()
                            .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
                            .and_then([put]);
                        runtime
                            .block_on(client.txn(txn))
                            .map(|response| response.succeeded())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::CreateOrGetWithLease {
                key,
                value,
                lease,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let put = TxnOp::put(
                            key.clone(),
                            value.clone(),
                            Some(PutOptions::new().with_lease(lease)),
                        );
                        let txn = Txn::new()
                            .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
                            .and_then([put])
                            .or_else([TxnOp::get(key.clone(), None)]);
                        runtime.block_on(client.txn(txn)).map(|response| {
                            if response.succeeded() {
                                return EtcdCreateOrGet {
                                    created: true,
                                    existing: None,
                                };
                            }
                            let existing = response.op_responses().into_iter().find_map(|op| {
                                let TxnOpResponse::Get(get) = op else {
                                    return None;
                                };
                                get.kvs().first().map(|kv| EtcdKeyValue {
                                    key: kv.key().to_vec(),
                                    value: kv.value().to_vec(),
                                    create_revision: kv.create_revision(),
                                    mod_revision: kv.mod_revision(),
                                    lease: kv.lease(),
                                })
                            });
                            EtcdCreateOrGet {
                                created: false,
                                existing,
                            }
                        })
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::Create { key, value, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let txn = Txn::new()
                            .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
                            .and_then([TxnOp::put(key.clone(), value.clone(), None)]);
                        runtime
                            .block_on(client.txn(txn))
                            .map(|response| response.succeeded())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::CompareValueAndPut {
                key,
                expected_value,
                value,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let txn = Txn::new()
                            .when([Compare::value(
                                key.clone(),
                                CompareOp::Equal,
                                expected_value.clone(),
                            )])
                            .and_then([TxnOp::put(key.clone(), value.clone(), None)]);
                        runtime
                            .block_on(client.txn(txn))
                            .map(|response| response.succeeded())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::CompareAndPutWithLease {
                key,
                expected_mod_revision,
                value,
                lease,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let put = TxnOp::put(
                            key.clone(),
                            value.clone(),
                            Some(PutOptions::new().with_lease(lease)),
                        );
                        let txn = Txn::new()
                            .when([Compare::mod_revision(
                                key.clone(),
                                CompareOp::Equal,
                                expected_mod_revision,
                            )])
                            .and_then([put]);
                        runtime
                            .block_on(client.txn(txn))
                            .map(|response| response.succeeded())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::CompareAndPut {
                key,
                expected_mod_revision,
                value,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let txn = Txn::new()
                            .when([Compare::mod_revision(
                                key.clone(),
                                CompareOp::Equal,
                                expected_mod_revision,
                            )])
                            .and_then([TxnOp::put(key.clone(), value.clone(), None)]);
                        runtime
                            .block_on(client.txn(txn))
                            .map(|response| response.succeeded())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::DeleteIfModRevision {
                key,
                expected_mod_revision,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let txn = Txn::new()
                            .when([Compare::mod_revision(
                                key.clone(),
                                CompareOp::Equal,
                                expected_mod_revision,
                            )])
                            .and_then([TxnOp::delete(key.clone(), None)]);
                        runtime
                            .block_on(client.txn(txn))
                            .map(|response| response.succeeded())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::DeleteKeysAndPutWithLease {
                delete_keys,
                key,
                value,
                lease,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let mut operations = delete_keys
                            .iter()
                            .cloned()
                            .map(|key| TxnOp::delete(key, None))
                            .collect::<Vec<_>>();
                        operations.push(TxnOp::put(
                            key.clone(),
                            value.clone(),
                            Some(PutOptions::new().with_lease(lease)),
                        ));
                        runtime
                            .block_on(client.txn(Txn::new().and_then(operations)))
                            .map(|_| ())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::Delete { key, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        runtime
                            .block_on(client.delete(key.clone(), None))
                            .map(|_| ())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::DeletePrefix { prefix, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    security,
                    |runtime, mut client| {
                        let options = DeleteOptions::new().with_prefix();
                        runtime
                            .block_on(client.delete(prefix.clone(), Some(options)))
                            .map(|_| ())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::Get {
                key,
                timeout: operation_timeout,
                reply,
            } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    operation_timeout,
                    security,
                    |runtime, mut client| {
                        let options = GetOptions::new().with_limit(1);
                        runtime
                            .block_on(client.get(key.clone(), Some(options)))
                            .map(|mut response| {
                                response
                                    .take_kvs()
                                    .into_iter()
                                    .next()
                                    .map(|kv| kv.into_key_value().1)
                            })
                    },
                );
                let _ = reply.send(result);
            }
        }
    }
}

/// Runs one call against the first endpoint that answers.
///
/// A client that failed is dropped rather than reused: etcd inside a
/// restarted PD gets a fresh connection on the next call instead of a
/// permanently broken one.
fn across_endpoints<T>(
    runtime: &tokio::runtime::Runtime,
    endpoints: &[String],
    clients: &mut HashMap<(String, Duration), RawEtcdClient>,
    timeout: Duration,
    security: &ClusterSecurity,
    mut call: impl FnMut(&tokio::runtime::Runtime, RawEtcdClient) -> Result<T, RawEtcdError>,
) -> Result<T, EtcdError> {
    let mut last = None;
    for endpoint in endpoints {
        let cache_key = (endpoint.clone(), timeout);
        if !clients.contains_key(&cache_key) {
            match connect_etcd_client(runtime, endpoint, timeout, security) {
                Ok(client) => {
                    clients.insert(cache_key.clone(), client);
                }
                Err(error) => {
                    last = Some(error);
                    continue;
                }
            }
        }
        let client = clients
            .get(&cache_key)
            .expect("the client was just inserted")
            .clone();
        match call(runtime, client) {
            Ok(value) => return Ok(value),
            Err(error) => {
                clients.remove(&cache_key);
                last = Some(classify_rpc_error(endpoint, error));
            }
        }
    }
    Err(last.unwrap_or(EtcdError::NoEndpoint))
}

/// etcd routing identity stays plaintext-shaped
/// ([`crate::security::secure_endpoint`]'s doc), but `etcd_client`'s own
/// endpoint parser rejects an explicit `http://` prefix when TLS options are
/// set ("TLS options are only supported with HTTPS URLs"); it derives the
/// scheme itself from whether TLS is configured. Strip the prefix this
/// crate's own normalization adds so `etcd_client` can make that choice.
fn strip_scheme(endpoint: &str) -> &str {
    endpoint.strip_prefix("http://").unwrap_or(endpoint)
}

fn classify_rpc_error(endpoint: &str, error: RawEtcdError) -> EtcdError {
    match error {
        RawEtcdError::GRpcStatus(status) => EtcdError::Unreachable {
            endpoint: endpoint.to_owned(),
            code: format!("{:?}", status.code()),
            message: status.message().to_owned(),
        },
        other => EtcdError::Unreachable {
            endpoint: endpoint.to_owned(),
            code: "transport".to_owned(),
            message: other.to_string(),
        },
    }
}

/// Builds the TLS half of [`ConnectOptions`] from this crate's shared
/// [`ClusterSecurity`], leaving timeouts to the caller: a one-shot KV call
/// wants a bounded per-request timeout (`ConnectOptions::with_timeout`), but
/// a long-lived watch stream must not have one -- a `grpc-timeout` deadline
/// applies to the whole streaming RPC's lifetime, not just its first
/// message, so applying it here would silently kill every watch after
/// `timeout` elapsed.
fn etcd_connect_options_with_tls(
    endpoint: &str,
    security: &ClusterSecurity,
    options: ConnectOptions,
) -> Result<ConnectOptions, EtcdError> {
    match security
        .client_tls_config()
        .map_err(|error| EtcdError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: error.to_string(),
        })? {
        Some(tls) => Ok(options.with_tls(tls)),
        None => Ok(options),
    }
}

fn connect_etcd_client(
    runtime: &tokio::runtime::Runtime,
    endpoint: &str,
    timeout: Duration,
    security: &ClusterSecurity,
) -> Result<RawEtcdClient, EtcdError> {
    let options = etcd_connect_options_with_tls(
        endpoint,
        security,
        ConnectOptions::new()
            .with_connect_timeout(timeout)
            .with_timeout(timeout),
    )?;
    runtime
        .block_on(RawEtcdClient::connect(
            [strip_scheme(endpoint)],
            Some(options),
        ))
        .map_err(|error| EtcdError::Unreachable {
            endpoint: endpoint.to_owned(),
            code: "connect".to_owned(),
            message: error.to_string(),
        })
}

/// What one watched key change was.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EtcdWatchEvent {
    /// Changed key.
    pub key: Vec<u8>,
    /// Whether the key was written or deleted.
    pub deleted: bool,
    /// The value written, empty for a delete.
    pub value: Vec<u8>,
    /// The store revision the change was applied at.
    pub mod_revision: i64,
}

/// One Go `clientv3.WatchResponse` after client-side create-frame filtering.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EtcdWatchResponse {
    /// Store revision carried by the response header.
    pub header_revision: i64,
    /// Key changes delivered in this response.
    pub events: Vec<EtcdWatchEvent>,
    /// Whether etcd canceled this watch.
    pub canceled: bool,
    /// Minimum available revision when cancellation was caused by compaction.
    pub compact_revision: i64,
    /// Server-provided cancellation reason.
    pub cancel_reason: String,
}

/// What the watch thread has observed, for tests and for operators.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EtcdWatchStats {
    /// Watch streams successfully created, including reconnections.
    pub streams: u64,
    /// Key changes delivered to the callback.
    pub events: u64,
    /// Streams that ended or failed and had to be re-established.
    pub reconnects: u64,
}

#[derive(Debug, Default)]
struct WatchCounters {
    streams: AtomicU64,
    events: AtomicU64,
    reconnects: AtomicU64,
}

impl WatchCounters {
    fn snapshot(&self) -> EtcdWatchStats {
        EtcdWatchStats {
            streams: self.streams.load(Ordering::Acquire),
            events: self.events.load(Ordering::Acquire),
            reconnects: self.reconnects.load(Ordering::Acquire),
        }
    }
}

/// A Go `concurrency.Session`-style lease refresher.
///
/// Dropping the handle or canceling its context stops refreshes and lets the
/// lease expire. It deliberately does not revoke the lease: pinned
/// `pkg/ddl/serverstate` retains a session but never calls `Session.Close`.
#[derive(Debug)]
pub struct EtcdLeaseSession {
    shutdown: watch::Sender<bool>,
    worker: Option<JoinHandle<()>>,
}

impl EtcdLeaseSession {
    fn spawn<I, S>(
        endpoints: I,
        timeout: Duration,
        security: Arc<ClusterSecurity>,
        lease: i64,
        ttl_seconds: i64,
        is_cancelled: impl Fn() -> bool + Send + 'static,
    ) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let endpoints = normalize_endpoints(endpoints, false)?;
        if endpoints.is_empty() {
            return Err(EtcdError::NoEndpoint);
        }
        let (shutdown, shutdown_rx) = watch::channel(false);
        let worker = std::thread::Builder::new()
            .name("etcd-lease-session".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(keep_lease_alive(
                    &endpoints,
                    timeout,
                    &security,
                    lease,
                    ttl_seconds,
                    &is_cancelled,
                    shutdown_rx,
                ));
            })
            .map_err(|error| EtcdError::Runtime(error.to_string()))?;
        Ok(Self {
            shutdown,
            worker: Some(worker),
        })
    }

    /// Stops refreshing and waits for the session worker. The lease is left
    /// to expire, matching a canceled Go session context.
    pub fn shutdown(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for EtcdLeaseSession {
    fn drop(&mut self) {
        self.shutdown();
    }
}

const LEASE_RECONNECT_DELAY: Duration = Duration::from_millis(500);

async fn keep_lease_alive(
    endpoints: &[String],
    timeout: Duration,
    security: &ClusterSecurity,
    lease: i64,
    ttl_seconds: i64,
    is_cancelled: &(impl Fn() -> bool + Send + 'static),
    mut shutdown: watch::Receiver<bool>,
) {
    let ttl = Duration::from_secs(u64::try_from(ttl_seconds).unwrap_or(1));
    let first_response_timeout = timeout
        .checked_add(Duration::from_secs(1))
        .unwrap_or(Duration::MAX);
    let mut deadline = std::time::Instant::now()
        .checked_add(first_response_timeout)
        .unwrap_or(std::time::Instant::now());

    'reconnect: loop {
        if *shutdown.borrow() || is_cancelled() || std::time::Instant::now() >= deadline {
            return;
        }
        for endpoint in endpoints {
            let Ok(options) = etcd_connect_options_with_tls(
                endpoint,
                security,
                ConnectOptions::new().with_connect_timeout(timeout),
            ) else {
                continue;
            };
            let mut client = tokio::select! {
                result = RawEtcdClient::connect([strip_scheme(endpoint)], Some(options)) => {
                    let Ok(client) = result else { continue; };
                    client
                }
                _ = shutdown.changed() => return,
                () = wait_until_cancelled(is_cancelled) => return,
                () = tokio::time::sleep(deadline.saturating_duration_since(std::time::Instant::now())) => return,
            };
            let (mut keeper, mut stream) = tokio::select! {
                result = client.lease_keep_alive(lease) => {
                    let Ok(session) = result else { continue; };
                    session
                }
                _ = shutdown.changed() => return,
                () = wait_until_cancelled(is_cancelled) => return,
                () = tokio::time::sleep(deadline.saturating_duration_since(std::time::Instant::now())) => return,
            };

            // `lease_keep_alive` has already sent and validated the first
            // response. Subsequent sends follow etcd/clientv3's TTL/3 cadence;
            // while a response is overdue it retries every 500ms.
            deadline = std::time::Instant::now()
                .checked_add(ttl)
                .unwrap_or(std::time::Instant::now());
            let mut next_send = std::time::Instant::now()
                .checked_add(ttl / 3)
                .unwrap_or(std::time::Instant::now());
            loop {
                let now = std::time::Instant::now();
                let message = tokio::select! {
                    result = stream.message() => Some(result),
                    () = tokio::time::sleep(next_send.saturating_duration_since(now)) => {
                        if keeper.keep_alive().await.is_err() {
                            break;
                        }
                        next_send = std::time::Instant::now()
                            .checked_add(LEASE_RECONNECT_DELAY)
                            .unwrap_or(std::time::Instant::now());
                        None
                    }
                    _ = shutdown.changed() => return,
                    () = wait_until_cancelled(is_cancelled) => return,
                    () = tokio::time::sleep(deadline.saturating_duration_since(now)) => return,
                };
                let Some(message) = message else {
                    continue;
                };
                let Ok(Some(response)) = message else {
                    break;
                };
                if response.ttl() <= 0 {
                    return;
                }
                let response_ttl = Duration::from_secs(u64::try_from(response.ttl()).unwrap_or(1));
                let received = std::time::Instant::now();
                deadline = received.checked_add(response_ttl).unwrap_or(received);
                next_send = received.checked_add(response_ttl / 3).unwrap_or(received);
            }
        }

        tokio::select! {
            () = tokio::time::sleep(LEASE_RECONNECT_DELAY) => continue 'reconnect,
            _ = shutdown.changed() => return,
            () = wait_until_cancelled(is_cancelled) => return,
            () = tokio::time::sleep(deadline.saturating_duration_since(std::time::Instant::now())) => return,
        }
    }
}

/// A single-key etcd watch, running until dropped.
///
/// Go's `Syncer.SyncLoop` treats a closed watch channel as "need rewatch" and
/// re-establishes it while the `lease/2` ticker keeps reloading meanwhile
/// (`pkg/infoschema/issyncer/syncer.go`). The same division holds here: this
/// thread reconnects on its own, and the caller's tick is what guarantees
/// progress while it is disconnected.
#[derive(Debug)]
pub struct EtcdWatcher {
    shutdown: watch::Sender<bool>,
    stats: Arc<WatchCounters>,
    worker: Option<JoinHandle<()>>,
}

impl EtcdWatcher {
    /// Starts watching one key, calling `on_event` for every change.
    ///
    /// The callback runs on the watch thread and must not block for long; the
    /// intended use is nudging a reload thread, not reloading inline.
    pub fn spawn<I, S>(
        endpoints: I,
        timeout: Duration,
        key: impl Into<Vec<u8>>,
        on_event: impl Fn(&EtcdWatchEvent) + Send + 'static,
    ) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::spawn_with_security(
            endpoints,
            timeout,
            Arc::new(ClusterSecurity::plaintext()),
            key,
            on_event,
        )
    }

    /// Starts a watch, securing the stream channel with the given cluster TLS
    /// material. Plaintext security keeps [`Self::spawn`]'s `http://` behavior.
    pub fn spawn_with_security<I, S>(
        endpoints: I,
        timeout: Duration,
        security: Arc<ClusterSecurity>,
        key: impl Into<Vec<u8>>,
        on_event: impl Fn(&EtcdWatchEvent) + Send + 'static,
    ) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::spawn_from_revision(endpoints, timeout, security, key, 0, on_event)
    }

    fn spawn_from_revision<I, S>(
        endpoints: I,
        timeout: Duration,
        security: Arc<ClusterSecurity>,
        key: impl Into<Vec<u8>>,
        start_revision: i64,
        on_event: impl Fn(&EtcdWatchEvent) + Send + 'static,
    ) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::spawn_responses_from_revision(
            endpoints,
            timeout,
            security,
            key,
            start_revision,
            false,
            || false,
            move |response| {
                for event in &response.events {
                    on_event(event);
                }
            },
        )
    }

    fn spawn_responses_from_revision<I, S>(
        endpoints: I,
        timeout: Duration,
        security: Arc<ClusterSecurity>,
        key: impl Into<Vec<u8>>,
        start_revision: i64,
        with_prefix: bool,
        is_cancelled: impl Fn() -> bool + Send + 'static,
        on_response: impl Fn(&EtcdWatchResponse) + Send + 'static,
    ) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let endpoints = normalize_endpoints(endpoints, false)?;
        if endpoints.is_empty() {
            return Err(EtcdError::NoEndpoint);
        }
        let key = key.into();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let stats = Arc::new(WatchCounters::default());
        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("etcd-watch".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(watch_forever(
                    &endpoints,
                    timeout,
                    &security,
                    &key,
                    start_revision,
                    with_prefix,
                    &is_cancelled,
                    &on_response,
                    &worker_stats,
                    shutdown_rx,
                ));
            })
            .map_err(|error| EtcdError::Runtime(error.to_string()))?;
        Ok(Self {
            shutdown,
            stats,
            worker: Some(worker),
        })
    }

    /// What the watch thread has observed so far.
    #[must_use]
    pub fn stats(&self) -> EtcdWatchStats {
        self.stats.snapshot()
    }

    /// Stops the watch thread and waits for it. Idempotent; [`Drop`] calls it.
    pub fn shutdown(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for EtcdWatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Pinned clientv3's maximum unavailable-stream backoff.
const WATCH_RECONNECT_DELAY: Duration = Duration::from_millis(100);

async fn watch_forever(
    endpoints: &[String],
    timeout: Duration,
    security: &ClusterSecurity,
    key: &[u8],
    mut next_revision: i64,
    with_prefix: bool,
    is_cancelled: &(impl Fn() -> bool + Send + 'static),
    on_response: &(impl Fn(&EtcdWatchResponse) + Send + 'static),
    stats: &WatchCounters,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut established = false;
    loop {
        if *shutdown.borrow() || is_cancelled() {
            return;
        }
        for endpoint in endpoints {
            if *shutdown.borrow() || is_cancelled() {
                return;
            }
            if established {
                stats.reconnects.fetch_add(1, Ordering::AcqRel);
                established = false;
            }
            match watch_one_stream(
                endpoint,
                timeout,
                security,
                key,
                &mut next_revision,
                with_prefix,
                is_cancelled,
                on_response,
                stats,
                &mut shutdown,
            )
            .await
            {
                WatchEnd::Shutdown | WatchEnd::Canceled | WatchEnd::ContextCancelled => return,
                WatchEnd::Disconnected => {
                    established = true;
                    break;
                }
                WatchEnd::NotEstablished => {}
            }
        }
        // Either the stream ended or no endpoint accepted one. Waiting before
        // the next attempt is what keeps a down PD from becoming a spin loop;
        // the caller's lease tick is still reloading throughout.
        tokio::select! {
            () = tokio::time::sleep(WATCH_RECONNECT_DELAY) => {}
            _ = shutdown.changed() => return,
            () = wait_until_cancelled(is_cancelled) => return,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WatchEnd {
    NotEstablished,
    Disconnected,
    Canceled,
    ContextCancelled,
    Shutdown,
}

async fn wait_until_cancelled(is_cancelled: &(impl Fn() -> bool + Send + 'static)) {
    while !is_cancelled() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Runs one watch stream to its end.
async fn watch_one_stream(
    endpoint: &str,
    timeout: Duration,
    security: &ClusterSecurity,
    key: &[u8],
    next_revision: &mut i64,
    with_prefix: bool,
    is_cancelled: &(impl Fn() -> bool + Send + 'static),
    on_response: &(impl Fn(&EtcdWatchResponse) + Send + 'static),
    stats: &WatchCounters,
    shutdown: &mut watch::Receiver<bool>,
) -> WatchEnd {
    let Ok(options) = etcd_connect_options_with_tls(
        endpoint,
        security,
        ConnectOptions::new().with_connect_timeout(timeout),
    ) else {
        return WatchEnd::NotEstablished;
    };
    let mut client = tokio::select! {
        result = RawEtcdClient::connect([strip_scheme(endpoint)], Some(options)) => {
            let Ok(client) = result else {
                return WatchEnd::NotEstablished;
            };
            client
        }
        _ = shutdown.changed() => return WatchEnd::Shutdown,
        () = wait_until_cancelled(is_cancelled) => return WatchEnd::ContextCancelled,
    };
    let options = if *next_revision > 0 || with_prefix {
        let mut options = etcd_client::WatchOptions::new();
        if *next_revision > 0 {
            options = options.with_start_revision(*next_revision);
        }
        if with_prefix {
            options = options.with_prefix();
        }
        Some(options)
    } else {
        None
    };
    let mut stream = tokio::select! {
        result = client.watch(key.to_vec(), options) => {
            let Ok(stream) = result else {
                return WatchEnd::NotEstablished;
            };
            stream
        }
        _ = shutdown.changed() => return WatchEnd::Shutdown,
        () = wait_until_cancelled(is_cancelled) => return WatchEnd::ContextCancelled,
    };
    stats.streams.fetch_add(1, Ordering::AcqRel);
    loop {
        let message = tokio::select! {
            message = stream.message() => message,
            _ = shutdown.changed() => return WatchEnd::Shutdown,
            () = wait_until_cancelled(is_cancelled) => return WatchEnd::ContextCancelled,
        };
        let Ok(Some(response)) = message else {
            return WatchEnd::Disconnected;
        };
        let header_revision = response.header().map_or(0, |header| header.revision());
        if response.created() {
            if *next_revision == 0 {
                *next_revision = header_revision;
            }
            continue;
        }
        if header_revision != 0 {
            *next_revision = header_revision + 1;
        }
        let raw_events = response.events();
        let mut events = Vec::with_capacity(raw_events.len());
        for event in raw_events {
            let deleted = event.event_type() == EventType::Delete;
            let (key, value, mod_revision) = event.kv().map_or_else(
                || (Vec::new(), Vec::new(), 0),
                |kv| (kv.key().to_vec(), kv.value().to_vec(), kv.mod_revision()),
            );
            *next_revision = (*next_revision).max(mod_revision + 1);
            stats.events.fetch_add(1, Ordering::AcqRel);
            events.push(EtcdWatchEvent {
                key,
                deleted,
                value,
                mod_revision,
            });
        }
        let canceled = response.canceled();
        on_response(&EtcdWatchResponse {
            header_revision,
            events,
            canceled,
            compact_revision: response.compact_revision(),
            cancel_reason: response.cancel_reason().to_owned(),
        });
        if canceled {
            return WatchEnd::Canceled;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_global_schema_version_value_is_decimal_ascii() {
        // `OwnerUpdateGlobalVersion` writes `strconv.FormatInt(version, 10)`:
        // text, not a fixed-width or varint encoding.
        assert_eq!(parse_global_schema_version(b"127").unwrap(), 127);
        assert_eq!(parse_global_schema_version(b"-1").unwrap(), -1);
        assert_eq!(
            parse_global_schema_version(i64::MAX.to_string().as_bytes()).unwrap(),
            i64::MAX
        );
        assert!(matches!(
            parse_global_schema_version(&[0, 0, 0, 7]),
            Err(EtcdError::UnexpectedResponse(_))
        ));
        assert!(matches!(
            parse_global_schema_version(b"1.5"),
            Err(EtcdError::UnexpectedResponse(_))
        ));
    }

    #[test]
    fn the_watched_key_is_the_one_the_ddl_owner_writes() {
        // A typo here would make the watch silently never fire, which is
        // exactly the failure the lease tick would hide.
        assert_eq!(
            DDL_GLOBAL_SCHEMA_VERSION_KEY,
            "/tidb/ddl/global_schema_version"
        );
    }

    #[test]
    fn a_client_without_endpoints_is_refused_rather_than_started() {
        assert_eq!(
            EtcdClient::connect(Vec::<String>::new(), Duration::from_secs(1)).unwrap_err(),
            EtcdError::NoEndpoint
        );
        assert_eq!(
            EtcdWatcher::spawn(Vec::<String>::new(), Duration::from_secs(1), "/k", |_| {})
                .unwrap_err(),
            EtcdError::NoEndpoint
        );
    }

    #[test]
    fn endpoints_are_normalized_to_the_plaintext_form_pd_is_dialed_with() {
        let client = EtcdClient::connect(["127.0.0.1:2379"], Duration::from_millis(50)).unwrap();
        assert_eq!(client.endpoints(), ["http://127.0.0.1:2379".to_owned()]);
    }

    #[test]
    fn a_put_to_an_unreachable_endpoint_fails_without_hanging() {
        // Port 1 has no listener; the call must come back as Unreachable
        // rather than block the DDL path that is best-effort calling it.
        let client = EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(200)).unwrap();
        assert!(matches!(
            client.put_global_schema_version(9),
            Err(EtcdError::Unreachable { .. })
        ));
    }

    #[test]
    fn one_key_operations_honor_the_call_site_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let (stop_sender, stop_receiver) = mpsc::channel();
        let blocker = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            let _ = stop_receiver.recv_timeout(Duration::from_secs(3));
        });
        let client = EtcdClient::connect([endpoint], Duration::from_secs(5)).unwrap();
        let started = std::time::Instant::now();
        assert!(client
            .get_with_timeout(b"/serverstate-timeout", Duration::from_millis(100))
            .is_err());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the call-site timeout must override the client's five-second timeout"
        );
        let _ = stop_sender.send(());
        drop(client);
        blocker.join().unwrap();
    }

    #[test]
    fn lease_session_worker_stops_with_its_context() {
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let mut session = EtcdLeaseSession::spawn(
            ["127.0.0.1:1"],
            Duration::from_secs(5),
            Arc::new(ClusterSecurity::plaintext()),
            1,
            90,
            move || worker_cancelled.load(Ordering::Acquire),
        )
        .unwrap();
        cancelled.store(true, Ordering::Release);
        let started = std::time::Instant::now();
        session.worker.take().unwrap().join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn a_watcher_on_an_unreachable_endpoint_stops_promptly() {
        let mut watcher = EtcdWatcher::spawn(
            ["127.0.0.1:1"],
            Duration::from_millis(100),
            DDL_GLOBAL_SCHEMA_VERSION_KEY,
            |_| unreachable!("no event can arrive from a closed port"),
        )
        .unwrap();
        let stopping = std::time::Instant::now();
        watcher.shutdown();
        assert!(stopping.elapsed() < Duration::from_secs(5));
        assert_eq!(watcher.stats().events, 0);
    }

    /// End-to-end against a real PD, opt in with
    /// `TIDB_ETCD_PROBE_PD=127.0.0.1:2379 cargo test -p tidb-pd-client -- --ignored`.
    ///
    /// Nothing else in this file can prove the watch stream actually works:
    /// PD's embedded etcd is the only implementation of the contract, and a
    /// projection that compiles is not a projection that is understood.
    #[test]
    #[ignore = "requires a live PD; set TIDB_ETCD_PROBE_PD"]
    fn a_put_wakes_a_watch_on_a_real_pd() {
        let Ok(endpoint) = std::env::var("TIDB_ETCD_PROBE_PD") else {
            panic!("set TIDB_ETCD_PROBE_PD to a PD client address");
        };
        let timeout = Duration::from_secs(5);
        let key = "/tidb/ddl/global_schema_version";
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut watcher = EtcdWatcher::spawn([endpoint.as_str()], timeout, key, move |event| {
            let _ = sender.send(event.clone());
        })
        .unwrap();
        // The stream is created asynchronously; a PUT that races its creation
        // would be missed by etcd itself, not by this code.
        std::thread::sleep(Duration::from_millis(500));

        let client = EtcdClient::connect([endpoint.as_str()], timeout).unwrap();
        client.put_global_schema_version(4242).unwrap();
        let event = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("the watch must deliver the PUT");
        assert_eq!(event.value, b"4242");
        assert!(!event.deleted);
        assert_eq!(client.global_schema_version().unwrap(), Some(4242));
        assert!(watcher.stats().streams >= 1);
        watcher.shutdown();
    }

    #[test]
    fn a_cloned_client_keeps_the_worker_alive_until_the_last_handle_drops() {
        let client = EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(100)).unwrap();
        let clone = client.clone();
        drop(client);
        // The worker is still there: this answers with a transport failure,
        // not `Closed`.
        assert!(matches!(
            clone.get(b"/k"),
            Err(EtcdError::Unreachable { .. })
        ));
    }
}
