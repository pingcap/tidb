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

//! Transaction opener over the vendored client-rust engine.
//!
//! This is the Rust counterpart of Go's `pkg/store/driver.tikvStore` begin
//! family: one long-lived object per store that holds the PD client and hands
//! out transactions. It replaces this crate's own
//! `RealOptimisticTransactionOpener` at the store construction sites — the
//! coordinator behind that opener is what the vendored engine now supplies.
//!
//! The PD client is a type parameter, so the same opener serves a live
//! cluster and an in-process store, the way Go's driver serves both TiKV and
//! `mockstore` through one `kv.Storage`.

use std::sync::Arc;

use tikv_client::pd::PdClient;
use tikv_client::transaction::Transaction;
use tikv_client::{Timestamp, TimestampExt, TransactionOptions};

use crate::driver::tikv_transaction::{TikvTransactionDriver, TikvTransactionError};

/// The store an opener hands transactions out of.
///
/// The vendored engine constructs production transactions through its
/// `TransactionClient` and injected-client transactions through a test-gated
/// constructor — the same split Go draws between `tikv.NewKVStore` and
/// `tikv.NewTestTiKVStore`. This trait is the seam over both, so the opener,
/// the driver, and everything above them are written once.
pub trait TikvTransactionSource {
    /// The PD client the produced transactions route through.
    type PdC: PdClient;

    /// Spends one timestamp from this store's oracle.
    fn current_timestamp(&self) -> Result<Timestamp, TikvTransactionError>;

    /// The PD cluster identity, as Go's store reports it.
    fn cluster_id(&self) -> Result<u64, TikvTransactionError>;

    /// Opens one transaction at the supplied timestamp.
    fn begin(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Result<Transaction<Self::PdC>, TikvTransactionError>;
}

/// Commit-protocol selection for the transactions an opener hands out.
///
/// These are the session variables `@@tidb_enable_async_commit` and
/// `@@tidb_enable_1pc`, resolved once per store exactly as the coordinator
/// facade resolved them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TikvCommitProtocol {
    /// Commit at the completed prewrite using `max(min_commit_ts)` instead of
    /// a second PD round trip.
    pub async_commit: bool,
    /// Let TiKV commit a single-region transaction inside the prewrite.
    pub one_pc: bool,
}

impl TikvCommitProtocol {
    /// The protocol set of a transaction that must use normal two-phase commit.
    #[must_use]
    pub const fn two_phase_only() -> Self {
        Self {
            async_commit: false,
            one_pc: false,
        }
    }

    fn apply(self, mut options: TransactionOptions) -> TransactionOptions {
        if self.async_commit {
            options = options.use_async_commit();
        }
        if self.one_pc {
            options = options.try_one_pc();
        }
        options
    }
}

/// Opens transactions over one client-rust store.
pub struct TikvTransactionOpener<S: TikvTransactionSource> {
    source: S,
    runtime: Arc<tokio::runtime::Runtime>,
    commit_protocol: TikvCommitProtocol,
    size_limits: Option<(u64, u64)>,
}

impl<S: TikvTransactionSource> TikvTransactionOpener<S> {
    /// Builds an opener over one store and the runtime its blocking
    /// transactions will use.
    #[must_use]
    pub fn new(source: S, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self {
            source,
            runtime,
            commit_protocol: TikvCommitProtocol::two_phase_only(),
            size_limits: None,
        }
    }

    /// Fixes the commit protocol for every transaction from this opener.
    #[must_use]
    pub fn with_commit_protocol(mut self, protocol: TikvCommitProtocol) -> Self {
        self.commit_protocol = protocol;
        self
    }

    /// The store this opener hands transactions out of.
    #[must_use]
    pub const fn source(&self) -> &S {
        &self.source
    }

    /// The runtime the opener's transactions block on.
    #[must_use]
    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.runtime
    }

    /// Spends one PD timestamp, as Go's oracle does for a new transaction.
    pub fn current_timestamp(&self) -> Result<u64, TikvTransactionError> {
        Ok(self.source.current_timestamp()?.version())
    }

    /// Opens a normal optimistic transaction at a fresh PD timestamp.
    pub fn begin(&self) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        let timestamp = self.source.current_timestamp()?;
        self.open(timestamp, TransactionOptions::new_optimistic())
    }

    /// Opens an optimistic transaction at a timestamp the caller has ALREADY
    /// spent — the one its own read is at — spending none of its own. This is
    /// what makes an implicit single-statement transaction a single PD round
    /// trip, exactly as the coordinator facade documented.
    pub fn begin_at(
        &self,
        start_ts: u64,
    ) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        self.open(
            Timestamp::from_version(start_ts),
            TransactionOptions::new_optimistic(),
        )
    }

    /// Opens a pessimistic transaction at a fresh PD timestamp.
    pub fn begin_pessimistic(&self) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        let timestamp = self.source.current_timestamp()?;
        self.open(timestamp, TransactionOptions::new_pessimistic())
    }

    /// Opens a read-only transaction at a fresh PD timestamp. Read-only
    /// transactions never prewrite, so they carry no commit protocol.
    pub fn begin_read_only(&self) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        let timestamp = self.source.current_timestamp()?;
        self.open_read_only(timestamp)
    }

    /// Opens a read-only transaction at an already-spent timestamp.
    pub fn begin_read_only_at(
        &self,
        start_ts: u64,
    ) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        self.open_read_only(Timestamp::from_version(start_ts))
    }

    fn open_read_only(
        &self,
        timestamp: Timestamp,
    ) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        Ok(TikvTransactionDriver::new_read_only(
            self.source
                .begin(timestamp, TransactionOptions::new_optimistic().read_only())?,
            self.runtime.clone(),
        ))
    }

    fn open(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        self.open_raw(timestamp, self.commit_protocol.apply(options))
    }

    fn open_raw(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Result<TikvTransactionDriver<S::PdC>, TikvTransactionError> {
        let mut driver = TikvTransactionDriver::new(
            self.source.begin(timestamp, options)?,
            self.runtime.clone(),
        );
        if let Some((entry_limit, buffer_limit)) = self.size_limits {
            driver.set_size_limits(entry_limit, buffer_limit);
        }
        Ok(driver)
    }
}

/// Production store: the vendored engine's own `TransactionClient` over a live
/// PD cluster, the counterpart of Go `tikv.NewKVStore`.
pub struct TikvClusterSource {
    client: tikv_client::TransactionClient,
    runtime: Arc<tokio::runtime::Runtime>,
    cluster_id: u64,
}

impl TikvClusterSource {
    /// Wraps one connected transaction client.
    #[must_use]
    /// The cluster identity is supplied by the caller: the engine exposes it
    /// on its store handle rather than on the transaction client, and the
    /// caller that connected the store already holds it.
    pub fn new(
        client: tikv_client::TransactionClient,
        runtime: Arc<tokio::runtime::Runtime>,
        cluster_id: u64,
    ) -> Self {
        Self {
            client,
            runtime,
            cluster_id,
        }
    }

    /// The wrapped client, for store-wide operations (GC, safe points, lock
    /// cleanup) that are not per-transaction.
    #[must_use]
    pub const fn client(&self) -> &tikv_client::TransactionClient {
        &self.client
    }
}

impl TikvTransactionSource for TikvClusterSource {
    type PdC = tikv_client::pd::PdRpcClient;

    fn current_timestamp(&self) -> Result<Timestamp, TikvTransactionError> {
        Ok(self.runtime.block_on(self.client.current_timestamp())?)
    }

    fn cluster_id(&self) -> Result<u64, TikvTransactionError> {
        Ok(self.cluster_id)
    }

    fn begin(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Result<Transaction<Self::PdC>, TikvTransactionError> {
        Ok(self.runtime.block_on(
            self.client
                .begin_with_options(options.start_timestamp(timestamp)),
        )?)
    }
}

/// In-process store: transactions over an injected PD client, the counterpart
/// of Go `tikv.NewTestTiKVStore` over `mockstore`.
///
/// The vendored engine gates injected-client construction behind its
/// `internal-tests` feature, so this source is gated the same way. An embedded
/// TiDB node needs this path in an ordinary build, which is why the feature is
/// re-exported here rather than being test-only: an ungated in-process store
/// constructor upstream would let this gate go away.
#[cfg(feature = "tikv-inprocess")]
pub struct TikvInProcessSource<PdC: PdClient> {
    pd: Arc<PdC>,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[cfg(feature = "tikv-inprocess")]
impl<PdC: PdClient> TikvInProcessSource<PdC> {
    /// Wraps one in-process PD client.
    #[must_use]
    pub fn new(pd: Arc<PdC>, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { pd, runtime }
    }

    /// The wrapped PD client.
    #[must_use]
    pub const fn pd(&self) -> &Arc<PdC> {
        &self.pd
    }
}

#[cfg(feature = "tikv-inprocess")]
impl<PdC: PdClient> TikvTransactionSource for TikvInProcessSource<PdC> {
    type PdC = PdC;

    fn current_timestamp(&self) -> Result<Timestamp, TikvTransactionError> {
        let pd = self.pd.clone();
        Ok(self.runtime.block_on(pd.get_timestamp())?)
    }

    fn cluster_id(&self) -> Result<u64, TikvTransactionError> {
        // An in-process store has no PD cluster identity of its own.
        Ok(0)
    }

    fn begin(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Result<Transaction<Self::PdC>, TikvTransactionError> {
        Ok(Transaction::new(
            timestamp,
            self.pd.clone(),
            options,
            tikv_client::request::Keyspace::Disable,
        ))
    }
}

/// Store-identity and size-limit surface the previous coordinator facade
/// exposed, kept so consumers that read it do not have to change shape.
impl<S: TikvTransactionSource> TikvTransactionOpener<S> {
    /// The PD cluster this opener writes to, as PD itself names it.
    pub fn cluster_id(&self) -> Result<u64, TikvTransactionError> {
        self.source.cluster_id()
    }

    /// Bounds one transaction's staged buffer, as TiDB's
    /// `kv.TxnEntrySizeLimit` / `kv.TxnTotalSizeLimit` do.
    ///
    /// The previous facade took these as planning hints on `begin`; client-go
    /// has no such parameter, so they are applied to the engine's own buffer
    /// limits instead, which is where the source enforces them.
    #[must_use]
    pub const fn with_size_limits(mut self, entry_limit: u64, buffer_limit: u64) -> Self {
        self.size_limits = Some((entry_limit, buffer_limit));
        self
    }
}
