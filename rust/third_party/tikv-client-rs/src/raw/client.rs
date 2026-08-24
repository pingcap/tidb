// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use core::ops::Range;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use crate::async_util::Cancellation;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::common::Error;
use crate::config::{Config, RawApiVersion};
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::kvrpcpb::{RawScanRequest, RawScanResponse};
use crate::proto::metapb;
use crate::raw::lowering::*;
use crate::request::CollectSingle;
use crate::request::Dispatch;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::KvRequest;
use crate::request::NoTarget;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::request::TruncateKeyspace;
use crate::request::{build_keyspace_name, keyspace_from_pd_meta, Keyspace};
use crate::request::{plan, Collect};
use crate::retry::{RetryBackoffer, BO_REGION_MISS};
use crate::store::{HasRegionError, RegionStore};
use crate::Backoff;
use crate::BoundRange;
use crate::ColumnFamily;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::Value;

const MAX_RAW_KV_SCAN_LIMIT: u32 = 10240;
/// `rawkv.rawkvMaxBackoff`: every client-go RawKV request owns a fresh,
/// cumulative 20-second retry budget.
const RAWKV_MAX_BACKOFF_MS: u64 = 20_000;
/// client-go's `internal/client.MaxWriteExecutionTime`: `ReadTimeoutShort`
/// (30 seconds) minus the 10-second post-proposal allowance.
const RAW_MAX_WRITE_EXECUTION_DURATION: Duration = Duration::from_secs(20);

/// The TiKV raw `Client` is used to interact with TiKV using raw requests.
///
/// Raw requests don't need a wrapping transaction.
/// Each request is immediately processed once executed.
///
/// The returned results of raw request methods are [`Future`](std::future::Future)s that must be
/// awaited to execute.
pub struct Client<PdC: PdClient = PdRpcClient> {
    rpc: Arc<PdC>,
    cluster_id: u64,
    cf: Option<ColumnFamily>,
    backoff: Backoff,
    /// Whether to use the [`atomic mode`](Client::with_atomic_for_cas).
    atomic: bool,
    keyspace: Keyspace,
    /// The canonical name returned by PD for an API V2 keyspace. Client-go
    /// places this metadata in every request context it encodes.
    keyspace_name: Option<String>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            cluster_id: self.cluster_id,
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
        }
    }
}

impl Client<PdRpcClient> {
    /// Create a raw [`Client`] and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::RawClient;
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// # });
    /// ```
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_config(pd_endpoints, Config::default()).await
    }

    /// Create a raw [`Client`] with a custom configuration, and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient};
    /// # use futures::prelude::*;
    /// # use std::time::Duration;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new_with_config(
    ///     vec!["192.168.0.100"],
    ///     Config::default().with_timeout(Duration::from_secs(60)),
    /// )
    /// .await
    /// .unwrap();
    /// # });
    /// ```
    pub async fn new_with_config<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Self> {
        // Client-go's raw constructor uses the unwrapped V1 codec for both
        // V1 and V1TTL, and resolves a keyspace only for API V2. Preserve the
        // existing Rust convenience that `with_keyspace` selects V2 while the
        // default raw API version is V1.
        let keyspace_name = match config.raw_api_version {
            RawApiVersion::V1Ttl => None,
            RawApiVersion::V2 => Some(build_keyspace_name(
                config.keyspace.as_deref().unwrap_or_default(),
            )),
            RawApiVersion::V1 => config.keyspace.as_deref().map(build_keyspace_name),
        };
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let rpc = match &keyspace_name {
            Some(name) => {
                PdRpcClient::connect_with_keyspace(
                    &pd_endpoints,
                    config.clone(),
                    KeyMode::Raw,
                    name.clone(),
                )
                .await?
            }
            None => PdRpcClient::connect(&pd_endpoints, config.clone(), false).await?,
        };
        let rpc = Arc::new(rpc);
        let cluster_id = rpc.cluster_id().await;
        let (keyspace, keyspace_name) = match keyspace_name {
            Some(_) => {
                let meta = rpc
                    .keyspace_meta()
                    .expect("V2 PD client retains the metadata used to build its codec");
                (keyspace_from_pd_meta(meta)?, Some(meta.name.clone()))
            }
            None => match config.raw_api_version {
                RawApiVersion::V1 => (Keyspace::Disable, None),
                RawApiVersion::V1Ttl => (Keyspace::V1Ttl, None),
                RawApiVersion::V2 => unreachable!("API V2 always resolves a keyspace"),
            },
        };
        Ok(Client {
            rpc,
            cluster_id,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace,
            keyspace_name,
        })
    }
}

impl<PdC: PdClient> Client<PdC> {
    /// Return the PD cluster ID associated with this client.
    ///
    /// This is retained during construction, matching client-go's
    /// `RawKVClient.ClusterID` behavior without requiring an RPC per call.
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Return the PD client used for region routing and timestamp operations.
    ///
    /// This is the shared-ownership Rust equivalent of client-go's
    /// `RawKVClient.GetPDClient()`.
    pub fn pd_client(&self) -> Arc<PdC> {
        self.rpc.clone()
    }

    /// Close this raw client and release its transport resources.
    ///
    /// Rust clients use shared ownership for PD and TiKV connections, so close
    /// consumes this handle. The final owner drops the channel clients and the
    /// PD timestamp-request sender, ending its background stream. Other
    /// independently cloned [`Client`] handles remain usable until closed or
    /// dropped themselves.
    pub fn close(self) -> Result<()> {
        drop(self);
        Ok(())
    }

    /// Create a new client which is a clone of `self`, but which uses an explicit column family for
    /// all requests.
    ///
    /// This function returns a new `Client`; requests created with the new client will use the
    /// supplied column family. The original `Client` can still be used (without the new
    /// column family).
    ///
    /// By default, raw clients use the `Default` column family.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient, ColumnFamily};
    /// # use futures::prelude::*;
    /// # use std::convert::TryInto;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new(vec!["192.168.0.100"])
    ///     .await
    ///     .unwrap()
    ///     .with_cf(ColumnFamily::Write);
    /// // Fetch a value at "foo" from the Write CF.
    /// let get_request = client.get("foo".to_owned());
    /// # });
    /// ```
    #[must_use]
    pub fn with_cf(&self, cf: ColumnFamily) -> Self {
        self.with_cf_option(Some(cf))
    }

    /// Create a client using a raw column-family name for subsequent requests.
    ///
    /// An empty name clears the explicit selection and restores TiKV's default
    /// column family. Unlike client-go's mutating `SetColumnFamily(string)`,
    /// this builder leaves the original client unchanged. Use
    /// [`set_column_family`](Client::set_column_family) for source-compatible
    /// in-place behavior.
    #[must_use]
    pub fn with_cf_name(&self, cf: impl AsRef<str>) -> Self {
        self.with_cf_option(Self::column_family_from_name(cf.as_ref()))
    }

    /// Set the raw column family for subsequent requests on this client.
    ///
    /// This is the mutating, chainable equivalent of client-go's
    /// `RawKVClient.SetColumnFamily(string)`. An empty name restores TiKV's
    /// default column family; other names, including server-defined custom
    /// names, are sent unchanged.
    pub fn set_column_family(&mut self, cf: impl AsRef<str>) -> &mut Self {
        self.cf = Self::column_family_from_name(cf.as_ref());
        self
    }

    fn column_family_from_name(cf: &str) -> Option<ColumnFamily> {
        match cf {
            "" => None,
            "default" => Some(ColumnFamily::Default),
            "lock" => Some(ColumnFamily::Lock),
            "write" => Some(ColumnFamily::Write),
            custom => Some(ColumnFamily::Custom(custom.to_owned())),
        }
    }

    fn with_cf_option(&self, cf: Option<ColumnFamily>) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cluster_id: self.cluster_id,
            cf,
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
        }
    }

    /// Legacy compatibility builder for the pre-source-parity retry API.
    ///
    /// Client-go RawKV always creates a fresh cumulative 20-second backoffer
    /// for each operation, so this attempt-count strategy is no longer used
    /// by RawKV dispatch. It is retained temporarily to avoid an abrupt API
    /// removal; callers should remove it rather than rely on a configuration
    /// that client-go does not have.
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient, ColumnFamily};
    /// # use futures::prelude::*;
    /// # use std::convert::TryInto;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// // Fetch a value at "foo" from the Write CF.
    /// let get_request = client.get("foo".to_owned());
    /// # });
    /// ```
    #[deprecated(
        note = "client-go RawKV uses a fixed cumulative 20-second retry budget; this legacy attempt-count strategy is ignored"
    )]
    #[must_use]
    pub fn with_backoff(&self, backoff: Backoff) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cluster_id: self.cluster_id,
            cf: self.cf.clone(),
            backoff,
            atomic: self.atomic,
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
        }
    }

    /// Set to use the atomic mode.
    ///
    /// The only reason of using atomic mode is the
    /// [`compare_and_swap`](Client::compare_and_swap) operation. To guarantee
    /// the atomicity of CAS, write operations like [`put`](Client::put) or
    /// [`delete`](Client::delete) in atomic mode are more expensive. Some
    /// operations are not supported in the mode.
    #[must_use]
    pub fn with_atomic_for_cas(&self) -> Self {
        self.with_atomic_for_cas_enabled(true)
    }

    /// Enable or disable atomic mode for subsequent raw write and CAS requests.
    ///
    /// Unlike client-go's mutating `SetAtomicForCAS(bool)`, this derives a
    /// client with the requested setting. Use
    /// [`set_atomic_for_cas`](Client::set_atomic_for_cas) for source-compatible
    /// in-place behavior.
    #[must_use]
    pub fn with_atomic_for_cas_enabled(&self, enabled: bool) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cluster_id: self.cluster_id,
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: enabled,
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
        }
    }

    /// Enable or disable atomic mode for subsequent requests on this client.
    ///
    /// This is the mutating, chainable equivalent of client-go's
    /// `RawKVClient.SetAtomicForCAS(bool)`. Disabling atomic mode restores
    /// normal raw write behavior and makes compare-and-swap unavailable.
    pub fn set_atomic_for_cas(&mut self, enabled: bool) -> &mut Self {
        self.atomic = enabled;
        self
    }
}

impl<PdC: PdClient> Client<PdC> {
    /// Build a raw-request plan with the same API context as client-go's
    /// keyspace codec. Centralizing this prevents a newly added raw operation
    /// from accidentally omitting the resolved keyspace name.
    fn plan<Req: KvRequest>(&self, request: Req) -> PlanBuilder<PdC, Dispatch<Req>, NoTarget> {
        PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .keyspace_name_option(self.keyspace_name.as_deref())
    }

    fn retry_backoffer(&self) -> RetryBackoffer {
        RetryBackoffer::new(Cancellation::default(), RAWKV_MAX_BACKOFF_MS)
    }

    /// Build a raw request carrying client-go's server-side execution budget.
    /// The context is attached before plan cloning, so it survives every
    /// shard and retry.
    fn max_execution_plan<Req: KvRequest>(
        &self,
        request: Req,
    ) -> PlanBuilder<PdC, Dispatch<Req>, NoTarget> {
        self.plan(request)
            .max_execution_duration(RAW_MAX_WRITE_EXECUTION_DURATION)
    }

    /// Create a new 'get' request.
    ///
    /// Once resolved this request will result in the fetching of the value associated with the
    /// given key.
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let req = client.get(key);
    /// let result: Option<Value> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        debug!("invoking raw get request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = new_raw_get_request(key, self.cf.clone());
        let plan = self
            .plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(CollectSingle)
            .extract_error()
            .post_process_default()
            .plan();
        plan.execute().await
    }

    /// Create a new 'batch get' request.
    ///
    /// Once resolved this request will result in the fetching of the values associated with the
    /// given keys.
    ///
    /// Returns one value per input key, in input order.
    ///
    /// A missing key is represented by `None`; a present key with an empty
    /// value is `Some(Value::default())`. This mirrors client-go's distinction
    /// between a nil missing value and an empty stored value.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient, Value};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let req = client.batch_get(keys);
    /// let result: Vec<Option<Value>> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<Option<Value>>> {
        debug!("invoking raw batch_get request");
        let keys = keys.into_iter().map(Into::into).collect::<Vec<Key>>();
        let request_keys = keys
            .iter()
            .cloned()
            .map(|key| key.encode_keyspace(self.keyspace, KeyMode::Raw));
        let request = new_raw_batch_get_request(request_keys, self.cf.clone());
        let plan = self
            .max_execution_plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(Collect)
            .plan();
        let values = plan
            .execute()
            .await?
            .into_iter()
            .map(|pair: KvPair| (pair.key().clone(), pair.value().clone()))
            .collect::<HashMap<_, _>>();
        Ok(keys
            .into_iter()
            .map(|key| values.get(&key).cloned())
            .collect())
    }

    /// Fetch existing raw key/value pairs without preserving the input order.
    ///
    /// This is the previous Rust-native batch-get shape. Prefer
    /// [`batch_get`](Client::batch_get) when client-go-compatible positional
    /// results are required.
    pub async fn batch_get_pairs(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking raw batch_get_pairs request");
        let keys = keys
            .into_iter()
            .map(|key| key.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let request = new_raw_batch_get_request(keys, self.cf.clone());
        let plan = self
            .max_execution_plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(Collect)
            .plan();
        plan.execute().await
    }

    /// Create a new 'get key ttl' request.
    ///
    /// Once resolved this request will result in the fetching of the alive time left for the
    /// given key.
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// # use tikv_client::{Value, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let req = client.get_key_ttl_secs(key);
    /// let result: Option<Value> = req.await.unwrap();
    /// # });
    pub async fn get_key_ttl_secs(&self, key: impl Into<Key>) -> Result<Option<u64>> {
        debug!("invoking raw get_key_ttl_secs request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = new_raw_get_key_ttl_request(key, self.cf.clone());
        let plan = self
            .plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(CollectSingle)
            .extract_error()
            .post_process_default()
            .plan();
        plan.execute().await
    }

    /// Create a new 'put' request.
    ///
    /// Once resolved this request will result in the setting of the value associated with the given key.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let val = "TiKV".to_owned();
    /// let req = client.put(key, val);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.put_with_ttl(key, value, 0).await
    }

    pub async fn put_with_ttl(
        &self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl_secs: u64,
    ) -> Result<()> {
        debug!("invoking raw put request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request =
            new_raw_put_request(key, value.into(), self.cf.clone(), ttl_secs, self.atomic);
        let plan = self
            .plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(CollectSingle)
            .extract_error()
            .plan();
        plan.execute().await?;
        Ok(())
    }

    /// Create a new 'batch put' request.
    ///
    /// Once resolved this request will result in the setting of the values associated with the given keys.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Result, KvPair, Key, Value, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let kvpair1 = ("PD".to_owned(), "Go".to_owned());
    /// let kvpair2 = ("TiKV".to_owned(), "Rust".to_owned());
    /// let iterable = vec![kvpair1, kvpair2];
    /// let req = client.batch_put(iterable);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn batch_put(
        &self,
        pairs: impl IntoIterator<Item = impl Into<KvPair>>,
    ) -> Result<()> {
        let pairs = pairs
            .into_iter()
            .map(|pair| pair.into().encode_keyspace(self.keyspace, KeyMode::Raw))
            .collect::<Vec<_>>();
        let ttls = vec![0; pairs.len()];
        self.batch_put_encoded(pairs, ttls).await
    }

    /// Store key/value pairs with per-pair time-to-live values.
    ///
    /// Supplying no TTL values uses zero for every pair, as client-go's
    /// `BatchPutWithTTL` does. Any non-empty TTL list must have exactly one
    /// value per pair.
    pub async fn batch_put_with_ttl(
        &self,
        pairs: impl IntoIterator<Item = impl Into<KvPair>>,
        ttls: impl IntoIterator<Item = u64>,
    ) -> Result<()> {
        let pairs = pairs
            .into_iter()
            .map(|pair| pair.into().encode_keyspace(self.keyspace, KeyMode::Raw))
            .collect::<Vec<_>>();
        let ttls = ttls.into_iter().collect::<Vec<_>>();
        let ttls = if ttls.is_empty() {
            vec![0; pairs.len()]
        } else if ttls.len() == pairs.len() {
            ttls
        } else {
            return Err(Error::StringError(
                "the len of ttls is not equal to the len of values".to_owned(),
            ));
        };
        self.batch_put_encoded(pairs, ttls).await
    }

    async fn batch_put_encoded(&self, pairs: Vec<KvPair>, ttls: Vec<u64>) -> Result<()> {
        debug!("invoking raw batch_put request");
        let request = new_raw_batch_put_request(
            pairs.into_iter(),
            ttls.into_iter(),
            self.cf.clone(),
            self.atomic,
        );
        let plan = self
            .max_execution_plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .extract_error()
            .plan();
        plan.execute().await?;
        Ok(())
    }

    /// Create a new 'delete' request.
    ///
    /// Once resolved this request will result in the deletion of the given key.
    ///
    /// It does not return an error if the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let req = client.delete(key);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn delete(&self, key: impl Into<Key>) -> Result<()> {
        debug!("invoking raw delete request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = new_raw_delete_request(key, self.cf.clone(), self.atomic);
        let plan = self
            .max_execution_plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(CollectSingle)
            .extract_error()
            .plan();
        plan.execute().await?;
        Ok(())
    }

    /// Create a new 'batch delete' request.
    ///
    /// Once resolved this request will result in the deletion of the given keys.
    ///
    /// It does not return an error if some of the keys do not exist and will delete the others.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let req = client.batch_delete(keys);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn batch_delete(&self, keys: impl IntoIterator<Item = impl Into<Key>>) -> Result<()> {
        debug!("invoking raw batch_delete request");
        let keys = keys
            .into_iter()
            .map(|k| k.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let request = new_raw_batch_delete_request(keys, self.cf.clone(), self.atomic);
        let plan = self
            .max_execution_plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .extract_error()
            .plan();
        plan.execute().await?;
        Ok(())
    }

    /// Create a new 'delete range' request.
    ///
    /// Once resolved this request will result in the deletion of all keys lying in the given range.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.delete_range(inclusive_range.into_owned());
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn delete_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        debug!("invoking raw delete_range request");
        let range = range.into();
        let (start_key, end_key) = range.clone().into_keys();
        // Client-go's `for startKey < endKey` loop performs no routing or RPC
        // for an empty bounded half-open range. An inclusive Rust range has
        // already been converted to `end + 0`, so it remains non-empty here.
        if end_key
            .as_ref()
            .is_some_and(|end_key| &start_key >= end_key)
        {
            return Ok(());
        }
        let range = range.encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = new_raw_delete_range_request(range, self.cf.clone());
        let plan = self
            .max_execution_plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .extract_error()
            .plan();
        plan.execute().await?;
        Ok(())
    }

    /// Checksum every raw key/value pair in `range`.
    ///
    /// The result uses TiKV's CRC64-XOR reduction and aggregates every region
    /// intersecting the range. An unbounded end scans to the end of the current
    /// keyspace.
    pub async fn checksum(&self, range: impl Into<BoundRange>) -> Result<crate::RawChecksum> {
        debug!("invoking raw checksum request");
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = new_raw_checksum_request(range);
        self.plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(Collect)
            .plan()
            .execute()
            .await
    }

    /// Create a new 'scan' request.
    ///
    /// Once resolved this request will result in a `Vec` of key-value pairs that lies in the specified range.
    ///
    /// If the number of eligible key-value pairs are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{KvPair, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan(inclusive_range.into_owned(), 2);
    /// let result: Vec<KvPair> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan(&self, range: impl Into<BoundRange>, limit: u32) -> Result<Vec<KvPair>> {
        debug!("invoking raw scan request");
        self.scan_inner(range.into(), limit, false, false).await
    }

    /// Create a new 'scan' request but scans in "reverse" direction.
    ///
    /// Once resolved this request will result in a `Vec` of key-value pairs that lies in the specified range.
    ///
    /// If the number of eligible key-value pairs are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// Reverse Scan queries continuous kv pairs in range [startKey, endKey),
    /// from startKey(lowerBound) to endKey(upperBound) in reverse order, up to limit pairs.
    /// The returned keys are in reversed lexicographical order.
    /// If you want to include the endKey or exclude the startKey, push a '\0' to the key.
    /// An unbounded upper endpoint is unsupported because client-go cannot
    /// locate the last region; it returns an empty successful result.
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{KvPair, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan_reverse(inclusive_range.into_owned(), 2);
    /// let result: Vec<KvPair> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan_reverse(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking raw reverse scan request");
        self.scan_inner(range.into(), limit, false, true).await
    }

    /// Create a new 'scan' request that only returns the keys.
    ///
    /// Once resolved this request will result in a `Vec` of keys that lies in the specified range.
    ///
    /// If the number of eligible keys are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan_keys(inclusive_range.into_owned(), 2);
    /// let result: Vec<Key> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan_keys(&self, range: impl Into<BoundRange>, limit: u32) -> Result<Vec<Key>> {
        debug!("invoking raw scan_keys request");
        Ok(self
            .scan_inner(range, limit, true, false)
            .await?
            .into_iter()
            .map(KvPair::into_key)
            .collect())
    }

    /// Create a new 'scan' request that only returns the keys in reverse order.
    ///
    /// Once resolved this request will result in a `Vec` of keys that lies in the specified range.
    ///
    /// If the number of eligible keys are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan_keys(inclusive_range.into_owned(), 2);
    /// let result: Vec<Key> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan_keys_reverse(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<Vec<Key>> {
        debug!("invoking raw scan_keys_reverse request");
        Ok(self
            .scan_inner(range, limit, true, true)
            .await?
            .into_iter()
            .map(KvPair::into_key)
            .collect())
    }

    /// Create a new 'batch scan' request.
    ///
    /// Once resolved this request will result in a set of scanners over the given keys.
    ///
    /// **Warning**: This method is experimental. The `each_limit` parameter does not work as expected.
    /// It does not limit the number of results returned of each range,
    /// instead it limits the number of results in each region of each range.
    /// As a result, you may get **more than** `each_limit` key-value pairs for each range.
    /// But you should not miss any entries.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range1 = "TiDB"..="TiKV";
    /// let inclusive_range2 = "TiKV"..="TiSpark";
    /// let iterable = vec![inclusive_range1.into_owned(), inclusive_range2.into_owned()];
    /// let req = client.batch_scan(iterable, 2);
    /// let result = req.await;
    /// # });
    /// ```
    pub async fn batch_scan(
        &self,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        each_limit: u32,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking raw batch_scan request");
        self.batch_scan_inner(ranges, each_limit, false).await
    }

    /// Create a new 'batch scan' request that only returns the keys.
    ///
    /// Once resolved this request will result in a set of scanners over the given keys.
    ///
    /// **Warning**: This method is experimental.
    /// The `each_limit` parameter does not limit the number of results returned of each range,
    /// instead it limits the number of results in each region of each range.
    /// As a result, you may get **more than** `each_limit` key-value pairs for each range,
    /// but you should not miss any entries.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range1 = "TiDB"..="TiKV";
    /// let inclusive_range2 = "TiKV"..="TiSpark";
    /// let iterable = vec![inclusive_range1.into_owned(), inclusive_range2.into_owned()];
    /// let req = client.batch_scan(iterable, 2);
    /// let result = req.await;
    /// # });
    /// ```
    pub async fn batch_scan_keys(
        &self,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        each_limit: u32,
    ) -> Result<Vec<Key>> {
        debug!("invoking raw batch_scan_keys request");
        Ok(self
            .batch_scan_inner(ranges, each_limit, true)
            .await?
            .into_iter()
            .map(KvPair::into_key)
            .collect())
    }

    /// Create a new *atomic* 'compare and set' request.
    ///
    /// Once resolved this request will result in an atomic `compare and set'
    /// operation for the given key.
    ///
    /// If the value retrived is equal to `current_value`, `new_value` is
    /// written.
    ///
    /// # Return Value
    ///
    /// A tuple is returned if successful: the previous value and whether the
    /// value is swapped
    pub async fn compare_and_swap(
        &self,
        key: impl Into<Key>,
        previous_value: impl Into<Option<Value>>,
        new_value: impl Into<Value>,
    ) -> Result<(Option<Value>, bool)> {
        debug!("invoking raw compare_and_swap request");
        self.assert_atomic()?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let req = new_cas_request(
            key,
            new_value.into(),
            previous_value.into(),
            self.cf.clone(),
        );
        let plan = self
            .max_execution_plan(req)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(CollectSingle)
            .extract_error()
            .post_process_default()
            .plan();
        plan.execute().await
    }

    pub async fn coprocessor(
        &self,
        copr_name: impl Into<String>,
        copr_version_req: impl Into<String>,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        request_builder: impl Fn(metapb::Region, Vec<Range<Key>>) -> Vec<u8> + Send + Sync + 'static,
    ) -> Result<Vec<(Vec<Range<Key>>, Vec<u8>)>> {
        let copr_version_req = copr_version_req.into();
        semver::VersionReq::from_str(&copr_version_req)?;
        let ranges = ranges
            .into_iter()
            .map(|range| range.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let keyspace = self.keyspace;
        let request_builder = move |region, ranges: Vec<Range<Key>>| {
            request_builder(
                region,
                ranges
                    .into_iter()
                    .map(|range| range.truncate_keyspace(keyspace))
                    .collect(),
            )
        };
        let req = new_raw_coprocessor_request(
            copr_name.into(),
            copr_version_req,
            ranges,
            request_builder,
        );
        let plan = self
            .plan(req)
            .preserve_shard()
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .post_process_default()
            .plan();
        Ok(plan
            .execute()
            .await?
            .into_iter()
            .map(|(ranges, data)| (ranges.truncate_keyspace(keyspace), data))
            .collect())
    }

    async fn scan_inner(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
        key_only: bool,
        reverse: bool,
    ) -> Result<Vec<KvPair>> {
        if limit > MAX_RAW_KV_SCAN_LIMIT {
            return Err(Error::MaxScanLimitExceeded {
                limit,
                max_limit: MAX_RAW_KV_SCAN_LIMIT,
            });
        }
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut result = Vec::new();
        let mut current_limit = limit;
        let (start_key, end_key) = range.clone().into_keys();
        let mut current_key = if reverse {
            // client-go's ReverseScan does not locate the last region. Its
            // `bytes.Compare(nil, lower) > 0` loop guard therefore returns an
            // empty successful result for an unbounded upper endpoint.
            let Some(end_key) = end_key.clone() else {
                return Ok(Vec::new());
            };
            end_key
        } else {
            start_key.clone()
        };

        while current_limit > 0 {
            if (!reverse && end_key.clone().is_some_and(|end| end <= current_key))
                || (reverse && current_key <= start_key)
            {
                break;
            }
            let (request_start_key, request_end_key) = if reverse {
                (start_key.clone(), Some(current_key.clone()))
            } else {
                (current_key.clone(), end_key.clone())
            };
            let scan_args = ScanInnerArgs {
                start_key: request_start_key,
                end_key: request_end_key,
                limit: current_limit,
                key_only,
                reverse,
                backoff: self.retry_backoffer(),
            };
            let (res, next_key) = self.retryable_scan(scan_args).await?;

            let mut kvs = res
                .map(|r| r.kvs.into_iter().map(Into::into).collect::<Vec<KvPair>>())
                .unwrap_or(Vec::new());

            if !kvs.is_empty() {
                current_limit -= kvs.len() as u32;
                result.append(&mut kvs);
            }
            if (!reverse && end_key.clone().is_some_and(|end| end <= next_key))
                || (reverse && next_key <= start_key)
            {
                break;
            } else {
                current_key = next_key;
            }
        }

        // limit is a soft limit, so we need check the number of results
        result.truncate(limit as usize);

        Ok(result)
    }

    async fn retryable_scan(
        &self,
        mut scan_args: ScanInnerArgs,
    ) -> Result<(Option<RawScanResponse>, Key)> {
        let start_key = scan_args.start_key;
        let end_key = scan_args.end_key;
        loop {
            let region = if scan_args.reverse {
                self.rpc
                    .clone()
                    .region_for_end_key(end_key.as_ref().expect("reverse scan upper bound"))
                    .await?
            } else {
                self.rpc.clone().region_for_key(&start_key).await?
            };
            let store = self.rpc.clone().store_for_id(region.id()).await?;
            let request = new_raw_scan_request(
                (start_key.clone(), end_key.clone()).into(),
                scan_args.limit,
                scan_args.key_only,
                scan_args.reverse,
                self.cf.clone(),
            );
            let resp = self.do_store_scan(store.clone(), request.clone()).await;
            return match resp {
                Ok(mut r) => {
                    if let Some(err) = r.region_error() {
                        let action = match plan::handle_region_error(
                            self.rpc.clone(),
                            err.clone(),
                            store.clone(),
                        )
                        .await
                        {
                            Ok(action) => action,
                            // RawKV's source outer `sendReq` loop receives a
                            // terminal sender region error, charges one
                            // region-miss backoff, then re-locates the key.
                            Err(Error::RegionError(_)) => {
                                match scan_args
                                    .backoff
                                    .backoff(BO_REGION_MISS, format!("raw region error: {err:?}"))
                                    .await
                                {
                                    Ok(()) => continue,
                                    Err(error) => {
                                        return Err(Error::StringError(error.to_string()))
                                    }
                                }
                            }
                            Err(error) => return Err(error),
                        };
                        let config = match action {
                            plan::RegionErrorRetry::Immediate => continue,
                            plan::RegionErrorRetry::Backoff(config) => config,
                            plan::RegionErrorRetry::TerminalAfterBackoff(config) => {
                                if let Err(error) = scan_args
                                    .backoff
                                    .backoff(config, format!("region error: {err:?}"))
                                    .await
                                {
                                    return Err(Error::StringError(error.to_string()));
                                }
                                BO_REGION_MISS
                            }
                        };
                        match scan_args
                            .backoff
                            .backoff(config, format!("region error: {err:?}"))
                            .await
                        {
                            Ok(()) => continue,
                            Err(error) => return Err(Error::StringError(error.to_string())),
                        }
                    }
                    let next_key = if scan_args.reverse {
                        region.start_key()
                    } else {
                        region.end_key()
                    };
                    Ok((Some(r), next_key))
                }
                Err(err) => Err(err),
            };
        }
    }

    async fn do_store_scan(
        &self,
        store: RegionStore,
        scan_request: RawScanRequest,
    ) -> Result<RawScanResponse> {
        self.plan(scan_request)
            .single_region_with_store(store.clone())
            .await?
            .plan()
            .execute()
            .await
    }

    async fn batch_scan_inner(
        &self,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        each_limit: u32,
        key_only: bool,
    ) -> Result<Vec<KvPair>> {
        if each_limit > MAX_RAW_KV_SCAN_LIMIT {
            return Err(Error::MaxScanLimitExceeded {
                limit: each_limit,
                max_limit: MAX_RAW_KV_SCAN_LIMIT,
            });
        }

        let ranges = ranges
            .into_iter()
            .map(|range| range.into().encode_keyspace(self.keyspace, KeyMode::Raw));

        let request = new_raw_batch_scan_request(ranges, each_limit, key_only, self.cf.clone());
        let plan = self
            .plan(request)
            .retry_multi_region_with_retry_backoffer(self.retry_backoffer())
            .merge(Collect)
            .plan();
        plan.execute().await
    }

    fn assert_atomic(&self) -> Result<()> {
        if self.atomic {
            Ok(())
        } else {
            Err(Error::UnsupportedMode)
        }
    }
}

#[derive(Clone)]
struct ScanInnerArgs {
    start_key: Key,
    end_key: Option<Key>,
    limit: u32,
    key_only: bool,
    reverse: bool,
    backoff: RetryBackoffer,
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::Result;

    #[test]
    fn close_releases_the_raw_client_pd_handle() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            rpc: pd_client.clone(),
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        assert_eq!(Arc::strong_count(&pd_client), 2);
        client.close().unwrap();
        assert_eq!(Arc::strong_count(&pd_client), 1);
    }

    #[tokio::test]
    async fn raw_get_retries_a_region_miss_with_its_cumulative_source_budget() -> Result<()> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = attempts.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::RawGetRequest>());
                if observed_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            region_not_found: Some(Default::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else {
                    Ok(Box::new(kvrpcpb::RawGetResponse {
                        value: b"value".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>)
                }
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        assert_eq!(client.get(b"key".to_vec()).await?, Some(b"value".to_vec()));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn raw_scan_retries_a_terminal_region_error_in_its_outer_loop() -> Result<()> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = attempts.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::RawScanRequest>());
                if observed_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(Box::new(kvrpcpb::RawScanResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            region_not_found: Some(Default::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else {
                    Ok(Box::new(kvrpcpb::RawScanResponse {
                        kvs: vec![kvrpcpb::KvPair {
                            key: b"key".to_vec(),
                            value: b"value".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>)
                }
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        assert_eq!(
            client.scan(b"key".to_vec()..b"keyz".to_vec(), 1).await?,
            vec![KvPair(b"key".to_vec().into(), b"value".to_vec())]
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn v1ttl_raw_requests_keep_v1_keys_and_context_version() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawGetRequest>()
                    .expect("raw get request");
                let context = req.context.as_ref().expect("request context");
                assert_eq!(req.key, b"key");
                assert_eq!(context.api_version, kvrpcpb::ApiVersion::V1 as i32);
                assert_eq!(context.keyspace_id, 0);
                assert!(context.keyspace_name.is_empty());
                Ok(Box::new(kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::V1Ttl,
            keyspace_name: None,
        };

        assert_eq!(client.get(b"key".to_vec()).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn v1ttl_batch_put_keeps_the_v1_context_version() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawBatchPutRequest>()
                    .expect("raw batch put request");
                let context = req.context.as_ref().expect("request context");
                assert_eq!(context.api_version, kvrpcpb::ApiVersion::V1 as i32);
                assert_eq!(context.keyspace_id, 0);
                assert!(context.keyspace_name.is_empty());
                Ok(Box::new(kvrpcpb::RawBatchPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::V1Ttl,
            keyspace_name: None,
        };

        client
            .batch_put(vec![KvPair(b"key".to_vec().into(), b"value".to_vec())])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn raw_execution_duration_matches_the_client_go_command_matrix() -> Result<()> {
        let durations = Arc::new(Mutex::new(Vec::new()));
        let captured_durations = durations.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let (command, duration, response): (&str, u64, Box<dyn Any>) =
                    if let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() {
                        (
                            "get",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawGetResponse {
                                not_found: true,
                                ..Default::default()
                            }),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawGetKeyTtlRequest>() {
                        (
                            "get_ttl",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawGetKeyTtlResponse {
                                not_found: true,
                                ..Default::default()
                            }),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawPutRequest>() {
                        (
                            "put",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawPutResponse::default()),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawBatchGetRequest>() {
                        (
                            "batch_get",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawBatchGetResponse::default()),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawBatchPutRequest>() {
                        (
                            "batch_put",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawBatchPutResponse::default()),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawDeleteRequest>() {
                        (
                            "delete",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawDeleteResponse::default()),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawBatchDeleteRequest>() {
                        (
                            "batch_delete",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawBatchDeleteResponse::default()),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawDeleteRangeRequest>() {
                        (
                            "delete_range",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawDeleteRangeResponse::default()),
                        )
                    } else if let Some(req) = req.downcast_ref::<kvrpcpb::RawCasRequest>() {
                        (
                            "cas",
                            req.context.as_ref().unwrap().max_execution_duration_ms,
                            Box::new(kvrpcpb::RawCasResponse::default()),
                        )
                    } else {
                        unreachable!("unexpected raw request")
                    };
                captured_durations.lock().unwrap().push((command, duration));
                Ok(response)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: true,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        client.get(b"get".to_vec()).await?;
        client.get_key_ttl_secs(b"ttl".to_vec()).await?;
        client.put(b"put".to_vec(), b"value".to_vec()).await?;
        client.batch_get_pairs(vec![b"batch-get".to_vec()]).await?;
        client
            .batch_put(vec![KvPair(
                b"batch-put".to_vec().into(),
                b"value".to_vec(),
            )])
            .await?;
        client.delete(b"delete".to_vec()).await?;
        client.batch_delete(vec![b"batch-delete".to_vec()]).await?;
        client
            .delete_range(b"delete-range-a".to_vec()..b"delete-range-z".to_vec())
            .await?;
        client
            .compare_and_swap(b"cas".to_vec(), None, b"value".to_vec())
            .await?;

        let mut durations = durations.lock().unwrap().clone();
        durations.sort_unstable();
        assert_eq!(
            durations,
            vec![
                ("batch_delete", 20_000),
                ("batch_get", 20_000),
                ("batch_put", 20_000),
                ("cas", 20_000),
                ("delete", 20_000),
                ("delete_range", 20_000),
                ("get", 0),
                ("get_ttl", 0),
                ("put", 0),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_batch_put_with_ttl() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawBatchPutRequest>() {
                    let context = req.context.as_ref().unwrap();
                    assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                    assert_eq!(context.keyspace_id, 0);
                    assert_eq!(context.keyspace_name, "tenant");
                    assert_eq!(req.cf, "tenant_cf");
                    assert_eq!(req.ttl, 7);
                    assert_eq!(req.ttls, vec![7, 11]);
                    let resp = kvrpcpb::RawBatchPutResponse {
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else {
                    unreachable!()
                }
            },
        )));
        let client = Client {
            rpc: pd_client.clone(),
            cluster_id: 0,
            cf: Some(ColumnFamily::try_from("tenant_cf").unwrap()),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Enable { keyspace_id: 0 },
            keyspace_name: Some("tenant".to_owned()),
        };
        let pairs = vec![
            KvPair(vec![11].into(), vec![12]),
            KvPair(vec![11].into(), vec![12]),
        ];
        let ttls = vec![7, 11];
        assert!(client.batch_put_with_ttl(pairs, ttls).await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn atomic_mode_can_be_disabled_after_being_enabled() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawPutRequest>()
                    .expect("raw put request");
                assert!(!req.for_cas);
                Ok(Box::new(kvrpcpb::RawPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client.clone(),
            cluster_id: 41,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        let normal_client = client
            .with_atomic_for_cas()
            .with_atomic_for_cas_enabled(false);
        assert_eq!(normal_client.cluster_id(), 41);
        assert!(Arc::ptr_eq(&normal_client.pd_client(), &pd_client));
        normal_client
            .put(b"key".to_vec(), b"value".to_vec())
            .await?;
        assert!(matches!(
            normal_client
                .compare_and_swap(b"key".to_vec(), None, b"value".to_vec())
                .await,
            Err(Error::UnsupportedMode)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn batch_put_with_ttl_validates_lengths_and_defaults_empty_ttls() -> Result<()> {
        let dispatches = Arc::new(Mutex::new(0));
        let captured_dispatches = dispatches.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawBatchPutRequest>()
                    .expect("raw batch put request");
                *captured_dispatches.lock().unwrap() += 1;
                assert_eq!(req.ttls, vec![0]);
                Ok(Box::new(kvrpcpb::RawBatchPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        for ttls in [vec![1], vec![1, 2, 3]] {
            assert!(matches!(
                client
                    .batch_put_with_ttl(
                        vec![KvPair(b"key-a".to_vec().into(), b"value-a".to_vec()), KvPair(b"key-b".to_vec().into(), b"value-b".to_vec())],
                        ttls,
                    )
                    .await,
                Err(Error::StringError(message)) if message == "the len of ttls is not equal to the len of values"
            ));
        }
        assert_eq!(*dispatches.lock().unwrap(), 0);

        client
            .batch_put_with_ttl(
                vec![KvPair(b"key".to_vec().into(), b"value".to_vec())],
                Vec::new(),
            )
            .await?;
        assert_eq!(*dispatches.lock().unwrap(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn atomic_mode_does_not_block_batch_delete_or_delete_range() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = requests.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawBatchDeleteRequest>() {
                    assert_eq!(req.keys, vec![b"key".to_vec()]);
                    assert!(req.for_cas);
                    captured_requests.lock().unwrap().push("batch_delete");
                    return Ok(Box::new(kvrpcpb::RawBatchDeleteResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawDeleteRangeRequest>() {
                    assert_eq!(req.start_key, b"a");
                    assert_eq!(req.end_key, b"z");
                    captured_requests.lock().unwrap().push("delete_range");
                    return Ok(Box::new(kvrpcpb::RawDeleteRangeResponse::default()) as Box<dyn Any>);
                }
                unreachable!("unexpected raw request")
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: true,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        client.batch_delete(vec![b"key".to_vec()]).await?;
        client.delete_range(b"a".to_vec()..b"z".to_vec()).await?;
        assert_eq!(*requests.lock().unwrap(), ["batch_delete", "delete_range"]);
        Ok(())
    }

    #[tokio::test]
    async fn unbounded_delete_range_covers_every_mock_region() -> Result<()> {
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let captured_ranges = ranges.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawDeleteRangeRequest>()
                    .expect("raw delete range request");
                captured_ranges
                    .lock()
                    .unwrap()
                    .push((req.start_key.clone(), req.end_key.clone()));
                Ok(Box::new(kvrpcpb::RawDeleteRangeResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        client.delete_range(..).await?;
        let mut ranges = ranges.lock().unwrap().clone();
        ranges.sort();
        assert_eq!(
            ranges,
            vec![
                (Vec::new(), vec![10]),
                (vec![10], vec![250, 250]),
                (vec![250, 250], Vec::new()),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_delete_range_returns_without_routing_or_dispatch() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_req: &dyn Any| -> Result<Box<dyn Any>> {
                panic!("client-go does not dispatch an empty raw delete range")
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        client
            .delete_range(b"same".to_vec()..b"same".to_vec())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn column_family_name_supports_custom_names_and_default_reset() -> Result<()> {
        let column_families = Arc::new(Mutex::new(Vec::new()));
        let captured_column_families = column_families.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawPutRequest>()
                    .expect("raw put request");
                captured_column_families
                    .lock()
                    .unwrap()
                    .push(req.cf.clone());
                Ok(Box::new(kvrpcpb::RawPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        client
            .with_cf_name("tenant_cf")
            .put(b"custom".to_vec(), b"value".to_vec())
            .await?;
        client
            .with_cf_name("tenant_cf")
            .with_cf_name("")
            .put(b"default".to_vec(), b"value".to_vec())
            .await?;
        assert_eq!(*column_families.lock().unwrap(), ["tenant_cf", ""]);
        Ok(())
    }

    #[tokio::test]
    async fn source_mutating_setters_update_subsequent_requests() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = requests.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawPutRequest>()
                    .expect("raw put request");
                captured_requests
                    .lock()
                    .unwrap()
                    .push((req.cf.clone(), req.for_cas));
                Ok(Box::new(kvrpcpb::RawPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let mut client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        client
            .set_column_family("tenant_cf")
            .set_atomic_for_cas(true)
            .put(b"atomic".to_vec(), b"value".to_vec())
            .await?;
        client
            .set_column_family("")
            .set_atomic_for_cas(false)
            .put(b"normal".to_vec(), b"value".to_vec())
            .await?;

        assert_eq!(
            *requests.lock().unwrap(),
            [("tenant_cf".to_owned(), true), (String::new(), false)]
        );
        assert!(matches!(
            client
                .compare_and_swap(b"normal".to_vec(), None, b"value".to_vec())
                .await,
            Err(Error::UnsupportedMode)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn raw_point_reads_and_cas_propagate_server_string_errors() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::RawGetRequest>() {
                    return Ok(Box::new(kvrpcpb::RawGetResponse {
                        error: "get failed".to_owned(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::RawGetKeyTtlRequest>() {
                    return Ok(Box::new(kvrpcpb::RawGetKeyTtlResponse {
                        error: "ttl failed".to_owned(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::RawCasRequest>() {
                    return Ok(Box::new(kvrpcpb::RawCasResponse {
                        error: "cas failed".to_owned(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                unreachable!("unexpected raw request")
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: true,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        for (error, operation) in [("get failed", 0), ("ttl failed", 1), ("cas failed", 2)] {
            let result = match operation {
                0 => client.get(b"key".to_vec()).await.map(|_| ()),
                1 => client.get_key_ttl_secs(b"key".to_vec()).await.map(|_| ()),
                2 => client
                    .compare_and_swap(b"key".to_vec(), None, b"value".to_vec())
                    .await
                    .map(|_| ()),
                _ => unreachable!(),
            };
            assert!(matches!(
                result,
                Err(Error::MultipleKeyErrors(errors))
                    if matches!(errors.as_slice(), [Error::KvError { message }] if message == error)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn api_v2_scan_returns_dispatch_decoded_keys_without_high_level_truncation() -> Result<()>
    {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawScanRequest>()
                    .expect("raw scan request");
                let context = req.context.as_ref().unwrap();
                assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                assert_eq!(context.keyspace_id, 7);
                assert_eq!(context.keyspace_name, "tenant");
                assert_eq!(req.start_key, b"r\0\0\x07\x01");
                assert_eq!(req.end_key, b"r\0\0\x07\x02");
                Ok(Box::new(kvrpcpb::RawScanResponse {
                    kvs: vec![kvrpcpb::KvPair {
                        key: req.start_key.clone(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::try_enable(7)?,
            keyspace_name: Some("tenant".to_owned()),
        };

        assert_eq!(
            client.scan(vec![1]..vec![2], 1).await?,
            vec![KvPair(vec![1].into(), b"value".to_vec())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_batch_get_preserves_input_order_missing_empty_and_duplicate_values() -> Result<()>
    {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawBatchGetRequest>()
                    .expect("raw batch get request");
                let context = req.context.as_ref().unwrap();
                assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                assert_eq!(context.keyspace_id, 7);
                assert_eq!(context.keyspace_name, "tenant");
                assert_eq!(
                    req.keys,
                    vec![
                        b"r\0\0\x07a".to_vec(),
                        b"r\0\0\x07b".to_vec(),
                        b"r\0\0\x07b".to_vec(),
                        b"r\0\0\x07missing".to_vec(),
                    ]
                );
                Ok(Box::new(kvrpcpb::RawBatchGetResponse {
                    pairs: vec![
                        kvrpcpb::KvPair {
                            key: b"r\0\0\x07b".to_vec(),
                            value: Vec::new(),
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: b"r\0\0\x07a".to_vec(),
                            value: b"value-a".to_vec(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::try_enable(7)?,
            keyspace_name: Some("tenant".to_owned()),
        };

        assert_eq!(
            client
                .batch_get(vec![
                    b"b".to_vec(),
                    b"missing".to_vec(),
                    b"a".to_vec(),
                    b"b".to_vec()
                ])
                .await?,
            vec![
                Some(Vec::new()),
                None,
                Some(b"value-a".to_vec()),
                Some(Vec::new()),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_reverse_scan_routes_from_upper_bounds_and_walks_region_starts() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = requests.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::RawScanRequest>()
                    .expect("raw scan request");
                assert!(request.reverse);
                captured_requests
                    .lock()
                    .unwrap()
                    .push((request.start_key.clone(), request.end_key.clone()));
                let keys = match (request.start_key.as_slice(), request.end_key.as_slice()) {
                    ([20], [5]) => vec![19, 10],
                    ([10], [5]) => vec![9, 5],
                    bounds => panic!("unexpected reverse scan bounds: {bounds:?}"),
                };
                Ok(Box::new(kvrpcpb::RawScanResponse {
                    kvs: keys
                        .into_iter()
                        .map(|key| kvrpcpb::KvPair {
                            key: vec![key],
                            value: vec![key],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        assert_eq!(
            client.scan_reverse(vec![5]..vec![20], 4).await?,
            vec![19, 10, 9, 5]
                .into_iter()
                .map(|key| KvPair(vec![key].into(), vec![key]))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            *requests.lock().unwrap(),
            vec![(vec![20], vec![5]), (vec![10], vec![5])]
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_reverse_scan_with_unbounded_upper_endpoint_is_empty() -> Result<()> {
        let client = Client {
            rpc: Arc::new(MockPdClient::default()),
            cluster_id: 0,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            keyspace_name: None,
        };

        assert!(client.scan_reverse(vec![5].., 10).await?.is_empty());
        assert!(client.scan_reverse(.., 10).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_raw_coprocessor() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawCoprocessorRequest>() {
                    assert_eq!(req.copr_name, "example");
                    assert_eq!(req.copr_version_req, "0.1.0");
                    let context = req.context.as_ref().expect("request context");
                    assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                    assert_eq!(context.keyspace_id, 0);
                    assert_eq!(context.keyspace_name, "tenant");
                    let resp = kvrpcpb::RawCoprocessorResponse {
                        data: req.data.clone(),
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else {
                    unreachable!()
                }
            },
        )));
        let client = Client {
            rpc: pd_client,
            cluster_id: 0,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Enable { keyspace_id: 0 },
            keyspace_name: Some("tenant".to_owned()),
        };
        let resps = client
            .coprocessor(
                "example",
                "0.1.0",
                vec![vec![5]..vec![15], vec![20]..vec![]],
                |region, ranges| format!("{:?}:{:?}", region.id, ranges).into_bytes(),
            )
            .await?;
        let resps: Vec<_> = resps
            .into_iter()
            .map(|(ranges, data)| (ranges, String::from_utf8(data).unwrap()))
            .collect();
        assert_eq!(
            resps,
            vec![(
                vec![
                    Key::from(vec![5])..Key::from(vec![15]),
                    Key::from(vec![20])..Key::from(vec![])
                ],
                "2:[Key(05)..Key(0F), Key(14)..Key()]".to_string(),
            ),]
        );
        Ok(())
    }
}
