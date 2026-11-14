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

//! Keeping a node's loaded catalog current while it serves queries.
//!
//! Go source of truth: `pkg/domain/domain.go`'s lease-driven reload goroutine
//! and `pkg/infoschema/issyncer`'s `Syncer.Reload`. Go ticks at `lease/2` so
//! that a node is never more than one lease behind; the same cadence is used
//! here, with the interval left configurable because this tier has no DDL
//! owner telling it what the lease is.
//!
//! # Consistency
//!
//! The published catalog is one `Arc`, replaced whole. A reader takes one
//! `Arc` clone at the start of a statement and holds it for that statement's
//! lifetime, so a statement is planned and executed against exactly one schema
//! version even if a reload lands mid-flight; the reload's own snapshot read
//! never observes a partially applied catalog either (see
//! [`crate::catalog_reload`]).
//!
//! Go pairs the reload loop with a *schema validator* holding a lease: a
//! transaction that started at schema version `v` and commits after a
//! conflicting DDL, or after the node lost contact with the schema for longer
//! than the lease, is refused with `ErrInfoSchemaChanged` (8028) or
//! `ErrInfoSchemaExpired` (8027) rather than committing against a stale
//! plan. The validator is [`tidb_infoschema_isvalidator`]; the node's reload
//! pass renews its lease on every pass and records each diff's changed
//! tables, and the transaction tier asks it at commit
//! (`tidb_txnkv::transaction::SchemaLeaseChecker`).
//!
//! # Watch-triggered reload
//!
//! Go does not rely on the lease tick alone: after a DDL owner commits, it
//! also PUTs the new version to etcd so every follower's watch fires
//! immediately, instead of waiting up to `lease/2`. This tier does the same.
//! Source of truth:
//!
//! * `pkg/ddl/util/util.go`: `DDLGlobalSchemaVersion =
//!   "/tidb/ddl/global_schema_version"` is the etcd key. The value is the
//!   decimal ASCII text of the `int64` version
//!   (`strconv.FormatInt(version, 10)`; see `pkg/ddl/schemaver/syncer.go`
//!   `OwnerUpdateGlobalVersion`, a plain `etcdCli.Put` with no lease
//!   attached to that key).
//! * `pkg/infoschema/issyncer/syncer.go` `Syncer.SyncLoop` selects on
//!   `syncer.GlobalVersionCh()` (an etcd `Watch` on that same key) *and* the
//!   `lease/2` ticker; either one triggers `s.Reload()`. A closed channel is
//!   logged as "schema syncer need rewatch" and re-watched, with the ticker
//!   still reloading meanwhile.
//!
//! The shape here is the same, with the select replaced by a condvar because
//! the reload thread is a plain thread and the watch lives on its own thread:
//! [`Syncer::waker`] hands out a [`CatalogReloadWaker`], the etcd
//! watch thread calls [`CatalogReloadWaker::nudge`] on every event, and the
//! reload thread wakes early and runs exactly the pass its tick would have
//! run. Nothing about *which* version was published is read from the event:
//! Go ignores the value too and simply reloads, so a spurious or duplicate
//! event costs one comparison against `SchemaVersionKey` and nothing more.
//! The tick is never disabled -- it is what keeps a node correct while the
//! watch is disconnected, which is the same division of labour Go has.
//!
//! Reachability was checked, not assumed: TiDB's etcd client dials PD's own
//! client port (`pkg/store/etcd.go` `NewEtcdCli` takes `store.EtcdAddrs()`,
//! which are PD addresses) -- PD embeds a real etcd server and exposes its
//! full v3 KV/Watch gRPC surface on that same port, which is why
//! `tidb-pd-client`'s `EtcdClient`/`EtcdWatcher` need no endpoint of their
//! own. The message shapes are a dependency-closed projection of the
//! vendored `go.etcd.io/etcd/api/v3@v3.5.15` (`etcdserverpb/rpc.proto`,
//! `mvccpb/kv.proto`) in `tidb-proto`.
//!
//! Out of scope, deliberately: the privilege reloader stays tick-only. Go
//! notifies privilege changes over a *different* key entirely --
//! `pkg/domain/domain.go`'s `notifyUpdatePrivilege` PUTs a JSON
//! `PrivilegeEvent` to `privilegeKey = "/tidb/privilege"`, and
//! `LoadPrivilegeLoop` watches that key, not this one -- so driving the
//! privilege reloader from the schema-version key would be a guess.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_infoschema::SessionExtendedInfoSchema;

/// What builds a loaded version's tables (Go's loader runs
/// `infoschema.NewBuilder(...).InitWithDBInfos`, or `InitWithOldInfoSchema`
/// then `ApplyDiff`, before inserting the result into `InfoCache`): from the
/// loaded schema image (its `DBInfo`s, policies, resource groups and
/// version) and the image in force before it, the image with every table
/// built, a schema whose `DBInfo` the previous image already built taken
/// from it. The domain installs it, as only the domain holds the auto-id
/// allocators the tables are built over.
pub type SchemaBuilder = Box<
    dyn Fn(&SessionExtendedInfoSchema, Option<&SessionExtendedInfoSchema>) -> SessionExtendedInfoSchema
        + Send
        + Sync,
>;

/// Go `infoschema.InfoCache`: each schema version's built `InfoSchema`, the
/// latest published, replaced whole by the reload thread.
///
/// A reader's [`get_latest`](InfoCache::get_latest) hands back an owned `Arc`, so a
/// statement in flight keeps the exact schema it started with and a publish
/// never has to wait for readers.
pub struct InfoCache {
    /// Go `Loader.crossKS`: historical reads use the same isolated loader.
    cross_ks: bool,
    published: RwLock<Arc<SessionExtendedInfoSchema>>,
    historical: Mutex<tidb_infoschema::cache::InfoCache>,
    capacity: AtomicUsize,
    gc_store: RwLock<Option<Arc<dyn tidb_txnkv::kv_storage::Storage>>>,
    gc_check: Mutex<(i64, std::time::Instant)>,
    /// Go `Data.recentMinTS`.
    pub data: Arc<tidb_infoschema::infoschema_v2::Data>,
    /// Go `Syncer`'s `s.m`: one reload at a time, held across the load, the
    /// schema validator's update and the publication, so two reload paths
    /// never interleave their validator updates.
    reload: Mutex<()>,
    /// The installed [`SchemaBuilder`]; until the domain installs one, a
    /// version is held as loaded, without table objects.
    builder: RwLock<Option<SchemaBuilder>>,
    /// Go Loader: serialize cache misses across worker, snapshot and reload paths.
    loading: Mutex<()>,
    row_ids: RwLock<Option<Arc<dyn tidb_table::table_from_meta::TableAutoIds>>>,
}

impl std::fmt::Debug for InfoCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InfoCache")
            .field("latest_version", &self.get_latest().schema_meta_version())
            .finish_non_exhaustive()
    }
}

impl InfoCache {
    /// Publishes an initial catalog, normally the node's startup full load.
    #[must_use]
    pub fn new(mut catalog: SessionExtendedInfoSchema) -> Self {
        catalog.set_uses_infoschema_v2(tidb_vardef::SCHEMA_CACHE_SIZE.load(Ordering::SeqCst) > 0);
        let data = catalog.v2().map_or_else(|| Arc::new(tidb_infoschema::infoschema_v2::Data::new()), |v2| v2.data.clone());
        let built = Arc::new(catalog);
        let mut historical = tidb_infoschema::cache::InfoCache::new(16);
        historical.insert(built.clone(), built.schema_commit_ts());
        Self {
            cross_ks: false,
            published: RwLock::new(Arc::clone(&built)),
            historical: Mutex::new(historical),
            capacity: AtomicUsize::new(16),
            gc_store: RwLock::new(None),
            gc_check: Mutex::new((built.schema_meta_version(), std::time::Instant::now())),
            data,
            reload: Mutex::new(()),
            builder: RwLock::new(None),
            loading: Mutex::new(()),
            row_ids: RwLock::new(None),
        }
    }

    /// Go `NewLoaderForCrossKS`: keep both current and snapshot loads scoped.
    pub fn new_for_cross_ks(catalog: SessionExtendedInfoSchema) -> Self {
        let mut cache = Self::new(catalog);
        cache.cross_ks = true;
        cache
    }

    /// Go `Loader.crossKS` / `Syncer.crossKS`.
    pub fn is_cross_ks(&self) -> bool { self.cross_ks }

    /// Go `NewCache(store)`: install the store used to compact V2 btrees.
    pub fn install_gc_store(&self, store: Arc<dyn tidb_txnkv::kv_storage::Storage>) {
        *self.gc_store.write().unwrap() = Some(store);
    }

    /// Go `InfoCache.Insert`'s 128-version / one-minute GC trigger.
    fn check_old_versions(&self, version: i64) {
        let mut check = self.gc_check.lock().unwrap();
        if check.0 == 0 { *check = (version, std::time::Instant::now()); return; }
        if version <= check.0.saturating_add(128) || check.1.elapsed() <= std::time::Duration::from_secs(60) { return; }
        *check = (version, std::time::Instant::now());
        let Some(store) = self.gc_store.read().unwrap().clone() else { return; };
        let data = self.data.clone();
        let _ = std::thread::Builder::new().name("infoschema-gc".into()).spawn(move || {
            if let Ok(Some(oldest)) = store.oldest_schema_version() { data.gc_old_version(oldest); }
        });
    }

    /// Go `InfoCache.GetAndResetRecentInfoSchemaTS`: the min start ts for
    /// `InfoSyncer.ReportMinStartTS`, reset to the round's current TS.
    pub fn get_and_reset_recent_info_schema_ts(&self, now: u64) -> u64 {
        self.data.recent_min_ts.swap(now, Ordering::AcqRel)
    }

    /// Go Loader.LoadWithTS cache hit: refresh the V2 reader timestamp even
    /// when no DDL changed its schema version.
    pub fn refresh_snapshot_ts(&self, ts: u64) {
        let mut versions = self.historical.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut published = self.published.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !published.is_v2() { return; }
        let size = tidb_vardef::SCHEMA_CACHE_SIZE.load(Ordering::Acquire);
        if size > 0 && self.data.cache_capacity() != size { self.data.set_cache_capacity(size); }
        let mut schema = (**published).clone();
        schema.set_schema_ts(ts);
        let schema = Arc::new(schema);
        versions.insert(schema.clone(), schema.schema_commit_ts());
        *published = schema;
    }

    /// Go `InfoCache.GetBySnapshotTS`.
    pub fn get_by_snapshot_ts(&self, ts: u64) -> Option<Arc<SessionExtendedInfoSchema>> {
        self.historical.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_by_snapshot_ts(ts).map(|schema| {
            if schema.is_v2() {
                let mut reader = (*schema).clone();
                reader.set_schema_ts(ts);
                Arc::new(reader)
            } else { schema }
        })
    }

    /// Go `InfoCache.InsertEmptySchemaVersion`.
    pub fn insert_empty_schema_version(&self, version: i64) {
        self.historical.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert_empty_schema_version(version);
    }

    /// Go's loader building one version's `InfoSchema`.
    /// `previous` is an image this cache's builder built, whose schemas the
    /// new one may take over (an image held before the builder was installed
    /// has no tables to give).
    fn build(
        &self,
        loaded: SessionExtendedInfoSchema,
        previous: Option<&SessionExtendedInfoSchema>,
    ) -> Arc<SessionExtendedInfoSchema> {
        let builder = self
            .builder
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::new(match builder.as_ref() {
            Some(build) => build(&loaded, previous),
            None => loaded,
        })
    }

    /// Installs what builds each version's tables and rebuilds the versions
    /// this cache holds with it.
    pub fn install_schema_builder(&self, builder: SchemaBuilder) {
        *self
            .builder
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(builder);
        let mut versions = self
            .historical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for built in versions.values_mut() {
            *built = self.build(built.metadata_copy(), None);
        }
        let mut published = self
            .published
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let version = published.schema_meta_version();
        *published = match versions.get(&version) {
            Some(built) => Arc::clone(built),
            None => self.build(published.metadata_copy(), None),
        };
    }

    /// Serializes one reload (load, validator update, publication), as Go's
    /// `Syncer.Reload` holds `s.m`.
    pub fn lock_reload(&self) -> std::sync::MutexGuard<'_, ()> {
        self.reload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// How many versions this cache retains (Go `InfoCache`'s capacity).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Go `InfoCache.GetLatest`: the `InfoSchema` in force now. A poisoned
    /// lock still yields the value: a panicking publisher cannot leave the
    /// node unable to answer queries.
    #[must_use]
    pub fn get_latest(&self) -> Arc<SessionExtendedInfoSchema> {
        let schema = self.published.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if schema.is_v2() { Arc::new((**schema).clone()) } else { Arc::clone(&schema) }
    }

    /// Go `Loader.LoadWithTS(ts, isSnapshot)`: the schema at the snapshot's
    /// version, from this cache (`GetByVersion`) or loaded and inserted. Old
    /// reads never replace the latest publication; the newest versions are
    /// retained.
    pub fn load_with_ts<S: tidb_infoschema::cluster_catalog::MetaSnapshot>(
        &self,
        snapshot: &mut S,
        capacity: usize,
    ) -> Result<Arc<SessionExtendedInfoSchema>, tidb_infoschema::cluster_catalog::ClusterCatalogError>
    {
        let version = crate::catalog_reload::get_schema_version_with_non_empty_diff(snapshot)?;
        let start_ts = snapshot.start_ts();
        let mut schema_ts = if version > 0 { snapshot.schema_commit_ts(version).unwrap_or(0) } else { 0 };
        if schema_ts == 0 && version > 0 {
            let store = self.gc_store.read().unwrap().clone();
            if let Some(store) = store {
                schema_ts = store.mvcc_commit_ts(&tidb_meta::key::schema_diff_kv_key(version), start_ts).unwrap_or(0);
            }
        }
        let found = {
            let mut versions = self
                .historical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.capacity.store(capacity, Ordering::Relaxed);
            let latest = self.get_latest();
            let commit_ts = latest.schema_commit_ts();
            versions.insert(latest, commit_ts);
            // Go ReSize retains the newest entries before GetByVersion.
            versions.resize(capacity);
            versions.get_by_version(version)
        };
        if let Some(found) = found {
            let found = if found.is_v2() {
                let mut cloned = (*found).clone();
                cloned.set_schema_ts(start_ts);
                Arc::new(cloned)
            } else { found };
            self.historical.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(found.clone(), schema_ts);
            return Ok(found);
        }
        let _loading = self
            .loading
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(found) = self.cached_version(version) {
            return Ok(found);
        }
        // No shared cache lock spans storage RPCs.
        let latest = self.latest_cached();
        // Go `tryLoadSchemaDiffs`: a version newer than the latest one, fewer
        // than `LoadSchemaDiffVersionGapThreshold` diffs ahead, is the latest
        // advanced by those diffs; otherwise (a first load, an older version,
        // a diff that cannot apply) the whole schema is read.
        let mut loaded = if latest.schema_meta_version() != 0
            && version > latest.schema_meta_version()
        {
            let reloaded = if self.cross_ks {
                crate::catalog_reload::reload_cross_ks_catalog(snapshot, &latest)?
            } else {
                crate::catalog_reload::reload_cluster_catalog(
                    snapshot,
                    &latest,
                    self.row_ids
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_deref(),
                )?
            };
            match reloaded {
                crate::catalog_reload::ReloadedCatalog::Cached { catalog }
                | crate::catalog_reload::ReloadedCatalog::Diffs { catalog, .. }
                | crate::catalog_reload::ReloadedCatalog::Full { catalog, .. } => catalog,
                crate::catalog_reload::ReloadedCatalog::Unchanged { .. } => {
                    latest.try_metadata_copy()?
                }
            }
        } else {
            let mut loaded = if self.cross_ks {
                crate::cross_ks::load_info_schema(snapshot)?
            } else {
                crate::catalog_reload::load_info_schema_for_mode(snapshot, latest.is_v2())?
            };
            // The reserved counter may be ahead of the committed schema diff.
            // Use the same effective version as the live reload owner.
            loaded.set_schema_meta_version(version);
            // Go shouldUseV2: historical loads retain the current implementation.
            loaded.set_uses_infoschema_v2(latest.uses_infoschema_v2());
            loaded
        };
        // Go shouldUseV2 applies to diff fallbacks as well as direct full loads.
        loaded.set_uses_infoschema_v2(latest.uses_infoschema_v2());
        let built = self.build(loaded, Some(&latest));
        self.check_old_versions(built.schema_meta_version());
        let mut versions = self
            .historical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for &empty in built.empty_schema_versions() { versions.insert_empty_schema_version(empty); }
        let found = versions.get_by_version(version).unwrap_or_else(|| {
            versions.insert(built.clone(), schema_ts);
            built
        });
        // A concurrent request may have resized while storage was loading.
        versions.resize(self.capacity.load(Ordering::Relaxed));
        Ok(found)
    }

    /// Go `Loader`'s allocator requirement, shared by all callers of the loader.
    pub fn set_row_ids(&self, row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>) {
        *self
            .row_ids
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(row_ids);
    }

    /// Go `InfoCache.GetLatest` as the loader's diff base: a worker can
    /// cache a newer image before the domain publishes it to sessions.
    fn latest_cached(&self) -> Arc<SessionExtendedInfoSchema> {
        let latest = self
            .historical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_latest();
        latest.unwrap_or_else(|| self.get_latest())
    }

    /// Go `InfoCache.GetByVersion`.
    fn cached_version(&self, version: i64) -> Option<Arc<SessionExtendedInfoSchema>> {
        self.historical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&version)
            .cloned()
    }

    /// Go `Loader.LoadWithTS` as used by `Syncer.Reload`. A worker's prior
    /// load is reused, including its lazy cache and repair diversion; applying
    /// the diff a second time would record the repaired table twice.
    pub fn reload_with_ts<S: tidb_infoschema::cluster_catalog::MetaSnapshot>(
        &self,
        snapshot: &mut S,
        current: &SessionExtendedInfoSchema,
    ) -> Result<
        crate::catalog_reload::ReloadedCatalog,
        tidb_infoschema::cluster_catalog::ClusterCatalogError,
    > {
        self.reload_with_ts_at(snapshot, current, &|_| 0)
    }

    /// [`Self::reload_with_ts`] whose loaded image carries Go's
    /// `getTimestampForSchemaVersionWithNonEmptyDiff` before it is built and
    /// cached, as Go's `Loader.LoadWithTS` computes it before `Build`.
    pub fn reload_with_ts_at<S: tidb_infoschema::cluster_catalog::MetaSnapshot>(
        &self,
        snapshot: &mut S,
        current: &SessionExtendedInfoSchema,
        schema_commit_ts: &dyn Fn(i64) -> u64,
    ) -> Result<
        crate::catalog_reload::ReloadedCatalog,
        tidb_infoschema::cluster_catalog::ClusterCatalogError,
    > {
        use crate::catalog_reload::ReloadedCatalog;
        let version = crate::catalog_reload::get_schema_version_with_non_empty_diff(snapshot)?;
        let use_v2 = tidb_vardef::SCHEMA_CACHE_SIZE.load(Ordering::SeqCst) > 0;
        if current.uses_infoschema_v2() == use_v2 && version == current.schema_meta_version() {
            return Ok(ReloadedCatalog::Unchanged { version });
        }
        let _loading = self
            .loading
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = self.latest_cached();
        let changed = base.uses_infoschema_v2() != use_v2;
        if !changed {
            if let Some(found) = self
                .cached_version(version)
                .filter(|schema| schema.uses_infoschema_v2() == use_v2)
            {
                return Ok(ReloadedCatalog::Cached {
                    catalog: (*found).clone(),
                });
            }
        }
        let reloaded = if changed {
            let mut catalog = crate::catalog_reload::load_info_schema_for_mode(snapshot, use_v2)?;
            catalog.set_schema_meta_version(version);
            ReloadedCatalog::Full {
                catalog,
                reason: crate::catalog_reload::FullReloadReason::ImplementationChanged,
            }
        } else {
            crate::catalog_reload::reload_cluster_catalog(
                snapshot,
                &base,
                self.row_ids
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_deref(),
            )?
        };
        let mut reloaded = reloaded;
        if let ReloadedCatalog::Diffs { catalog, .. } | ReloadedCatalog::Full { catalog, .. } =
            &mut reloaded
        {
            if catalog.schema_meta_version() > 0 {
                catalog.set_schema_commit_ts(schema_commit_ts(catalog.schema_meta_version()));
            }
        }
        if let Some(catalog) = reloaded.catalog() {
            let built = self.build(catalog.clone(), Some(&base));
            let mut versions = self
                .historical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for &empty in built.empty_schema_versions() {
                versions.insert_empty_schema_version(empty);
            }
            versions.insert(Arc::clone(&built), built.schema_commit_ts());
            versions.resize(self.capacity.load(Ordering::Relaxed));
        }
        Ok(reloaded)
    }

    /// Go `InfoCache.Upsert`: admits `catalog` and publishes it unless a newer
    /// schema version is already published; returns whether it was published.
    ///
    /// Go's cache keeps schemas ordered by version and
    /// `GetLatest` answers the highest, so a reload that finishes late (a
    /// slow full load racing a DDL's inline reload) can never take the node
    /// back to an older schema. The older catalog still enters the
    /// historical cache, as Go's insert keeps it for snapshot reads.
    pub fn upsert(&self, catalog: SessionExtendedInfoSchema) -> bool {
        // Go Loader inserts the built image before Syncer publishes it. Keep
        // that image (including a lazy load already started by another reader).
        // An implementation switch replaces its same-version entry in the loader.
        let built = self
            .cached_version(catalog.schema_meta_version())
            .filter(|cached| cached.uses_infoschema_v2() == catalog.uses_infoschema_v2())
            .unwrap_or_else(|| self.build(catalog, Some(&self.get_latest())));
        let version = built.schema_meta_version();
        self.check_old_versions(version);
        // Publication and retained-version admission share the same lock order
        // as historical lookup, and holding the cache lock across the version
        // comparison serializes concurrent publishers.
        let mut versions = self
            .historical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for &empty in built.empty_schema_versions() { versions.insert_empty_schema_version(empty); }
        versions.insert(Arc::clone(&built), built.schema_commit_ts());
        versions.resize(self.capacity.load(Ordering::Relaxed));
        let mut guard = match self.published.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if version < guard.schema_meta_version() {
            return false;
        }
        if guard.is_v2() != built.is_v2() {
            versions.upsert(Arc::clone(&built), built.schema_commit_ts());
        } else {
            versions.insert(Arc::clone(&built), built.schema_commit_ts());
        }
        *guard = built;
        true
    }
}

/// Why a reload thread could not be started or stopped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogReloadError {
    /// A zero tick would spin the reload thread against PD without pause.
    ZeroInterval,
    /// The reload thread could not be created.
    Spawn(String),
    /// The reload thread panicked.
    WorkerPanicked,
}

impl std::fmt::Display for CatalogReloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("catalog reload interval must be nonzero"),
            Self::Spawn(message) => {
                write!(formatter, "failed to spawn catalog reloader: {message}")
            }
            Self::WorkerPanicked => formatter.write_str("catalog reloader panicked"),
        }
    }
}

impl std::error::Error for CatalogReloadError {}

/// Counts of what the reload thread has done, for tests and for operators.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogReloadStats {
    /// Passes that ran to a decision, successful or not.
    pub passes: u64,
    /// Passes that found the cluster still at the loaded version.
    pub unchanged: u64,
    /// Passes that published a catalog advanced by replaying diffs.
    pub diff_reloads: u64,
    /// Passes that published a freshly re-read catalog.
    pub full_reloads: u64,
    /// Passes whose read failed; the previous catalog stays published.
    pub failures: u64,
    /// Passes a watch event woke early, rather than the tick.
    pub nudged: u64,
}

#[derive(Debug, Default)]
struct StatCounters {
    passes: AtomicU64,
    unchanged: AtomicU64,
    diff_reloads: AtomicU64,
    full_reloads: AtomicU64,
    failures: AtomicU64,
    nudged: AtomicU64,
}

impl StatCounters {
    fn snapshot(&self) -> CatalogReloadStats {
        CatalogReloadStats {
            passes: self.passes.load(Ordering::Acquire),
            unchanged: self.unchanged.load(Ordering::Acquire),
            diff_reloads: self.diff_reloads.load(Ordering::Acquire),
            full_reloads: self.full_reloads.load(Ordering::Acquire),
            failures: self.failures.load(Ordering::Acquire),
            nudged: self.nudged.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Default)]
struct ReloadSignal {
    shutdown: bool,
    /// Set by a watch event, cleared by the pass it caused. One flag rather
    /// than a count on purpose: several events arriving during one pass are
    /// satisfied by the one following reload, exactly as Go's single-slot
    /// watch channel collapses them.
    nudged: bool,
}

/// A borrow-free reader of a reloader's [`CatalogReloadStats`].
pub type CatalogReloadStatsSource = Arc<dyn Fn() -> CatalogReloadStats + Send + Sync>;

/// One reload pass, as the caller performs it.
///
/// The closure is handed the currently published catalog and answers what to
/// publish. `Unchanged` and an error both leave the published catalog alone.
/// Keeping the storage access outside this module is what lets the thread be
/// tested without PD or TiKV.
pub type ReloadPass =
    Box<dyn FnMut(&SessionExtendedInfoSchema) -> Result<CatalogReloadPass, String> + Send + 'static>;

/// The outcome one reload pass reports back to the thread.
#[derive(Clone, Debug)]
pub enum CatalogReloadPass {
    /// Go Loader.LoadWithTS cache hit at a fresh timestamp.
    UnchangedAt(u64),
    /// The cluster is still at the published version.
    Unchanged,
    /// Publish this catalog; it was reached by replaying diffs.
    Diffs(SessionExtendedInfoSchema),
    /// Publish this catalog; it was re-read whole.
    Full(SessionExtendedInfoSchema),
}

/// Wakes a [`Syncer`] before its next tick.
///
/// This is the whole coupling between the etcd watch and the reload loop: the
/// watch thread owns no catalog and performs no read, it only says "something
/// changed", exactly like the value-ignoring receive in Go's `SyncLoop`.
/// Cloneable and inert once the reloader is gone, so a watch that outlives its
/// reloader by a moment cannot panic or block.
#[derive(Clone, Debug)]
pub struct CatalogReloadWaker {
    signal: Arc<(Mutex<ReloadSignal>, Condvar)>,
}

impl CatalogReloadWaker {
    /// Asks for one reload pass as soon as the thread can run it.
    pub fn nudge(&self) {
        let (lock, condvar) = &*self.signal;
        {
            let mut state = match lock.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.nudged = true;
        }
        condvar.notify_all();
    }
}

/// The running reload thread. Dropping it stops and joins the thread, so a
/// node cannot outlive its own reloader or leak it.
#[derive(Debug)]
pub struct Syncer {
    signal: Arc<(Mutex<ReloadSignal>, Condvar)>,
    stats: Arc<StatCounters>,
    worker: Option<JoinHandle<()>>,
}

impl Syncer {
    /// Starts the reload thread ticking every `interval`.
    ///
    /// Go ticks at `schemaLease/2`; a caller with a lease should pass half of
    /// it, which is why the interval is a parameter rather than a constant.
    pub fn spawn(
        catalog: Arc<InfoCache>,
        interval: Duration,
        mut pass: ReloadPass,
    ) -> Result<Self, CatalogReloadError> {
        if interval.is_zero() {
            return Err(CatalogReloadError::ZeroInterval);
        }
        let signal = Arc::new((Mutex::new(ReloadSignal::default()), Condvar::new()));
        let stats = Arc::new(StatCounters::default());
        let worker_signal = Arc::clone(&signal);
        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("catalog-reloader".to_owned())
            .spawn(move || {
                let (lock, condvar) = &*worker_signal;
                loop {
                    // Waiting on the condvar rather than sleeping is what makes
                    // shutdown prompt: a stop does not wait out the interval.
                    let mut state = match lock.lock() {
                        Ok(state) => state,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if !state.shutdown && !state.nudged {
                        state = match condvar.wait_timeout(state, interval) {
                            Ok((state, _)) => state,
                            Err(poisoned) => poisoned.into_inner().0,
                        };
                    }
                    let stopping = state.shutdown;
                    // Clearing before the pass, not after, is what makes an
                    // event that lands *during* a pass produce another one:
                    // that event may describe a version this pass's snapshot
                    // was taken too early to see.
                    let nudged = std::mem::take(&mut state.nudged);
                    drop(state);
                    if stopping {
                        return;
                    }
                    if nudged {
                        worker_stats.nudged.fetch_add(1, Ordering::AcqRel);
                    }
                    run_one_pass(&catalog, &worker_stats, &mut pass, nudged);
                }
            })
            .map_err(|error| CatalogReloadError::Spawn(error.to_string()))?;
        Ok(Self {
            signal,
            stats,
            worker: Some(worker),
        })
    }

    /// A handle the etcd watch uses to wake this thread before its tick.
    #[must_use]
    pub fn waker(&self) -> CatalogReloadWaker {
        CatalogReloadWaker {
            signal: Arc::clone(&self.signal),
        }
    }

    /// What the thread has done so far.
    #[must_use]
    pub fn stats(&self) -> CatalogReloadStats {
        self.stats.snapshot()
    }

    /// A handle that answers [`Self::stats`] without borrowing the reloader,
    /// for a thread that must wait for a reload pass to succeed (the schema
    /// syncer's restart sequence).
    #[must_use]
    pub fn stats_source(&self) -> CatalogReloadStatsSource {
        let stats = Arc::clone(&self.stats);
        Arc::new(move || stats.snapshot())
    }

    /// Stops the thread and waits for it, reporting a panicking worker.
    ///
    /// Idempotent: [`Drop`] calls it, so an explicit call is only needed when
    /// the caller wants to observe the failure.
    pub fn shutdown(&mut self) -> Result<(), CatalogReloadError> {
        let (lock, condvar) = &*self.signal;
        {
            let mut state = match lock.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.shutdown = true;
        }
        condvar.notify_all();
        match self.worker.take() {
            Some(worker) => worker
                .join()
                .map_err(|_| CatalogReloadError::WorkerPanicked),
            None => Ok(()),
        }
    }
}

impl Drop for Syncer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Runs one pass and publishes its result, counting what happened.
///
/// A failed read is not fatal: the previously published catalog stays in force
/// and the next tick tries again, exactly as Go's reload loop logs and retries.
fn run_one_pass(
    catalog: &InfoCache,
    stats: &StatCounters,
    pass: &mut ReloadPass,
    nudged: bool,
) {
    // `trigger` is what makes the watch leg observable from outside the
    // process: without it a prompt reload and a lucky tick look identical in
    // a log, and the live proof would be asserting nothing.
    let trigger = if nudged { "watch" } else { "tick" };
    let _reload = catalog.lock_reload();
    let current = catalog.get_latest();
    let outcome = pass(&current);
    stats.passes.fetch_add(1, Ordering::AcqRel);
    match outcome {
        Ok(CatalogReloadPass::UnchangedAt(ts)) => {
            catalog.refresh_snapshot_ts(ts);
            stats.unchanged.fetch_add(1, Ordering::AcqRel);
        }
        Ok(CatalogReloadPass::Unchanged) => {
            stats.unchanged.fetch_add(1, Ordering::AcqRel);
        }
        Ok(CatalogReloadPass::Diffs(next)) => {
            let schema_version = next.schema_meta_version();
            catalog.upsert(next);
            stats.diff_reloads.fetch_add(1, Ordering::AcqRel);
            emit_reloaded(schema_version, "diffs", trigger);
        }
        Ok(CatalogReloadPass::Full(next)) => {
            let schema_version = next.schema_meta_version();
            catalog.upsert(next);
            stats.full_reloads.fetch_add(1, Ordering::AcqRel);
            emit_reloaded(schema_version, "full", trigger);
        }
        // Go `SyncLoop`'s line for a failed `Reload` (issyncer's logger is
        // `BgLogger`).
        Err(message) => {
            stats.failures.fetch_add(1, Ordering::AcqRel);
            tidb_logutil::bg_logger().error(
                "reload schema in loop failed",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(message))],
            );
        }
    }
}

// Not a log line: rust/scripts/run-realtikv-ddl-notify.sh counts these on
// stderr to prove the watch leg reloads. Go logs its loader's own
// "diff/full load InfoSchema success" lines instead.
fn emit_reloaded(schema_version: i64, how: &str, trigger: &str) {
    eprintln!(
        "{{\"event\":\"catalog_reloaded\",\"schema_version\":{schema_version},\"how\":\"{how}\",\"trigger\":\"{trigger}\"}}"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Instant;

    use super::*;

    /// Go Loader cache hits CloneAndUpdateTS and InfoCache's reporting atomic
    /// are shared with V2 readers, including an older reader held by a statement.
    #[test]
    fn v2_reader_timestamps_feed_the_reporting_cache() {
        struct Empty;
        impl tidb_infoschema::infoschema_v2::TableLoader for Empty {
            fn load_table(&self, _: i64, _: i64, _: u64) -> Result<tidb_infoschema::infoschema_v2::CachedTable, tidb_infoschema::cluster_catalog::ClusterCatalogError> { panic!("no tables") }
            fn list_tables(&self, _: i64, _: u64) -> Result<Vec<tidb_model::GoShared<tidb_model::TableInfo>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> { Ok(vec![]) }
        }
        let cache = InfoCache::new(catalog_at(7));
        let data = cache.data.clone();
        cache.install_schema_builder(Box::new(move |loaded, _| {
            let mut schema = loaded.metadata_copy();
            schema.set_schema_ts(42);
            schema.set_schema_commit_ts(40);
            schema.init_with_db_infos_v2(data.clone(), Arc::new(Empty), 1024);
            schema
        }));
        let old = cache.get_latest();
        cache.refresh_snapshot_ts(100);
        assert_eq!(cache.get_latest().schema_ts(), 100);
        assert_eq!(old.schema_ts(), 42);
        cache.get_and_reset_recent_info_schema_ts(200);
        cache.get_latest().v2().unwrap().schema_table_infos("absent").unwrap();
        assert_eq!(cache.get_and_reset_recent_info_schema_ts(200), 100);
        old.v2().unwrap().schema_table_infos("absent").unwrap();
        assert_eq!(cache.get_and_reset_recent_info_schema_ts(200), 42);
        let historic = cache.get_by_snapshot_ts(70).unwrap();
        assert_eq!(historic.schema_ts(), 70);
        assert_eq!(cache.get_latest().schema_ts(), 100);
    }

    #[test]
    fn snapshot_provider_batch_publications_retain_previous_versions() {
        struct NoScan;
        impl tidb_infoschema::cluster_catalog::MetaSnapshot for NoScan {
            fn get(
                &mut self,
                key: &[u8],
            ) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
                Ok(Some(if key == tidb_meta::key::schema_version_kv_key() {
                    b"7".to_vec()
                } else {
                    b"{}".to_vec()
                }))
            }
            fn scan_prefix(
                &mut self,
                _: &[u8],
            ) -> Result<
                tidb_infoschema::cluster_catalog::MetaPairs,
                tidb_infoschema::cluster_catalog::ClusterCatalogError,
            > {
                panic!("a published retained version must not be full-loaded again")
            }
        }
        let shared = InfoCache::new(catalog_at(7));
        shared.upsert(catalog_at(8));
        assert_eq!(
            shared
                .load_with_ts(&mut NoScan, 16)
                .unwrap()
                .schema_meta_version(),
            7
        );
        assert_eq!(shared.get_latest().schema_meta_version(), 8);
    }

    /// Go `LoadWithTS`: a version newer than the latest is the latest
    /// advanced by its schema diffs (`tryLoadSchemaDiffs`), not a full load.
    #[test]
    fn a_newer_version_loads_by_applying_its_schema_diffs() {
        struct Snapshot {
            version: i64,
            scans: usize,
        }
        impl tidb_infoschema::cluster_catalog::MetaSnapshot for Snapshot {
            fn get(
                &mut self,
                key: &[u8],
            ) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
                if key == tidb_meta::key::schema_version_kv_key() {
                    return Ok(Some(self.version.to_string().into_bytes()));
                }
                // ActionDropSchema of a database nobody holds: a no-op diff.
                Ok((11..=self.version)
                    .find(|version| key == tidb_meta::key::schema_diff_kv_key(*version))
                    .map(|version| {
                        format!(r#"{{"version":{version},"type":2,"schema_id":99}}"#).into_bytes()
                    }))
            }
            fn scan_prefix(
                &mut self,
                _: &[u8],
            ) -> Result<
                tidb_infoschema::cluster_catalog::MetaPairs,
                tidb_infoschema::cluster_catalog::ClusterCatalogError,
            > {
                self.scans += 1;
                Ok(Vec::new())
            }
        }
        let shared = InfoCache::new(catalog_at(10));
        let mut snapshot = Snapshot {
            version: 12,
            scans: 0,
        };
        let loaded = shared.load_with_ts(&mut snapshot, 16).unwrap();
        assert_eq!(loaded.schema_meta_version(), 12);
        assert_eq!(snapshot.scans, 0, "the diffs advance the latest schema");
        assert_eq!(shared.get_latest().schema_meta_version(), 10, "a load never publishes");
    }

    #[test]
    fn snapshot_provider_batch_resize_precedes_lookup() {
        struct Snapshot {
            version: i64,
            scans: usize,
        }
        impl tidb_infoschema::cluster_catalog::MetaSnapshot for Snapshot {
            fn get(
                &mut self,
                key: &[u8],
            ) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
                Ok(Some(if key == tidb_meta::key::schema_version_kv_key() {
                    self.version.to_string().into_bytes()
                } else {
                    b"{}".to_vec()
                }))
            }
            fn scan_prefix(
                &mut self,
                _: &[u8],
            ) -> Result<
                tidb_infoschema::cluster_catalog::MetaPairs,
                tidb_infoschema::cluster_catalog::ClusterCatalogError,
            > {
                self.scans += 1;
                Ok(Vec::new())
            }
        }
        let shared = InfoCache::new(catalog_at(10));
        let mut snapshot = Snapshot {
            version: 7,
            scans: 0,
        };
        shared.load_with_ts(&mut snapshot, 4).unwrap();
        snapshot.version = 8;
        shared.load_with_ts(&mut snapshot, 4).unwrap();
        snapshot.version = 9;
        shared.load_with_ts(&mut snapshot, 4).unwrap();
        snapshot.version = 7;
        snapshot.scans = 0;
        shared.load_with_ts(&mut snapshot, 2).unwrap();
        assert!(snapshot.scans > 0, "Go ReSize evicts before GetByVersion");
    }

    #[test]
    fn historical_read_batch_version_cache_preserves_latest_and_live_capacity() {
        struct Snapshot {
            version: i64,
            scans: usize,
            diff: Option<Vec<u8>>,
        }
        impl tidb_infoschema::cluster_catalog::MetaSnapshot for Snapshot {
            fn get(
                &mut self,
                key: &[u8],
            ) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
                if key == tidb_meta::key::schema_version_kv_key() {
                    Ok(Some(self.version.to_string().into_bytes()))
                } else {
                    Ok(self.diff.clone())
                }
            }
            fn scan_prefix(
                &mut self,
                _: &[u8],
            ) -> Result<
                tidb_infoschema::cluster_catalog::MetaPairs,
                tidb_infoschema::cluster_catalog::ClusterCatalogError,
            > {
                self.scans += 1;
                Ok(Vec::new())
            }
        }
        let shared = InfoCache::new(catalog_at(10));
        let mut snapshot = Snapshot {
            version: 7,
            scans: 0,
            diff: Some(b"{}".to_vec()),
        };
        let old = shared.load_with_ts(&mut snapshot, 3).unwrap();
        assert_eq!(old.schema_meta_version(), 7);
        assert!(snapshot.scans > 0);
        snapshot.scans = 0;
        assert!(Arc::ptr_eq(
            &old,
            &shared.load_with_ts(&mut snapshot, 3).unwrap()
        ));
        assert_eq!(snapshot.scans, 0);
        snapshot.version = 8;
        shared.load_with_ts(&mut snapshot, 3).unwrap();
        snapshot.version = 9;
        shared.load_with_ts(&mut snapshot, 2).unwrap();
        assert_eq!(
            shared
                .historical
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![9, 10]
        );
        assert_eq!(shared.get_latest().schema_meta_version(), 10);
        assert_eq!(
            old.schema_meta_version(), 7,
            "eviction does not invalidate an active reader"
        );
        snapshot.version = 7;
        snapshot.scans = 0;
        shared.load_with_ts(&mut snapshot, 2).unwrap();
        assert!(
            snapshot.scans > 0,
            "evicted schema reloads from its own snapshot"
        );
        shared.upsert(catalog_at(11));
        snapshot.version = 11;
        assert_eq!(
            shared
                .load_with_ts(&mut snapshot, 2)
                .unwrap()
                .schema_meta_version(),
            11
        );
        assert_eq!(shared.historical.lock().unwrap().len(), 2);

        // Go meta.GetSchemaVersionWithNonEmptyDiff: version 11 was reserved
        // at this timestamp, but its diff was not yet published. The later
        // cached version 11 must not leak into the older snapshot.
        snapshot.diff = None;
        assert_eq!(
            shared
                .load_with_ts(&mut snapshot, 2)
                .unwrap()
                .schema_meta_version(),
            10
        );
        snapshot.version = 9;
        assert_eq!(
            shared
                .load_with_ts(&mut snapshot, 2)
                .unwrap()
                .schema_meta_version(),
            8,
            "full historical loads use the effective version too"
        );
        snapshot.version = 0;
        assert_eq!(
            shared
                .load_with_ts(&mut snapshot, 2)
                .unwrap()
                .schema_meta_version(),
            0
        );
        snapshot.version = 11;
        snapshot.diff = Some(b"invalid diff".to_vec());
        assert!(
            shared.load_with_ts(&mut snapshot, 2).is_err(),
            "a cached schema cannot suppress metadata decoding errors"
        );
        assert_eq!(shared.get_latest().schema_meta_version(), 11);
    }

    fn catalog_at(version: i64) -> SessionExtendedInfoSchema {
        SessionExtendedInfoSchema::init_with_db_infos(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            version,
        )
    }

    #[test]
    fn a_published_catalog_replaces_the_previous_one_whole() {
        let shared = InfoCache::new(catalog_at(1));
        let held = shared.get_latest();
        shared.upsert(catalog_at(2));
        // The in-flight reader keeps its own version; the next reader sees the
        // new one. That is the whole consistency contract of the swap.
        assert_eq!(held.schema_meta_version(), 1);
        assert_eq!(shared.get_latest().schema_meta_version(), 2);
    }

    /// Go `InfoCache.GetLatest` answers the highest version: a reload that
    /// finishes after a newer one was published is cached, not published.
    #[test]
    fn a_late_older_catalog_never_replaces_a_newer_one() {
        let shared = InfoCache::new(catalog_at(5));
        assert!(shared.upsert(catalog_at(8)));
        assert!(!shared.upsert(catalog_at(7)));
        assert_eq!(shared.get_latest().schema_meta_version(), 8);
        assert!(shared.historical.lock().unwrap().contains_key(&7));
        assert!(shared.upsert(catalog_at(8)));
        assert_eq!(shared.get_latest().schema_meta_version(), 8);
    }

    #[test]
    fn a_zero_interval_is_refused_rather_than_spinning() {
        let error = Syncer::spawn(
            Arc::new(InfoCache::new(catalog_at(1))),
            Duration::ZERO,
            Box::new(|_| Ok(CatalogReloadPass::Unchanged)),
        )
        .unwrap_err();
        assert_eq!(error, CatalogReloadError::ZeroInterval);
    }

    #[test]
    fn the_thread_publishes_each_pass_and_stops_promptly_on_shutdown() {
        let shared = Arc::new(InfoCache::new(catalog_at(1)));
        let (sender, receiver) = mpsc::channel();
        let mut version = 1;
        let mut reloader = Syncer::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move |current| {
                assert_eq!(current.schema_meta_version(), version);
                version += 1;
                sender.send(version).unwrap();
                Ok(CatalogReloadPass::Diffs(catalog_at(version)))
            }),
        )
        .unwrap();

        assert_eq!(receiver.recv().unwrap(), 2);
        assert_eq!(receiver.recv().unwrap(), 3);

        let stopping = Instant::now();
        reloader.shutdown().unwrap();
        // A condvar-based stop must not wait out a tick; the bound is generous
        // enough for a loaded machine yet far below a long interval.
        assert!(stopping.elapsed() < Duration::from_secs(5));
        assert!(shared.get_latest().schema_meta_version() >= 3);
        assert!(reloader.stats().diff_reloads >= 2);
        // The thread is gone: no further pass can arrive.
        drop(receiver);
    }

    #[test]
    fn a_watch_nudge_reloads_long_before_the_tick_would_have() {
        let shared = Arc::new(InfoCache::new(catalog_at(1)));
        let (sender, receiver) = mpsc::channel();
        // An hour-long tick: anything that arrives here arrived because the
        // nudge woke the thread, not because the lease elapsed.
        let mut reloader = Syncer::spawn(
            Arc::clone(&shared),
            Duration::from_secs(3600),
            Box::new(move |current| {
                let next = current.schema_meta_version() + 1;
                sender.send(next).unwrap();
                Ok(CatalogReloadPass::Diffs(catalog_at(next)))
            }),
        )
        .unwrap();
        let waker = reloader.waker();

        let nudged = Instant::now();
        waker.nudge();
        assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
        assert!(nudged.elapsed() < Duration::from_secs(5));
        waker.nudge();
        assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), 3);

        reloader.shutdown().unwrap();
        let stats = reloader.stats();
        assert_eq!(stats.nudged, 2);
        assert_eq!(stats.passes, 2);
        assert_eq!(shared.get_latest().schema_meta_version(), 3);
    }

    #[test]
    fn a_nudge_that_arrives_before_the_thread_waits_is_not_lost() {
        let shared = Arc::new(InfoCache::new(catalog_at(1)));
        let (sender, receiver) = mpsc::channel();
        let mut reloader = Syncer::spawn(
            Arc::clone(&shared),
            Duration::from_secs(3600),
            Box::new(move |_| {
                sender.send(()).unwrap();
                Ok(CatalogReloadPass::Unchanged)
            }),
        )
        .unwrap();
        // Racing the thread's first wait on purpose: the flag, not the
        // notification, is what the thread reads, so the wake cannot be missed.
        for _ in 0..64 {
            reloader.waker().nudge();
        }
        assert!(receiver.recv_timeout(Duration::from_secs(5)).is_ok());
        reloader.shutdown().unwrap();
    }

    #[test]
    fn a_waker_outliving_its_reloader_is_inert() {
        let mut reloader = Syncer::spawn(
            Arc::new(InfoCache::new(catalog_at(1))),
            Duration::from_secs(3600),
            Box::new(|_| Ok(CatalogReloadPass::Unchanged)),
        )
        .unwrap();
        let waker = reloader.waker();
        reloader.shutdown().unwrap();
        drop(reloader);
        // The watch thread may still be draining one last event.
        waker.nudge();
    }

    #[test]
    fn a_failed_pass_keeps_the_previous_catalog_published() {
        let shared = Arc::new(InfoCache::new(catalog_at(7)));
        let stats = Arc::new(StatCounters::default());
        let mut pass: ReloadPass = Box::new(|_| Err("snapshot read failed".to_owned()));
        run_one_pass(&shared, &stats, &mut pass, false);
        assert_eq!(shared.get_latest().schema_meta_version(), 7);
        assert_eq!(stats.snapshot().failures, 1);
    }

    #[test]
    fn an_unchanged_pass_publishes_nothing() {
        let shared = Arc::new(InfoCache::new(catalog_at(7)));
        let stats = Arc::new(StatCounters::default());
        let published = shared.get_latest();
        let mut pass: ReloadPass = Box::new(|_| Ok(CatalogReloadPass::Unchanged));
        run_one_pass(&shared, &stats, &mut pass, false);
        assert!(Arc::ptr_eq(&published, &shared.get_latest()));
        assert_eq!(stats.snapshot().unchanged, 1);
    }

    #[test]
    fn dropping_the_reloader_stops_its_thread() {
        let shared = Arc::new(InfoCache::new(catalog_at(1)));
        let (sender, receiver) = mpsc::channel();
        let reloader = Syncer::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move |_| {
                let _ = sender.send(());
                Ok(CatalogReloadPass::Unchanged)
            }),
        )
        .unwrap();
        receiver.recv().unwrap();
        drop(reloader);
        // Drain what the thread had already queued, then prove it sent no more:
        // the sender is owned by the closure, so the channel closes with it.
        while receiver.recv_timeout(Duration::from_millis(200)).is_ok() {}
        assert!(receiver.recv_timeout(Duration::from_millis(200)).is_err());
    }
}
