//! TiKV region identity, cache, and request routing authority.
//!
//! This module is the source-shaped boundary between DistSQL ranges and an
//! address-directed RPC. Concrete PD networking remains behind the loader
//! boundary so this module owns topology semantics without owning transport.

mod background;
mod batch_locate;
mod bucket;
mod cache;
mod cache_entry;
mod error;
mod health_policy;
mod id;
mod location;
mod recovery;
mod replica_selector;
mod request_selector;
mod route;
mod slow_score;
mod store_health;
mod store_state;
mod topology;

pub use crate::retry::{RegionBackoffBudget, RegionBackoffExhausted, RegionBackoffKind};
pub use crate::StoreLabel;
pub use background::{
    BackgroundMaintenanceRound, BackgroundRegionCache, BackgroundRegionCacheError,
    BackgroundRegionCacheOwner, StoreLivenessProbe,
};
pub use batch_locate::{
    merge_loaded_and_cached, ranges_after_key, regions_have_gap, regions_intersecting_ranges,
    DEFAULT_REGIONS_PER_BATCH, MAX_RANGES_PER_BATCH,
};
pub use bucket::{Bucket, BucketMetadata, BucketStats};
pub use cache::{
    BatchLoadOptions, BatchRegionLoader, BatchScanBackoff, BatchScanRetryReason, RegionCache,
    RegionGcRound, RegionLoader, RegionQuery, RegionQueryBackoff, RegionQueryLoader,
    RegionQueryOptions, RegionQueryRetryReason, RegionQueryRoute, RegionRecoveryLoader,
    StoreMaintenanceRound, StoreMetadata,
};
pub use cache_entry::{CacheEntryState, CacheReloadState};
pub use error::{RegionLoadError, RegionRouteError};
pub use health_policy::{ReplicaHealthFacts, ReplicaHealthPolicy, StoreSelectionScore};
pub use id::{RegionEpoch, RegionVerId};
pub use location::{
    KeyRange, Peer, PeerRole, RegionLocation, RegionMetadata, RegionMetadataPeer, Store,
};
pub use recovery::{
    OwnedLeaderRoute, RegionAttempt, RegionAttemptObservation, RegionErrorDisposition,
    RegionRebuildAction, RegionRecoveryError, RegionTerminalError,
};
pub use replica_selector::{ReadPolicy, ReplicaReadMode};
pub use request_selector::{
    LeaderRequest, RequestSelection, RequestSelector, SelectorRecovery, ServerBusyAction,
    MAX_REPLICA_ATTEMPTS, MAX_REPLICA_ATTEMPT_TIME,
};
pub use route::{RouteFeedback, RouteOutcome};
pub use slow_score::SlowScoreStat;
pub use store_health::{
    HealthInstant, StoreHealth, StoreHealthDetail, StoreLoad, StoreRoutingHealth,
};
pub use store_state::{
    StoreFailureOutcome, StoreLiveness, StoreRefreshOutcome, StoreResolveState, StoreState,
};
pub(crate) use topology::RegionStoreTopology;
pub use topology::{RouteFeedbackApplication, RoutePeer, RouteSnapshot};
