//! TiKV region identity, cache, and request routing authority.
//!
//! This module is the source-shaped boundary between DistSQL ranges and an
//! address-directed RPC. Concrete PD networking remains behind the loader
//! boundary so this module owns topology semantics without owning transport.

mod cache;
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
pub use cache::{RegionCache, RegionLoader, RegionRecoveryLoader};
pub use error::{RegionLoadError, RegionRouteError};
pub use health_policy::{ReplicaHealthFacts, ReplicaHealthPolicy, StoreLabel, StoreSelectionScore};
pub use id::{RegionEpoch, RegionVerId};
pub use location::{
    KeyRange, Peer, PeerRole, RegionLocation, RegionMetadata, RegionMetadataPeer, Store,
};
pub use recovery::{
    OwnedLeaderRoute, RegionAttempt, RegionErrorDisposition, RegionRebuildAction,
    RegionRecoveryError, RegionTerminalError,
};
pub use replica_selector::{ReadPolicy, ReplicaReadMode};
pub use request_selector::{
    LeaderRequest, RequestSelection, RequestSelector, SelectorRecovery, ServerBusyAction,
    MAX_REPLICA_ATTEMPTS, MAX_REPLICA_ATTEMPT_TIME,
};
pub use route::{RouteFeedback, RouteOutcome};
pub use store_health::{
    HealthInstant, StoreHealth, StoreHealthDetail, StoreLoad, StoreRoutingHealth,
};
pub use store_state::{StoreFailureOutcome, StoreLiveness, StoreResolveState, StoreState};
pub(crate) use topology::RegionStoreTopology;
pub use topology::{RouteFeedbackApplication, RoutePeer, RouteSnapshot};
