//! TiKV region identity, cache, and request routing authority.
//!
//! This module is the source-shaped boundary between DistSQL ranges and an
//! address-directed RPC. Concrete PD networking remains behind the loader
//! boundary so this module owns topology semantics without owning transport.

mod cache;
mod error;
mod id;
mod location;
mod recovery;
mod replica_selector;

pub use crate::retry::{RegionBackoffBudget, RegionBackoffExhausted, RegionBackoffKind};
pub use cache::{RegionCache, RegionLoader, RegionRecoveryLoader};
pub use error::{RegionLoadError, RegionRouteError};
pub use id::{RegionEpoch, RegionVerId};
pub use location::{
    KeyRange, Peer, PeerRole, RegionLocation, RegionMetadata, RegionMetadataPeer, Store,
};
pub use recovery::{
    OwnedLeaderRoute, RegionAttempt, RegionErrorDisposition, RegionRebuildAction,
    RegionRecoveryError, RegionTerminalError,
};
pub use replica_selector::{LeaderRoute, ReadPolicy, ReplicaReadMode, ReplicaSelector};
