//! TiKV region identity, cache, and request routing authority.
//!
//! This module is the source-shaped boundary between DistSQL ranges and an
//! address-directed RPC. Concrete PD networking remains behind an injected
//! loader until its separately pinned source universe is governed.

mod cache;
mod error;
mod id;
mod location;
mod replica_selector;

pub use cache::{RegionCache, RegionLoader};
pub use error::{RegionLoadError, RegionRouteError};
pub use id::{RegionEpoch, RegionVerId};
pub use location::{KeyRange, Peer, PeerRole, RegionLocation, Store};
pub use replica_selector::{LeaderRoute, ReadPolicy, ReplicaReadMode, ReplicaSelector};
