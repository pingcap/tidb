//! A bounded, foreground PD control-plane client.
//!
//! This crate implements the source call chain needed to discover a plaintext
//! PD member set, locate one region, and resolve its stores. A dedicated worker
//! owns the Tokio runtime so the public synchronous API never nests a runtime
//! owned by its caller. Membership refresh and direct endpoint failover happen
//! only in the foreground and are bounded by the current member set.

mod client;
mod error;
mod model;

pub use client::{PdClient, GET_MEMBERS_PATH, GET_REGION_PATH, GET_STORE_PATH};
pub use error::{PdClientError, PdOperation};
pub use model::{PdMemberSet, PdNodeState, PdPeer, PdRegion, PdRegionEpoch, PdStore, PdStoreState};
