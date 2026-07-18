//! A bounded, single-endpoint PD control-plane client.
//!
//! This crate implements only the source call chain needed to locate one
//! region: bootstrap cluster identity with `GetMembers`, locate by encoded key
//! with `GetRegion`, and resolve referenced stores with `GetStore`. A dedicated
//! worker owns the Tokio runtime so the public synchronous API never nests a
//! runtime owned by its caller.

mod client;
mod error;
mod model;

pub use client::{PdClient, GET_MEMBERS_PATH, GET_REGION_PATH, GET_STORE_PATH};
pub use error::{PdClientError, PdOperation};
pub use model::{PdNodeState, PdPeer, PdPeerRole, PdRegion, PdRegionEpoch, PdStore, PdStoreState};
