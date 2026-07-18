//! TiKV RPC transport authority.
//!
//! Region discovery stays outside this module. The concrete client accepts an
//! already selected address and attaches the caller-owned request context only
//! at the final send boundary.

mod channel_pool;
mod error;
mod tonic_coprocessor;

pub use error::{DirectUnaryClientError, DirectUnaryConnectionError};
pub use tonic_coprocessor::TonicCoprocessorClient;
