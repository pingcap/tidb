//! TiKV RPC transport authority.
//!
//! Region discovery stays outside this module. The concrete client accepts an
//! already selected address and attaches the caller-owned request context only
//! at the final send boundary.

mod async_completion;
mod batch;
mod channel_pool;
mod error;
mod forwarding;
mod liveness;
mod tonic_coprocessor;
mod unary;

pub use async_completion::{
    completion_pair, AsyncRequestDispatcher, CompletionCallback, CompletionCancellation,
    CompletionCancellationReason, CompletionError, CompletionPull, CompletionRunLoop,
    CompletionRunLoopState, CompletionRunOutcome, CompletionSpawner, PendingRequest,
};
pub use error::{
    DirectUnaryClientError, DirectUnaryConnectionError, DirectUnaryGrpcCode,
    DirectUnaryTransportClass,
};
pub use liveness::DEFAULT_STORE_LIVENESS_TIMEOUT;
pub use tonic_coprocessor::TonicCoprocessorClient;
pub use unary::{UnaryCallContext, UnaryCancellation};
