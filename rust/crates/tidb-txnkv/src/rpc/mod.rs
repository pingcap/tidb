//! TiKV RPC transport authority.
//!
//! Region discovery stays outside this module. The concrete client accepts an
//! already selected address and attaches the caller-owned request context only
//! at the final send boundary.

mod async_completion;
pub mod batch;
mod channel_pool;
mod error;
mod forwarding;
mod liveness;
mod tonic_coprocessor;
mod transport_runtime;
mod unary;

pub use async_completion::{
    completion_pair, AsyncRequestDispatcher, CompletionCallback, CompletionCancellation,
    CompletionCancellationReason, CompletionError, CompletionPull, CompletionRequest,
    CompletionRunLoop, CompletionRunLoopState, CompletionRunOutcome, CompletionSpawner,
    PendingRequest,
};
pub use error::{
    DirectUnaryClientError, DirectUnaryConnectionError, DirectUnaryGrpcCode,
    DirectUnaryTransportClass,
};
pub use liveness::DEFAULT_STORE_LIVENESS_TIMEOUT;
pub use tonic_coprocessor::TonicCoprocessorClient;
pub use transport_runtime::TransportShutdownCancellation;
pub use unary::{UnaryCallContext, UnaryCancellation};

impl<T, E> batch::BatchEntryCompletion for CompletionRequest<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    type Error = E;

    fn is_canceled(&self) -> bool {
        self.is_cancelled()
    }

    fn fail(&self, error: E) {
        self.schedule_error(error);
    }
}
