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

//! Command-neutral address-directed unary TiKV transport.
//!
//! One worker owns the Tokio runtime, the sole address-keyed channel pool,
//! connection generations, deadlines, and the in-flight cancellation wait.
//! Command adapters provide only a static gRPC path and encoded protobuf body;
//! region routing, retry, lock policy, and response interpretation stay above
//! this transport authority.

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

use crate::region::StoreLiveness;

use super::async_completion::RunLoopSignal;
use super::batch::{BatchCommandEntry, BatchPublicationReceipt};
use super::channel_pool::ChannelPool;
use super::forwarding;
use super::transport_runtime::{TransportRuntime, TransportShutdownCancellation};
use super::{DirectUnaryClientError, DirectUnaryConnectionError, DirectUnaryGrpcCode};

// client-go internal/client sets MaxRecvMsgSize to math.MaxInt64-1. Tonic's
// default is only 4 MiB, which is too small for valid TiKV responses.
const MAX_RECV_MESSAGE_SIZE: usize = (i64::MAX as usize).saturating_sub(1);

/// Cloneable caller-owned cancellation state for one or more unary calls.
///
/// The carrier deliberately contains no tonic status or command identity.
/// Cancellation is monotonic and can be triggered from any thread while the
/// synchronous caller is waiting for the worker reply.
#[derive(Clone, Debug)]
pub struct UnaryCancellation {
    state: tokio::sync::watch::Sender<bool>,
    blocking_wait: Arc<(Mutex<()>, Condvar)>,
    completion_waiters: Arc<Mutex<Vec<Weak<RunLoopSignal>>>>,
}

impl Default for UnaryCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl UnaryCancellation {
    /// Creates an active, not-yet-cancelled carrier.
    #[must_use]
    pub fn new() -> Self {
        let (state, _) = tokio::sync::watch::channel(false);
        Self {
            state,
            blocking_wait: Arc::new((Mutex::new(()), Condvar::new())),
            completion_waiters: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Makes cancellation visible to current and future calls.
    pub fn cancel(&self) {
        let (lock, changed) = self.blocking_wait.as_ref();
        let guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        self.state.send_replace(true);
        drop(guard);
        changed.notify_all();
        let waiters = {
            let mut registered = self
                .completion_waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut waiters = Vec::with_capacity(registered.len());
            registered.retain(|waiting| {
                if let Some(waiting) = waiting.upgrade() {
                    waiters.push(waiting);
                    true
                } else {
                    false
                }
            });
            waiters
        };
        for waiting in waiters {
            waiting.wake_all();
        }
    }

    /// Whether the caller has already cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.state.borrow()
    }

    /// Whether both carriers observe and update the same cancellation state.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        self.state.same_channel(&other.state)
    }

    /// Waits until this carrier is cancelled or `timeout` elapses.
    ///
    /// Returns `true` only for caller cancellation. A `false` result means the
    /// full timeout elapsed. The condition variable is paired with the same
    /// watch value used by in-flight tonic calls, so synchronous TTL waiting
    /// neither polls nor creates another cancellation authority.
    #[must_use]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let (lock, changed) = self.blocking_wait.as_ref();
        let guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_cancelled() {
            return true;
        }
        let _ = changed
            .wait_timeout_while(guard, timeout, |_| !self.is_cancelled())
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.is_cancelled()
    }

    pub(super) fn register_completion_waiter(&self, run_loop: &Arc<RunLoopSignal>) {
        let waiting = Arc::downgrade(run_loop);
        let mut waiters = self
            .completion_waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.is_cancelled()
            && !waiters
                .iter()
                .any(|registered| Weak::ptr_eq(registered, &waiting))
        {
            waiters.push(waiting);
        }
    }

    pub(super) fn unregister_completion_waiter(&self, run_loop: &Arc<RunLoopSignal>) {
        let waiting = Arc::downgrade(run_loop);
        self.completion_waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|registered| {
                registered.strong_count() > 0 && !Weak::ptr_eq(registered, &waiting)
            });
    }

    async fn cancelled(&self) {
        let mut receiver = self.state.subscribe();
        if *receiver.borrow() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                return;
            }
        }
    }
}

/// Transport-neutral per-call deadline and cancellation authority.
#[derive(Clone, Debug)]
pub struct UnaryCallContext {
    deadline: Instant,
    cancellation: UnaryCancellation,
}

impl UnaryCallContext {
    /// Binds an exact timeout and caller-owned cancellation carrier.
    #[must_use]
    pub fn new(timeout: Duration, cancellation: UnaryCancellation) -> Self {
        Self::with_deadline(Instant::now() + timeout, cancellation)
    }

    /// Binds an existing absolute deadline and caller-owned cancellation.
    #[must_use]
    pub const fn with_deadline(deadline: Instant, cancellation: UnaryCancellation) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    /// Constructs a non-cancelled call context for compatibility callers.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::new(timeout, UnaryCancellation::new())
    }

    /// Exact local and remote gRPC timeout.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Returns the one absolute deadline shared by every command in this read.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Shared cancellation carrier for this call.
    #[must_use]
    pub const fn cancellation(&self) -> &UnaryCancellation {
        &self.cancellation
    }
}

#[derive(Clone, Debug)]
pub(super) struct RawUnaryRequest {
    pub(super) path: &'static str,
    pub(super) encoded_request: Vec<u8>,
    pub(super) forwarded_host: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RawUnaryResponse {
    pub(super) encoded_response: Vec<u8>,
}

enum UnaryAttemptError {
    Connection(String),
    RemoteGrpc(tonic::Status),
}

enum UnaryCallOutcome {
    CallerCancelled,
    Completed(
        Result<Result<tonic::Response<Vec<u8>>, UnaryAttemptError>, tokio::time::error::Elapsed>,
    ),
}

/// Synchronous handle to the sole shared unary and BatchCommands transport.
pub(super) struct RawTransportClient {
    transport: TransportRuntime,
}

impl RawTransportClient {
    pub(super) fn new() -> Result<Self, DirectUnaryClientError> {
        Ok(Self {
            transport: TransportRuntime::new()?,
        })
    }

    pub(super) fn send(
        &mut self,
        address: &str,
        request: RawUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<RawUnaryResponse, DirectUnaryClientError> {
        if call.cancellation().is_cancelled() {
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        self.transport.unary_send(address, request, call)
    }

    pub(super) fn submit_batch(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        self.transport.batch_submit(address, entries)
    }

    pub(super) fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        self.transport.close_address(address)
    }

    pub(super) fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.transport.close_address_version(address, version)
    }

    pub(super) fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.transport.liveness(address, timeout)
    }

    pub(super) fn inspect(&self, address: &str) -> (Option<u64>, usize) {
        self.transport.inspect(address)
    }

    pub(super) fn inspect_batch(
        &self,
        address: &str,
        forwarded_host: Option<&str>,
    ) -> (Option<u64>, u64) {
        self.transport.inspect_batch(address, forwarded_host)
    }

    pub(super) fn shutdown_cancellation(&self) -> TransportShutdownCancellation {
        self.transport.shutdown_cancellation()
    }

    pub(super) fn shutdown(&mut self) {
        self.transport.shutdown();
    }
}

impl Drop for RawTransportClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(super) fn send_unary(
    runtime: &tokio::runtime::Runtime,
    channels: &mut ChannelPool,
    address: &str,
    request: RawUnaryRequest,
    call: &UnaryCallContext,
) -> Result<RawUnaryResponse, DirectUnaryClientError> {
    if call.cancellation().is_cancelled() {
        return Err(DirectUnaryClientError::CallerCancelled);
    }
    let mut rpc_request = tonic::Request::new(request.encoded_request);
    forwarding::attach_forwarded_host(&mut rpc_request, request.forwarded_host.as_deref())?;
    let selected = channels.get_or_create(address, runtime)?;
    let timeout = call.timeout();
    if timeout.is_zero() {
        return Err(timeout_error(
            address,
            selected.version,
            timeout,
            "absolute unary deadline elapsed",
        ));
    }
    rpc_request.set_timeout(timeout);
    let mut client =
        tonic::client::Grpc::new(selected.channel).max_decoding_message_size(MAX_RECV_MESSAGE_SIZE);
    let path = tonic::codegen::http::uri::PathAndQuery::from_static(request.path);
    let cancellation = call.cancellation().clone();
    let result = runtime.block_on(async {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => UnaryCallOutcome::CallerCancelled,
            result = tokio::time::timeout(timeout, async {
                client.ready().await.map_err(|error| {
                    UnaryAttemptError::Connection(format!("TiKV gRPC service is not ready: {error}"))
                })?;
                client
                    .unary(rpc_request, path, RawProtobufCodec)
                    .await
                    .map_err(UnaryAttemptError::RemoteGrpc)
            }) => UnaryCallOutcome::Completed(result),
        }
    });
    let response = match result {
        UnaryCallOutcome::CallerCancelled => {
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        UnaryCallOutcome::Completed(Ok(Ok(response))) => response.into_inner(),
        UnaryCallOutcome::Completed(Ok(Err(UnaryAttemptError::Connection(error)))) => {
            return Err(connection_error(address, selected.version, error));
        }
        UnaryCallOutcome::Completed(Ok(Err(UnaryAttemptError::RemoteGrpc(error)))) => {
            return Err(remote_grpc_error(address, selected.version, timeout, error));
        }
        UnaryCallOutcome::Completed(Err(error)) => {
            return Err(timeout_error(address, selected.version, timeout, error));
        }
    };
    Ok(RawUnaryResponse {
        encoded_response: response,
    })
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RawProtobufCodec;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RawProtobufEncoder;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RawProtobufDecoder;

impl Codec for RawProtobufCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = RawProtobufEncoder;
    type Decoder = RawProtobufDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawProtobufEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawProtobufDecoder
    }
}

impl Encoder for RawProtobufEncoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        destination.put_slice(&item);
        Ok(())
    }
}

impl Decoder for RawProtobufDecoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(copy_remaining(source)))
    }
}

fn copy_remaining(source: &mut impl Buf) -> Vec<u8> {
    source.copy_to_bytes(source.remaining()).to_vec()
}

fn connection_error(
    address: &str,
    version: u64,
    error: impl std::fmt::Display,
) -> DirectUnaryClientError {
    DirectUnaryClientError::Connection(DirectUnaryConnectionError::connection(
        address,
        version,
        error.to_string(),
    ))
}

fn remote_grpc_error(
    address: &str,
    version: u64,
    timeout: Duration,
    error: tonic::Status,
) -> DirectUnaryClientError {
    if error_chain_contains_timeout(&error) {
        return timeout_error(address, version, timeout, error);
    }
    match grpc_error_code(error.code()) {
        Some(code) => DirectUnaryClientError::Connection(DirectUnaryConnectionError::remote_grpc(
            address,
            version,
            code,
            error.to_string(),
        )),
        None => connection_error(address, version, error),
    }
}

fn error_chain_contains_timeout(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(candidate) = current {
        if candidate.downcast_ref::<tonic::TimeoutExpired>().is_some() {
            return true;
        }
        current = candidate.source();
    }
    false
}

fn timeout_error(
    address: &str,
    version: u64,
    timeout: Duration,
    error: impl std::fmt::Display,
) -> DirectUnaryClientError {
    DirectUnaryClientError::Timeout {
        connection: DirectUnaryConnectionError::local_deadline(address, version, error.to_string()),
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    }
}

const fn grpc_error_code(code: tonic::Code) -> Option<DirectUnaryGrpcCode> {
    match code {
        tonic::Code::Ok => None,
        tonic::Code::Cancelled => Some(DirectUnaryGrpcCode::Canceled),
        tonic::Code::Unknown => Some(DirectUnaryGrpcCode::Unknown),
        tonic::Code::InvalidArgument => Some(DirectUnaryGrpcCode::InvalidArgument),
        tonic::Code::DeadlineExceeded => Some(DirectUnaryGrpcCode::DeadlineExceeded),
        tonic::Code::NotFound => Some(DirectUnaryGrpcCode::NotFound),
        tonic::Code::AlreadyExists => Some(DirectUnaryGrpcCode::AlreadyExists),
        tonic::Code::PermissionDenied => Some(DirectUnaryGrpcCode::PermissionDenied),
        tonic::Code::ResourceExhausted => Some(DirectUnaryGrpcCode::ResourceExhausted),
        tonic::Code::FailedPrecondition => Some(DirectUnaryGrpcCode::FailedPrecondition),
        tonic::Code::Aborted => Some(DirectUnaryGrpcCode::Aborted),
        tonic::Code::OutOfRange => Some(DirectUnaryGrpcCode::OutOfRange),
        tonic::Code::Unimplemented => Some(DirectUnaryGrpcCode::Unimplemented),
        tonic::Code::Internal => Some(DirectUnaryGrpcCode::Internal),
        tonic::Code::Unavailable => Some(DirectUnaryGrpcCode::Unavailable),
        tonic::Code::DataLoss => Some(DirectUnaryGrpcCode::DataLoss),
        tonic::Code::Unauthenticated => Some(DirectUnaryGrpcCode::Unauthenticated),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{error_chain_contains_timeout, UnaryCancellation};

    #[test]
    fn tonic_timeout_source_is_not_misclassified_as_remote_canceled() {
        let status = tonic::Status::from_error(Box::new(tonic::TimeoutExpired(())));
        assert_eq!(status.code(), tonic::Code::Cancelled);
        assert!(error_chain_contains_timeout(&status));
    }

    #[test]
    fn blocking_wait_distinguishes_cancellation_from_elapsed_ttl() {
        let already_cancelled = UnaryCancellation::new();
        already_cancelled.cancel();
        assert!(already_cancelled.wait_timeout(Duration::from_secs(2)));

        let cancellation = UnaryCancellation::new();
        let waiter = cancellation.clone();
        let cancel = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            waiter.cancel();
        });
        let started = Instant::now();
        assert!(cancellation.wait_timeout(Duration::from_secs(2)));
        assert!(started.elapsed() < Duration::from_secs(1));
        cancel.join().unwrap();

        let elapsed = UnaryCancellation::new();
        assert!(!elapsed.wait_timeout(Duration::from_millis(10)));
        assert!(!elapsed.is_cancelled());
    }
}
