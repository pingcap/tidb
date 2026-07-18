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

//! One-attempt asynchronous region-request policy.
//!
//! This module deliberately does not own replica selection, cache mutation,
//! backoff, or the synchronous retry loop. The caller selects one immutable
//! [`LeaderRequest`], and [`AsyncRegionAttemptPolicy`] applies the completion
//! to the same request-local selector/cache authority used by synchronous
//! requests. A retry action therefore resumes that existing loop instead of
//! creating a second async retry runtime.

use std::time::{Duration, Instant};

use tidb_proto::{KvrpcPeer, KvrpcRegionEpoch};

use super::LeaderRequest;
use crate::rpc::{
    AsyncRequestDispatcher, CompletionRequest, DirectUnaryClientError, PendingRequest,
    UnaryCallContext,
};
use crate::{DirectUnaryRequest, DirectUnaryResponse, EndpointType};

/// Successful raw response paired with the final logical TiKV target.
///
/// The address is the selected target, not an optional forwarding proxy. A
/// synchronous fallback may therefore return a different final address from
/// the first asynchronous attempt without losing routing identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedRegionResponse {
    /// Raw successful response from TiKV.
    pub response: DirectUnaryResponse,
    /// Logical TiKV address which interpreted the request.
    pub logical_address: String,
}

impl RoutedRegionResponse {
    /// Binds a successful response to its final logical target.
    #[must_use]
    pub fn new(response: DirectUnaryResponse, logical_address: impl Into<String>) -> Self {
        Self {
            response,
            logical_address: logical_address.into(),
        }
    }
}

/// Why the caller-owned synchronous region-request loop must continue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AsyncRegionRetryCause {
    /// A successful RPC carried a region error which canonical cache policy
    /// consumed before requesting a retry.
    RegionError(DirectUnaryResponse),
    /// The selected physical connection failed and canonical store feedback
    /// consumed the exact transport error before requesting a retry.
    SendFailure(DirectUnaryClientError),
}

/// Callback decision made by the canonical selector/cache policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AsyncRegionAttemptDecision {
    /// Publish this result through the once-only terminal completion.
    Complete(Result<DirectUnaryResponse, DirectUnaryClientError>),
    /// Resume the existing synchronous region-request loop.
    Retry(AsyncRegionRetryCause),
}

/// Narrow adapter to the canonical request selector and region cache.
///
/// Implementations must record `elapsed` against `route.target()` exactly
/// once, then apply success, region-error, or send-failure feedback through
/// the existing selector/cache methods. This trait intentionally has no
/// selection or backoff methods: those remain owned by the caller's existing
/// synchronous request loop.
pub trait AsyncRegionAttemptPolicy {
    /// Applies one successful transport response, including decoding and
    /// canonical region-error handling.
    fn on_response(
        &mut self,
        route: &LeaderRequest,
        elapsed: Duration,
        response: DirectUnaryResponse,
    ) -> AsyncRegionAttemptDecision;

    /// Applies one transport failure, including canonical store liveness and
    /// route-failure feedback.
    fn on_send_failure(
        &mut self,
        route: &LeaderRequest,
        elapsed: Duration,
        error: DirectUnaryClientError,
    ) -> AsyncRegionAttemptDecision;
}

/// Observable state of one source-shaped asynchronous attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsyncRegionAttemptState {
    /// The exact selected attempt has not completed yet.
    Pending,
    /// Canonical policy requested the caller-owned synchronous retry loop.
    AwaitingSynchronousRetry,
    /// A terminal result won, or caller cancellation suppressed publication.
    Complete,
}

/// Result of non-blockingly driving one asynchronous attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AsyncRegionAttemptPoll {
    /// No callback is ready.
    Pending,
    /// Resume the existing synchronous loop with the already-applied feedback.
    Retry(AsyncRegionRetryCause),
    /// This state machine cannot publish or request another result.
    Complete,
}

enum AttemptPhase<P> {
    Pending { pending: P, started_at: Instant },
    AwaitingSynchronousRetry { cause: AsyncRegionRetryCause },
    Complete,
}

/// One immutable selected region attempt behind the shared async dispatcher.
///
/// Construction dispatches at most once. [`Self::poll`] never performs
/// selection or retry; a retry result transfers control back to the caller.
/// After that loop reaches a terminal result, [`Self::complete_retry`] routes
/// it through the same [`CompletionRequest`] once gate.
pub struct AsyncRegionRequestAttempt<P, A> {
    route: LeaderRequest,
    call: UnaryCallContext,
    completion: CompletionRequest<RoutedRegionResponse, DirectUnaryClientError>,
    policy: A,
    phase: AttemptPhase<P>,
}

impl<P, A> AsyncRegionRequestAttempt<P, A>
where
    P: PendingRequest,
    A: AsyncRegionAttemptPolicy,
{
    /// Validates and dispatches exactly one already-selected request.
    #[must_use]
    pub fn begin<D>(
        dispatcher: &mut D,
        route: LeaderRequest,
        request: &DirectUnaryRequest,
        call: UnaryCallContext,
        completion: CompletionRequest<RoutedRegionResponse, DirectUnaryClientError>,
        mut policy: A,
    ) -> Self
    where
        D: AsyncRequestDispatcher<Pending = P>,
    {
        if completion.is_cancelled() {
            return Self {
                route,
                call,
                completion,
                policy,
                phase: AttemptPhase::Complete,
            };
        }
        if call.cancellation().is_cancelled() {
            completion.invoke(Err(DirectUnaryClientError::CallerCancelled));
            return Self {
                route,
                call,
                completion,
                policy,
                phase: AttemptPhase::Complete,
            };
        }
        let prepared = match prepare_request(request, &route) {
            Ok(request) => request,
            Err(error) => {
                completion.invoke(Err(error));
                return Self {
                    route,
                    call,
                    completion,
                    policy,
                    phase: AttemptPhase::Complete,
                };
            }
        };
        let started_at = Instant::now();
        let phase = match dispatcher.begin(
            route.dispatch_address(),
            route.forwarded_host(),
            &prepared,
            &call,
        ) {
            Ok(pending) => AttemptPhase::Pending {
                pending,
                started_at,
            },
            Err(_) if completion.is_cancelled() => AttemptPhase::Complete,
            Err(_) if call.cancellation().is_cancelled() => {
                completion.schedule(Err(DirectUnaryClientError::CallerCancelled));
                AttemptPhase::Complete
            }
            Err(error) => {
                let decision = policy.on_send_failure(&route, started_at.elapsed(), error);
                if completion.is_cancelled() {
                    AttemptPhase::Complete
                } else if call.cancellation().is_cancelled() {
                    completion.schedule(Err(DirectUnaryClientError::CallerCancelled));
                    AttemptPhase::Complete
                } else {
                    match decision {
                        AsyncRegionAttemptDecision::Complete(result) => {
                            completion.schedule(route_result(&route, result));
                            AttemptPhase::Complete
                        }
                        AsyncRegionAttemptDecision::Retry(cause) => {
                            AttemptPhase::AwaitingSynchronousRetry { cause }
                        }
                    }
                }
            }
        };
        Self {
            route,
            call,
            completion,
            policy,
            phase,
        }
    }

    /// Current source-attempt state.
    #[must_use]
    pub const fn state(&self) -> AsyncRegionAttemptState {
        match &self.phase {
            AttemptPhase::Pending { .. } => AsyncRegionAttemptState::Pending,
            AttemptPhase::AwaitingSynchronousRetry { .. } => {
                AsyncRegionAttemptState::AwaitingSynchronousRetry
            }
            AttemptPhase::Complete => AsyncRegionAttemptState::Complete,
        }
    }

    /// Drives the exact pending callback without blocking.
    ///
    /// Caller cancellation is checked before selector/cache feedback. This is
    /// the source invariant which prevents a cancelled request from dropping
    /// or reloading a still-valid cached region.
    pub fn poll(&mut self) -> AsyncRegionAttemptPoll {
        if let AttemptPhase::AwaitingSynchronousRetry { cause } = &self.phase {
            return AsyncRegionAttemptPoll::Retry(cause.clone());
        }
        let AttemptPhase::Pending {
            pending,
            started_at,
        } = &mut self.phase
        else {
            return AsyncRegionAttemptPoll::Complete;
        };

        if self.completion.is_cancelled() {
            pending.cancel();
            self.phase = AttemptPhase::Complete;
            return AsyncRegionAttemptPoll::Complete;
        }
        if self.call.cancellation().is_cancelled() {
            pending.cancel();
            self.completion
                .schedule(Err(DirectUnaryClientError::CallerCancelled));
            self.phase = AttemptPhase::Complete;
            return AsyncRegionAttemptPoll::Complete;
        }

        let outcome = match pending.try_complete() {
            Ok(Some(outcome)) => outcome,
            Ok(None) => return AsyncRegionAttemptPoll::Pending,
            Err(error) => {
                self.completion
                    .schedule(Err(DirectUnaryClientError::Runtime(error.to_string())));
                self.phase = AttemptPhase::Complete;
                return AsyncRegionAttemptPoll::Complete;
            }
        };

        // Cancellation wins even when it races with an already-ready callback.
        if self.completion.is_cancelled() {
            self.phase = AttemptPhase::Complete;
            return AsyncRegionAttemptPoll::Complete;
        }
        if self.call.cancellation().is_cancelled()
            || matches!(&outcome, Err(DirectUnaryClientError::CallerCancelled))
        {
            self.completion
                .schedule(Err(DirectUnaryClientError::CallerCancelled));
            self.phase = AttemptPhase::Complete;
            return AsyncRegionAttemptPoll::Complete;
        }

        let elapsed = started_at.elapsed();
        let decision = match outcome {
            Ok(response) => self.policy.on_response(&self.route, elapsed, response),
            Err(error) => self.policy.on_send_failure(&self.route, elapsed, error),
        };
        if self.completion.is_cancelled() {
            self.phase = AttemptPhase::Complete;
            return AsyncRegionAttemptPoll::Complete;
        }
        if self.call.cancellation().is_cancelled() {
            self.completion
                .schedule(Err(DirectUnaryClientError::CallerCancelled));
            self.phase = AttemptPhase::Complete;
            return AsyncRegionAttemptPoll::Complete;
        }
        match decision {
            AsyncRegionAttemptDecision::Complete(result) => {
                self.completion.schedule(route_result(&self.route, result));
                self.phase = AttemptPhase::Complete;
                AsyncRegionAttemptPoll::Complete
            }
            AsyncRegionAttemptDecision::Retry(cause) => {
                self.phase = AttemptPhase::AwaitingSynchronousRetry {
                    cause: cause.clone(),
                };
                AsyncRegionAttemptPoll::Retry(cause)
            }
        }
    }

    /// Runs the caller-owned synchronous retry loop with the same policy and
    /// call context, then publishes through the same completion once gate.
    ///
    /// `retry` owns selection, cache reload, backoff, and every synchronous
    /// attempt. Cancellation is checked on both sides of the closure so a
    /// fallback result cannot win a race with caller cancellation.
    pub fn run_synchronous_retry<F>(&mut self, retry: F)
    where
        F: FnOnce(
            &mut A,
            &UnaryCallContext,
        ) -> Result<RoutedRegionResponse, DirectUnaryClientError>,
    {
        if !matches!(&self.phase, AttemptPhase::AwaitingSynchronousRetry { .. }) {
            return;
        }
        if self.completion.is_cancelled() {
            self.phase = AttemptPhase::Complete;
            return;
        }
        if self.call.cancellation().is_cancelled() {
            self.completion
                .schedule(Err(DirectUnaryClientError::CallerCancelled));
            self.phase = AttemptPhase::Complete;
            return;
        }

        let result = retry(&mut self.policy, &self.call);
        if self.completion.is_cancelled() {
            self.phase = AttemptPhase::Complete;
            return;
        }
        if self.call.cancellation().is_cancelled() {
            self.completion
                .schedule(Err(DirectUnaryClientError::CallerCancelled));
        } else {
            self.completion.schedule(result);
        }
        self.phase = AttemptPhase::Complete;
    }

    /// Publishes an already-computed routed fallback result.
    pub fn complete_retry(&mut self, result: Result<RoutedRegionResponse, DirectUnaryClientError>) {
        self.run_synchronous_retry(|_, _| result);
    }

    /// Returns the policy, including its canonical selector/cache adapter.
    #[must_use]
    pub fn into_policy(self) -> A {
        self.policy
    }
}

fn route_result(
    route: &LeaderRequest,
    result: Result<DirectUnaryResponse, DirectUnaryClientError>,
) -> Result<RoutedRegionResponse, DirectUnaryClientError> {
    result.map(|response| RoutedRegionResponse::new(response, route.target().address.clone()))
}

fn prepare_request(
    request: &DirectUnaryRequest,
    route: &LeaderRequest,
) -> Result<DirectUnaryRequest, DirectUnaryClientError> {
    if request.endpoint != EndpointType::TiKv {
        return Err(DirectUnaryClientError::InvalidRequest(
            "async region requests require a TiKV endpoint".to_owned(),
        ));
    }
    if route.attempt.region.id == 0 || route.attempt.peer_id == 0 || route.attempt.store_id == 0 {
        return Err(DirectUnaryClientError::InvalidRequest(
            "async region requests require a nonzero region, peer, and store".to_owned(),
        ));
    }
    if route.dispatch_address().is_empty() {
        return Err(DirectUnaryClientError::InvalidAddress {
            address: String::new(),
            message: "selected region attempt has no physical address".to_owned(),
        });
    }

    let mut prepared = request.clone();
    prepared.replica_read = route.replica_read;
    prepared.stale_read = route.stale_read;
    prepared.context.region_id = route.attempt.region.id;
    prepared.context.region_epoch = Some(KvrpcRegionEpoch {
        conf_ver: route.attempt.region.epoch.conf_ver,
        version: route.attempt.region.epoch.version,
    });
    prepared.context.peer = Some(KvrpcPeer {
        id: route.attempt.peer_id,
        store_id: route.attempt.store_id,
        role: route.role.as_i32(),
        is_witness: route.is_witness,
    });
    prepared.context.replica_read = route.replica_read;
    prepared.context.stale_read = route.stale_read;
    Ok(prepared)
}
