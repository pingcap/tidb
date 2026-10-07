// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Complete pinned PD `pkg/batch` collection and completion ownership.
//! Source channels are never closed. Rust channel closure is terminal and
//! finishes owned work instead of manufacturing Go's zero-value requests.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use prometheus::Histogram;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};

use crate::async_util::Cancellation;
use crate::{Error, Result};

const DEFAULT_BEST_BATCH_SIZE: usize = 8;

/// Indexed completion callback; owned requests may be non-Clone Rust senders.
pub type Finisher<'a, T> = dyn FnMut(usize, T, Option<&Error>) + Send + 'a;

/// A request queue projected by its owner. This lets synchronous-facing
/// clients share collection without copying the batch algorithm.
pub trait RequestReceiver<T> {
    /// Waits for the next request; None means the owner closed the queue.
    fn recv(&mut self) -> impl std::future::Future<Output = Option<T>> + Send;
    /// Takes a queued request without waiting.
    fn try_recv(&mut self) -> std::result::Result<T, mpsc::error::TryRecvError>;
}

impl<T: Send> RequestReceiver<T> for mpsc::Receiver<T> {
    async fn recv(&mut self) -> Option<T> {
        mpsc::Receiver::recv(self).await
    }
    fn try_recv(&mut self) -> std::result::Result<T, mpsc::error::TryRecvError> {
        mpsc::Receiver::try_recv(self)
    }
}

/// Collects one batch at a time and retains its buffer for the next round.
pub struct Controller<T> {
    max_batch_size: usize,
    best_batch_size: usize,
    requests: Vec<T>,
    finisher: Option<Box<Finisher<'static, T>>>,
    best_batch_observer: Option<Histogram>,
    extra_batching_start_time: Option<Instant>,
}

impl<T> Controller<T> {
    /// Creates the source's max+1 buffer and initial target of eight requests.
    pub fn new(
        max_batch_size: usize,
        finisher: Option<Box<Finisher<'static, T>>>,
        best_batch_observer: Option<Histogram>,
    ) -> Self {
        Self {
            max_batch_size,
            best_batch_size: DEFAULT_BEST_BATCH_SIZE,
            requests: Vec::with_capacity(
                max_batch_size.checked_add(1).expect("batch size overflow"),
            ),
            finisher,
            best_batch_observer,
            extra_batching_start_time: None,
        }
    }

    /// Collects after both the first request and an optional RPC token arrive.
    /// Success transfers the token to the caller. Failure or dropping this
    /// future returns the token before finishing every collected request.
    /// Zero wait corresponds to Go's nonpositive optional extra wait.
    pub async fn fetch_pending_requests(
        &mut self,
        ctx: &Cancellation,
        requests: &mut impl RequestReceiver<T>,
        tokens: Option<&Arc<Semaphore>>,
        max_batch_wait: Duration,
    ) -> Result<Option<OwnedSemaphorePermit>> {
        // Go resets the logical count without invoking the old finisher.
        // Rust drops old values while retaining the allocation.
        self.requests.clear();
        let mut fetching = Fetching {
            controller: self,
            token: None,
            armed: true,
        };
        match fetching
            .collect(ctx, requests, tokens, max_batch_wait)
            .await
        {
            Ok(()) => {
                fetching.armed = false;
                Ok(fetching.token.take())
            }
            Err(error) => {
                drop(fetching.token.take());
                fetching.armed = false;
                fetching
                    .controller
                    .finish_collected_requests(None, Some(&error));
                Err(error)
            }
        }
    }

    /// Extends the current batch until a caller-owned timer fires, then drains
    /// ready requests. On error, completion and token return remain caller-owned.
    pub async fn fetch_requests_with_timer(
        &mut self,
        ctx: &Cancellation,
        requests: &mut impl RequestReceiver<T>,
        mut timer: Pin<&mut Sleep>,
    ) -> Result<()> {
        while self.requests.len() < self.max_batch_size {
            tokio::select! {
                _ = ctx.cancelled() => return Err(Error::ContextCanceled),
                request = requests.recv() => self.push_request(received(request)?),
                _ = timer.as_mut() => break,
            }
        }
        self.drain_ready(ctx, requests)
    }

    fn push_request(&mut self, request: T) {
        // Preserve the source buffer bound even for small maxima below the
        // initial best size. Real TSO/router callers use maxima well above 8.
        assert!(
            self.requests.len() <= self.max_batch_size,
            "batch buffer overflow"
        );
        self.requests.push(request);
    }

    fn drain_ready(
        &mut self,
        ctx: &Cancellation,
        requests: &mut impl RequestReceiver<T>,
    ) -> Result<()> {
        while self.requests.len() < self.max_batch_size {
            if ctx.is_cancelled() {
                return Err(Error::ContextCanceled);
            }
            match requests.try_recv() {
                Ok(request) => self.push_request(request),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return Err(channel_closed()),
            }
        }
        Ok(())
    }

    /// Visits requests in order until the callback returns false.
    pub fn iter_collected_requests(&self, mut visit: impl FnMut(&T) -> bool) {
        for request in &self.requests {
            if !visit(request) {
                break;
            }
        }
    }

    /// Borrows the collected requests without copying their payloads.
    pub fn get_collected_requests(&self) -> &[T] {
        &self.requests
    }

    /// Returns the source's logical collected request count.
    pub fn get_collected_request_count(&self) -> usize {
        self.requests.len()
    }

    /// Observes the current target, then adjusts it with the source AIAD rule.
    pub fn adjust_best_batch_size(&mut self) {
        if let Some(observer) = &self.best_batch_observer {
            observer.observe(self.best_batch_size as f64);
        }
        let length = self.requests.len();
        if length < self.best_batch_size && self.best_batch_size > 1 {
            self.best_batch_size -= 1;
        } else if length > self.best_batch_size + 4 && self.best_batch_size < self.max_batch_size {
            self.best_batch_size += 1;
        }
    }

    /// Finishes in index order, using the default callback if no override is
    /// supplied. The empty buffer remains allocated and cannot be finished twice.
    pub fn finish_collected_requests(
        &mut self,
        mut finisher: Option<&mut Finisher<'_, T>>,
        error: Option<&Error>,
    ) {
        for (index, request) in self.requests.drain(..).enumerate() {
            if let Some(finish) = finisher.as_mut() {
                finish(index, request, error);
            } else if let Some(finish) = self.finisher.as_mut() {
                finish(index, request, error);
            }
        }
    }

    /// None is Go's zero time, including the first token-late/full-batch return.
    pub fn get_extra_batching_start_time(&self) -> Option<Instant> {
        self.extra_batching_start_time
    }
}

fn channel_closed() -> Error {
    Error::StringError("PD batch request channel is closed".to_owned())
}

fn received<T>(request: Option<T>) -> Result<T> {
    request.ok_or_else(channel_closed)
}

struct Fetching<'a, T> {
    controller: &'a mut Controller<T>,
    token: Option<OwnedSemaphorePermit>,
    armed: bool,
}

impl<T> Drop for Fetching<'_, T> {
    fn drop(&mut self) {
        if self.armed {
            drop(self.token.take());
            self.controller
                .finish_collected_requests(None, Some(&Error::ContextCanceled));
        }
    }
}

impl<T> Fetching<'_, T> {
    async fn collect(
        &mut self,
        ctx: &Cancellation,
        requests: &mut impl RequestReceiver<T>,
        tokens: Option<&Arc<Semaphore>>,
        max_batch_wait: Duration,
    ) -> Result<()> {
        loop {
            if self.controller.requests.len() >= self.controller.max_batch_size
                && self.token.is_none()
            {
                if let Some(tokens) = tokens {
                    self.token = Some(tokio::select! {
                        _ = ctx.cancelled() => return Err(Error::ContextCanceled),
                        token = tokens.clone().acquire_owned() => token.map_err(|_| Error::ContextCanceled)?,
                    });
                }
                return Ok(());
            }
            if let Some(tokens) = tokens {
                tokio::select! {
                    _ = ctx.cancelled() => return Err(Error::ContextCanceled),
                    request = requests.recv() => {
                        self.controller.push_request(received(request)?);
                        continue;
                    }
                    token = tokens.clone().acquire_owned() => {
                        self.token = Some(token.map_err(|_| Error::ContextCanceled)?);
                    }
                }
            }
            if self.controller.requests.is_empty() {
                let first = tokio::select! {
                    _ = ctx.cancelled() => return Err(Error::ContextCanceled),
                    request = requests.recv() => received(request)?,
                };
                self.controller.push_request(first);
            }
            break;
        }

        let controller = &mut self.controller;
        controller.extra_batching_start_time = Some(Instant::now());
        controller.drain_ready(ctx, requests)?;
        if controller.requests.len() >= controller.max_batch_size || max_batch_wait.is_zero() {
            return Ok(());
        }
        if controller.requests.len() < controller.best_batch_size {
            let timer = tokio::time::sleep(max_batch_wait);
            tokio::pin!(timer);
            while controller.requests.len() < controller.best_batch_size {
                tokio::select! {
                    request = requests.recv() => controller.push_request(received(request)?),
                    _ = ctx.cancelled() => return Err(Error::ContextCanceled),
                    _ = &mut timer => return Ok(()),
                }
            }
        }
        controller.drain_ready(ctx, requests)
    }
}

#[cfg(test)]
#[path = "batch_tests.rs"]
mod tests;
