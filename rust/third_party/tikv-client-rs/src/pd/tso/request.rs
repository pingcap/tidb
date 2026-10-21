// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `clients/tso/request.go`.

use std::time::Instant;

use tokio::sync::oneshot;

use crate::async_util::Cancellation;
use crate::proto::pdpb::Timestamp;
use crate::{Error, Result};

/// Go `tso.Request`: the caller's result slot and its ctx (`requestCtx`,
/// whose client span the batch reports to).
pub(crate) struct Request {
    done: Option<oneshot::Sender<Result<Timestamp>>>,
    pub(crate) client_spans: Option<crate::trace::ClientSpanContext>,
    /// Go `requestCtx`: the caller stopped waiting.
    pub(crate) request_ctx: Cancellation,
    /// Go `start`.
    start: Instant,
}

/// Go `TSFuture`: the waiting half of a [`Request`].
pub(crate) struct TsFuture {
    receiver: oneshot::Receiver<Result<Timestamp>>,
    request_ctx: Cancellation,
    start: Instant,
}

impl Request {
    /// Go `GetTSORequest`, without the pool.
    pub(crate) fn new() -> (Self, TsFuture) {
        let (done, receiver) = oneshot::channel();
        let start = Instant::now();
        let request_ctx = Cancellation::default();
        (
            Self {
                done: Some(done),
                client_spans: crate::trace::current_client_span_context(),
                request_ctx: request_ctx.clone(),
                start,
            },
            TsFuture {
                receiver,
                request_ctx,
                start,
            },
        )
    }

    /// Go `TryDone`: a second result is dropped.
    pub(crate) fn try_done(&mut self, result: Result<Timestamp>) {
        if let Some(done) = self.done.take() {
            let _ = done.send(result);
        }
    }
}

impl TsFuture {
    /// Go `Request.Wait` (`waitCtx(requestCtx)`). `client_ctx` is the TSO
    /// client's ctx (Go `clientCtx`); a result already delivered wins over a
    /// closed client.
    pub(crate) async fn wait(self, client_ctx: &Cancellation) -> Result<Timestamp> {
        let metrics = crate::pd::metrics::global_metrics();
        let start = Instant::now();
        metrics
            .cmd_duration_tso_async_wait
            .observe(start.duration_since(self.start).as_secs_f64());
        let _cancel_on_drop = CancelOnDrop(self.request_ctx);
        let result = tokio::select! {
            biased;
            result = self.receiver => result.unwrap_or_else(|_| {
                Err(crate::pd::errs::ERR_CLOSING.error().with_stack().into())
            }),
            _ = client_ctx.cancelled() => return Err(Error::ContextCanceled),
        };
        let now = Instant::now();
        match &result {
            Ok(_) => {
                metrics
                    .cmd_duration_tso_wait
                    .observe(now.duration_since(start).as_secs_f64());
                metrics
                    .cmd_duration_tso
                    .observe(now.duration_since(self.start).as_secs_f64());
            }
            Err(_) => {
                metrics
                    .cmd_failed_duration_tso_wait
                    .observe(now.duration_since(start).as_secs_f64());
                metrics
                    .cmd_failed_duration_tso
                    .observe(now.duration_since(self.start).as_secs_f64());
            }
        }
        result
    }
}

/// A waiter that goes away is Go's cancelled `requestCtx`.
struct CancelOnDrop(Cancellation);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
