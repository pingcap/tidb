// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `clients/router/request.go`.

use std::time::Instant;

use tokio::sync::oneshot;

use super::Region;
use crate::async_util::Cancellation;
use crate::pd::opt::GetRegionOp;
use crate::{Error, Result};

/// Go `router.Request`: one region query and the caller's result slot.
pub(crate) struct Request {
    /// Go `key`: set for a `GetRegion` request.
    pub(super) key: Option<Vec<u8>>,
    /// Go `prevKey`: set for a `GetPrevRegion` request.
    pub(super) prev_key: Option<Vec<u8>>,
    /// Go `id`: set for a `GetRegionByID` request.
    pub(super) id: u64,
    pub(super) options: GetRegionOp,
    done: Option<oneshot::Sender<Result<Option<Region>>>>,
}

/// The waiting half of a [`Request`] (Go `req.wait()`'s side).
pub(super) struct RequestWait {
    receiver: oneshot::Receiver<Result<Option<Region>>>,
    /// Go `start`.
    start: Instant,
}

impl Request {
    /// Go `newRequest`, without the pool.
    pub(super) fn new(options: GetRegionOp) -> (Self, RequestWait) {
        let (done, receiver) = oneshot::channel();
        (
            Self {
                key: None,
                prev_key: None,
                id: 0,
                options,
                done: Some(done),
            },
            RequestWait {
                receiver,
                start: Instant::now(),
            },
        )
    }

    /// Go `tryDone`: a second result is dropped.
    pub(super) fn try_done(&mut self, result: Result<Option<Region>>) {
        if let Some(done) = self.done.take() {
            let _ = done.send(result);
        }
    }
}

impl RequestWait {
    /// Go `req.wait()`. `deadline` is the request ctx's (Go `requestCtx`,
    /// which `client.GetRegion` bounds by the client timeout); `client_ctx`
    /// is the router client's (Go `clientCtx`).
    pub(super) async fn wait(
        self,
        deadline: tokio::time::Instant,
        client_ctx: &Cancellation,
    ) -> Result<Option<Region>> {
        let metrics = crate::pd::metrics::global_metrics();
        let start = Instant::now();
        metrics
            .cmd_duration_query_region_async_wait
            .observe(start.duration_since(self.start).as_secs_f64());
        // A request the dispatcher drops unfinished (it exited) leaves the
        // waiter to its ctxs, as Go's never-closed `done` channel does.
        let done = async {
            match self.receiver.await {
                Ok(result) => result,
                Err(_) => std::future::pending().await,
            }
        };
        tokio::select! {
            result = done => {
                let now = Instant::now();
                match result {
                    Err(error) => {
                        metrics
                            .cmd_failed_duration_query_region_wait
                            .observe(now.duration_since(start).as_secs_f64());
                        metrics
                            .cmd_failed_duration_query_region
                            .observe(now.duration_since(self.start).as_secs_f64());
                        Err(error)
                    }
                    Ok(region) => {
                        metrics
                            .cmd_duration_query_region_wait
                            .observe(now.duration_since(start).as_secs_f64());
                        metrics
                            .cmd_duration_query_region
                            .observe(now.duration_since(self.start).as_secs_f64());
                        Ok(region)
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => Err(Error::GrpcAPI(
                tonic::Status::deadline_exceeded("context deadline exceeded"),
            )),
            _ = client_ctx.cancelled() => Err(Error::ContextCanceled),
        }
    }
}
