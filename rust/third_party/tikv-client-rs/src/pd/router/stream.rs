// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The `QueryRegion` streams the router client sends its batches on: PD's
//! `pdpb.PD_QueryRegionClient` and the router service's
//! `routerpb.Router_QueryRegionClient`, which carry the same messages.

use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::sync::{mpsc, Mutex};
use tonic::transport::Channel;

use crate::async_util::Cancellation;
use crate::proto::{pdpb, routerpb};
use crate::{Error, Result};

/// Which service a stream is opened on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StreamKind {
    /// `pdpb.PD/QueryRegion`.
    Pd,
    /// `routerpb.Router/QueryRegion`.
    Router,
}

type Responses = tonic::codec::Streaming<pdpb::QueryRegionResponse>;

enum Inbound {
    /// The call, opened by the first receive (tonic resolves a streaming
    /// call only when the server's headers arrive, which PD sends with its
    /// first response).
    Opening(BoxFuture<'static, std::result::Result<Responses, tonic::Status>>),
    Open(Box<Responses>),
    Ended,
}

/// A `QueryRegion` stream: `send` hands a request to the wire without
/// waiting, `recv` waits for the next response. Cancelling the stream's ctx
/// (its connection context's cancel) ends both.
pub(crate) struct QueryRegionStream {
    outbound: mpsc::UnboundedSender<pdpb::QueryRegionRequest>,
    inbound: Mutex<Inbound>,
    ctx: Cancellation,
}

impl QueryRegionStream {
    /// Go `pdpb.NewPDClient(cc).QueryRegion(cctx)` /
    /// `routerpb.NewRouterClient(conn).QueryRegion(cctx)`.
    ///
    /// grpc-go returns the stream once the connection's transport is ready,
    /// failing fast with `Unavailable` when it cannot connect. tonic exposes
    /// no connectivity state and its channel reports ready before
    /// connecting, so the transport is established by one round trip on the
    /// same channel -- the gRPC health check every PD and router server
    /// serves; any answer means the transport is up -- as the TSO stream
    /// builder does. `timeout` bounds that wait (grpc-go's bound is its
    /// connect attempt).
    pub(super) async fn open(
        kind: StreamKind,
        channel: Channel,
        ctx: &Cancellation,
        timeout: Duration,
    ) -> Result<Self> {
        let transport_ready = async {
            match crate::pd::service_discovery::health_check(channel.clone(), timeout).await {
                Ok(_) => Ok(()),
                Err(status) if status.code() == tonic::Code::Unavailable => Err(status),
                Err(status) if status.code() == tonic::Code::DeadlineExceeded => Err(status),
                Err(_) => Ok(()),
            }
        };
        tokio::select! {
            ready = transport_ready => ready.map_err(Error::GrpcAPI)?,
            _ = ctx.cancelled() => {
                return Err(Error::GrpcAPI(tonic::Status::cancelled("context canceled")));
            }
        }
        let (outbound, requests) = mpsc::unbounded_channel();
        let requests = futures::stream::unfold(requests, |mut requests| async move {
            requests.recv().await.map(|request| (request, requests))
        });
        let opening = match kind {
            StreamKind::Pd => async move {
                pdpb::pd_client::PdClient::new(channel)
                    .query_region(requests)
                    .await
                    .map(tonic::Response::into_inner)
            }
            .boxed(),
            StreamKind::Router => async move {
                routerpb::router_client::RouterClient::new(channel)
                    .query_region(requests)
                    .await
                    .map(tonic::Response::into_inner)
            }
            .boxed(),
        };
        Ok(Self {
            outbound,
            inbound: Mutex::new(Inbound::Opening(opening)),
            ctx: ctx.clone(),
        })
    }

    /// Go `stream.Send`.
    pub(super) fn send(&self, request: pdpb::QueryRegionRequest) -> Result<()> {
        if self.ctx.is_cancelled() {
            return Err(canceled());
        }
        self.outbound
            .send(request)
            .map_err(|_| Error::StringError("EOF".to_owned()))
    }

    /// Go `stream.Recv`.
    pub(super) async fn recv(&self) -> Result<pdpb::QueryRegionResponse> {
        let mut inbound = self.inbound.lock().await;
        let next = async {
            loop {
                match &mut *inbound {
                    Inbound::Opening(opening) => match opening.await {
                        Ok(responses) => *inbound = Inbound::Open(Box::new(responses)),
                        Err(status) => {
                            *inbound = Inbound::Ended;
                            return Err(Error::GrpcAPI(status));
                        }
                    },
                    Inbound::Open(responses) => {
                        return match responses.message().await {
                            Ok(Some(response)) => Ok(response),
                            Ok(None) => {
                                *inbound = Inbound::Ended;
                                Err(Error::StringError("EOF".to_owned()))
                            }
                            Err(status) => {
                                *inbound = Inbound::Ended;
                                Err(Error::GrpcAPI(status))
                            }
                        };
                    }
                    Inbound::Ended => return Err(Error::StringError("EOF".to_owned())),
                }
            }
        };
        tokio::select! {
            biased;
            _ = self.ctx.cancelled() => Err(canceled()),
            result = next => result,
        }
    }
}

fn canceled() -> Error {
    Error::GrpcAPI(tonic::Status::cancelled("context canceled"))
}
