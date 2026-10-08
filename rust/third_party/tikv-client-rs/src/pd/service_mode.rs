// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD's cluster-wide mode observation is independent of TSO group discovery.

use crate::proto::pdpb;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::{watch, Mutex};
use tonic::{transport::Channel, Request, Status};

/// Accepted mode facts and serialized observations for one PD client lifetime.
/// Group lookups borrow snapshots and never hold the observation lock.
#[derive(Clone, Debug)]
pub struct ServiceModeDiscovery {
    accepted: watch::Sender<Option<pdpb::GetClusterInfoResponse>>,
    observation: Arc<Mutex<()>>,
}

impl Default for ServiceModeDiscovery {
    fn default() -> Self {
        Self {
            accepted: watch::channel(None).0,
            observation: Arc::default(),
        }
    }
}

impl ServiceModeDiscovery {
    /// Last valid cluster-wide observation; failures never replace it.
    pub fn snapshot(&self) -> Option<pdpb::GetClusterInfoResponse> {
        self.accepted.borrow().clone()
    }

    /// Changes invalidate in-flight group candidates without waiting for their RPC.
    pub fn subscribe(&self) -> watch::Receiver<Option<pdpb::GetClusterInfoResponse>> {
        self.accepted.subscribe()
    }

    /// A failed observation retains accepted facts but remains an error to the
    /// caller, which schedules the independent membership check as Go does.
    pub async fn refresh<F, Fut>(
        &self,
        leader: &str,
        timeout: Duration,
        dial: F,
    ) -> Result<(), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        tokio::time::timeout(timeout, async {
            let _observation = self.observation.lock().await;
            let channel = dial(leader.to_owned()).await?;
            let mut request = Request::new(pdpb::GetClusterInfoRequest::default());
            request.set_timeout(timeout);
            let info = match pdpb::pd_client::PdClient::new(channel)
                .get_cluster_info(request)
                .await
            {
                Ok(response) => {
                    let info = response.into_inner();
                    if let Some(error) = info
                        .header
                        .as_ref()
                        .and_then(|header| header.error.as_ref())
                    {
                        return Err(Status::unknown(error.message.clone()));
                    }
                    if info
                        .service_modes
                        .first()
                        .copied()
                        .and_then(|mode| pdpb::ServiceMode::try_from(mode).ok())
                        .is_none_or(|mode| {
                            !matches!(
                                mode,
                                pdpb::ServiceMode::PdSvcMode | pdpb::ServiceMode::ApiSvcMode
                            )
                        })
                    {
                        return Err(Status::unknown("no supported service mode returned"));
                    }
                    info
                }
                Err(error) if error.code() == tonic::Code::Unimplemented => {
                    pdpb::GetClusterInfoResponse {
                        service_modes: vec![pdpb::ServiceMode::PdSvcMode as i32],
                        ..Default::default()
                    }
                }
                Err(error) => return Err(error),
            };
            self.accepted.send_if_modified(|accepted| {
                if accepted.as_ref() == Some(&info) {
                    return false;
                }
                *accepted = Some(info);
                true
            });
            Ok(())
        })
        .await
        .map_err(|_| Status::deadline_exceeded("PD service mode observation timed out"))?
    }
}
