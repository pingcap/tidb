// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Per-operation network traffic details collected by region-routed RPCs.
//!
//! This is the native async context counterpart of client-go's
//! `util.ExecDetails` traffic fields consumed by `internal/locate`.

use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Atomic traffic totals shared by every physical RPC in one operation.
#[derive(Debug, Default)]
pub struct NetworkTrafficDetails {
    sent_kv_total: AtomicI64,
    received_kv_total: AtomicI64,
    sent_kv_cross_zone: AtomicI64,
    received_kv_cross_zone: AtomicI64,
    sent_mpp_total: AtomicI64,
    received_mpp_total: AtomicI64,
    sent_mpp_cross_zone: AtomicI64,
    received_mpp_cross_zone: AtomicI64,
}

/// A consistent-enough source-style observation of independently atomic
/// traffic counters. Concurrent RPCs may advance later fields during a read,
/// exactly as with client-go's independent atomic loads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NetworkTrafficSnapshot {
    pub sent_kv_total: i64,
    pub received_kv_total: i64,
    pub sent_kv_cross_zone: i64,
    pub received_kv_cross_zone: i64,
    pub sent_mpp_total: i64,
    pub received_mpp_total: i64,
    pub sent_mpp_cross_zone: i64,
    pub received_mpp_cross_zone: i64,
}

impl NetworkTrafficDetails {
    pub fn snapshot(&self) -> NetworkTrafficSnapshot {
        NetworkTrafficSnapshot {
            sent_kv_total: self.sent_kv_total.load(Ordering::Relaxed),
            received_kv_total: self.received_kv_total.load(Ordering::Relaxed),
            sent_kv_cross_zone: self.sent_kv_cross_zone.load(Ordering::Relaxed),
            received_kv_cross_zone: self.received_kv_cross_zone.load(Ordering::Relaxed),
            sent_mpp_total: self.sent_mpp_total.load(Ordering::Relaxed),
            received_mpp_total: self.received_mpp_total.load(Ordering::Relaxed),
            sent_mpp_cross_zone: self.sent_mpp_cross_zone.load(Ordering::Relaxed),
            received_mpp_cross_zone: self.received_mpp_cross_zone.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn add_request(&self, size: i64, mpp: bool, cross_zone: bool) {
        let (total, cross_zone_total) = if mpp {
            (&self.sent_mpp_total, &self.sent_mpp_cross_zone)
        } else {
            (&self.sent_kv_total, &self.sent_kv_cross_zone)
        };
        total.fetch_add(size, Ordering::Relaxed);
        if cross_zone {
            cross_zone_total.fetch_add(size, Ordering::Relaxed);
        }
    }

    pub(crate) fn add_response(&self, size: i64, mpp: bool, cross_zone: bool) {
        let (total, cross_zone_total) = if mpp {
            (&self.received_mpp_total, &self.received_mpp_cross_zone)
        } else {
            (&self.received_kv_total, &self.received_kv_cross_zone)
        };
        total.fetch_add(size, Ordering::Relaxed);
        if cross_zone {
            cross_zone_total.fetch_add(size, Ordering::Relaxed);
        }
    }
}

tokio::task_local! {
    static NETWORK_TRAFFIC_DETAILS: Arc<NetworkTrafficDetails>;
}

/// Collect network details from all physical RPCs awaited by `future`.
pub async fn with_network_traffic_details<F>(
    details: Arc<NetworkTrafficDetails>,
    future: F,
) -> F::Output
where
    F: Future,
{
    NETWORK_TRAFFIC_DETAILS.scope(details, future).await
}

pub(crate) fn current_network_traffic_details() -> Option<Arc<NetworkTrafficDetails>> {
    NETWORK_TRAFFIC_DETAILS
        .try_with(Arc::<NetworkTrafficDetails>::clone)
        .ok()
}

pub(crate) struct NetworkCollector {
    pub(crate) stale_read: bool,
    pub(crate) access_location: crate::kv::AccessLocationType,
    pub(crate) endpoint_type: crate::store::EndpointType,
    pub(crate) details: Option<Arc<NetworkTrafficDetails>>,
}

impl NetworkCollector {
    pub(crate) fn on_request(&self, request: &dyn crate::store::Request) {
        let size = request.network_request_size();
        if size == 0 {
            return;
        }
        let cross_zone = self.access_location == crate::kv::AccessLocationType::CrossZone;
        if let Some(details) = &self.details {
            details.add_request(
                i64::try_from(size).unwrap_or(i64::MAX),
                self.endpoint_type == crate::store::EndpointType::TiFlash,
                cross_zone,
            );
        }
        if self.stale_read {
            crate::stats::observe_stale_read_request(size, cross_zone);
        }
    }

    pub(crate) fn on_response(
        &self,
        request: &dyn crate::store::Request,
        response: &dyn std::any::Any,
    ) {
        let size = crate::store::network_response_size(response);
        if size == 0 {
            return;
        }
        let cross_zone = self.access_location == crate::kv::AccessLocationType::CrossZone;
        if let Some(details) = &self.details {
            details.add_response(
                i64::try_from(size).unwrap_or(i64::MAX),
                self.endpoint_type == crate::store::EndpointType::TiFlash,
                cross_zone,
            );
        }
        if self.stale_read {
            crate::stats::observe_stale_read_response(size, cross_zone);
        }
        if request.is_network_read_request() {
            let follower = request
                .tikv_context()
                .is_some_and(|context| context.replica_read);
            crate::stats::observe_read_request_bytes(
                request.network_request_size().saturating_add(size),
                follower,
                self.access_location,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::AccessLocationType;
    use crate::proto::{kvrpcpb, mpp};
    use crate::store::{EndpointType, Request};

    fn collector(
        details: Arc<NetworkTrafficDetails>,
        stale_read: bool,
        access_location: AccessLocationType,
        endpoint_type: EndpointType,
    ) -> NetworkCollector {
        NetworkCollector {
            stale_read,
            access_location,
            endpoint_type,
            details: Some(details),
        }
    }

    #[test]
    fn source_network_collector_request_and_response_accounting() {
        let details = Arc::new(NetworkTrafficDetails::default());
        let ordinary = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                busy_threshold_ms: 50,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };
        let stale = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                stale_read: true,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };
        assert_eq!(ordinary.network_request_size(), 10);
        assert_eq!(stale.network_request_size(), 10);

        collector(
            details.clone(),
            false,
            AccessLocationType::LocalZone,
            EndpointType::TiKv,
        )
        .on_request(&ordinary);
        let stale_request_before = crate::stats::stale_read_request_count("local");
        let stale_out_before = crate::stats::stale_read_bytes("local", "out");
        collector(
            details.clone(),
            true,
            AccessLocationType::LocalZone,
            EndpointType::TiKv,
        )
        .on_request(&stale);
        assert_eq!(details.snapshot().sent_kv_total, 20);
        assert_eq!(details.snapshot().sent_kv_cross_zone, 0);
        assert_eq!(
            crate::stats::stale_read_request_count("local") - stale_request_before,
            1
        );
        assert_eq!(
            crate::stats::stale_read_bytes("local", "out") - stale_out_before,
            10
        );

        let ordinary_response = kvrpcpb::GetResponse {
            value: b"value".to_vec(),
            ..Default::default()
        };
        let stale_response = kvrpcpb::GetResponse {
            value: b"stale-value".to_vec(),
            ..Default::default()
        };
        assert_eq!(crate::store::network_response_size(&ordinary_response), 7);
        assert_eq!(crate::store::network_response_size(&stale_response), 13);
        collector(
            details.clone(),
            false,
            AccessLocationType::LocalZone,
            EndpointType::TiKv,
        )
        .on_response(&ordinary, &ordinary_response);
        let stale_in_before = crate::stats::stale_read_bytes("local", "in");
        collector(
            details.clone(),
            true,
            AccessLocationType::LocalZone,
            EndpointType::TiKv,
        )
        .on_response(&stale, &stale_response);
        assert_eq!(details.snapshot().received_kv_total, 20);
        assert_eq!(details.snapshot().received_kv_cross_zone, 0);
        assert_eq!(
            crate::stats::stale_read_bytes("local", "in") - stale_in_before,
            13
        );
    }

    #[test]
    fn source_network_collector_cross_zone_mpp_and_replica_metrics() {
        let details = Arc::new(NetworkTrafficDetails::default());
        let mpp_request = mpp::DispatchTaskRequest {
            encoded_plan: vec![1, 2],
            ..Default::default()
        };
        let mpp_response = mpp::DispatchTaskResponse {
            error: Some(mpp::Error {
                msg: "retry".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mpp = collector(
            details.clone(),
            false,
            AccessLocationType::CrossZone,
            EndpointType::TiFlash,
        );
        mpp.on_request(&mpp_request);
        mpp.on_response(&mpp_request, &mpp_response);
        let snapshot = details.snapshot();
        assert_eq!(
            snapshot.sent_mpp_total,
            mpp_request.network_request_size() as i64
        );
        assert_eq!(snapshot.sent_mpp_cross_zone, snapshot.sent_mpp_total);
        assert_eq!(
            snapshot.received_mpp_total,
            crate::store::network_response_size(&mpp_response) as i64
        );
        assert_eq!(
            snapshot.received_mpp_cross_zone,
            snapshot.received_mpp_total
        );

        let follower = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                replica_read: true,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };
        let response = kvrpcpb::GetResponse {
            value: b"value".to_vec(),
            ..Default::default()
        };
        let before = crate::stats::read_request_bytes_samples("follower", "cross-zone");
        collector(
            details,
            false,
            AccessLocationType::CrossZone,
            EndpointType::TiKv,
        )
        .on_response(&follower, &response);
        let after = crate::stats::read_request_bytes_samples("follower", "cross-zone");
        assert_eq!(after.0 - before.0, 1);
        assert_eq!(
            after.1 - before.1,
            (follower.network_request_size() + crate::store::network_response_size(&response))
                as f64
        );

        assert_eq!(kvrpcpb::RawGetRequest::default().network_request_size(), 0);
        assert_eq!(
            crate::store::network_response_size(&kvrpcpb::RawGetResponse::default()),
            0
        );
    }
}
