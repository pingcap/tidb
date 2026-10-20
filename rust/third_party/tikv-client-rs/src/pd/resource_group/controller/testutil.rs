// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `testutil.go`: request and response info implementations for tests.

use std::time::Duration;

use super::model::{AccessLocationType, RequestInfo, ResponseInfo};

/// A request info for tests.
#[derive(Clone, Debug, Default)]
pub struct TestRequestInfo {
    pub(crate) is_write: bool,
    pub(crate) write_bytes: u64,
    pub(crate) num_replicas: i64,
    pub(crate) store_id: u64,
    pub(crate) access_type: AccessLocationType,
    pub(crate) request_source: String,
    pub(crate) predicted_read_bytes: u64,
    pub(crate) is_cop: bool,
}

/// Go `NewTestRequestInfo`.
pub fn new_test_request_info(
    is_write: bool,
    write_bytes: u64,
    store_id: u64,
    location_type: AccessLocationType,
) -> TestRequestInfo {
    TestRequestInfo {
        is_write,
        write_bytes,
        num_replicas: 1,
        store_id,
        access_type: location_type,
        ..TestRequestInfo::default()
    }
}

impl RequestInfo for TestRequestInfo {
    fn is_write(&self) -> bool {
        self.is_write
    }
    fn write_bytes(&self) -> u64 {
        self.write_bytes
    }
    fn replica_number(&self) -> i64 {
        self.num_replicas
    }
    fn store_id(&self) -> u64 {
        self.store_id
    }
    /// The written bytes stand in for the request size.
    fn request_size(&self) -> u64 {
        self.write_bytes
    }
    fn access_location_type(&self) -> AccessLocationType {
        self.access_type
    }
    fn request_source(&self) -> &str {
        &self.request_source
    }
    fn predicted_read_bytes(&self) -> u64 {
        self.predicted_read_bytes
    }
    fn is_cop(&self) -> bool {
        self.is_cop
    }
}

/// A response info for tests.
#[derive(Clone, Debug, Default)]
pub struct TestResponseInfo {
    pub(crate) read_bytes: u64,
    pub(crate) kv_cpu: Duration,
    pub(crate) succeed: bool,
}

/// Go `NewTestResponseInfo`.
pub fn new_test_response_info(
    read_bytes: u64,
    kv_cpu: Duration,
    succeed: bool,
) -> TestResponseInfo {
    TestResponseInfo {
        read_bytes,
        kv_cpu,
        succeed,
    }
}

impl ResponseInfo for TestResponseInfo {
    fn read_bytes(&self) -> u64 {
        self.read_bytes
    }
    fn kv_cpu(&self) -> Duration {
        self.kv_cpu
    }
    fn succeed(&self) -> bool {
        self.succeed
    }
    /// The read bytes stand in for the response size.
    fn response_size(&self) -> u64 {
        self.read_bytes
    }
}
