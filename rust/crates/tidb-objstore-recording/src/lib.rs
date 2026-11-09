// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


//! Go `pkg/objstore/recording`: the requests and bytes a component made to
//! object storage.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Go `Requests`: the number of requests made to object storage.
#[derive(Debug, Default)]
pub struct Requests {
    /// Go `Get`.
    pub get: AtomicU64,
    /// Go `Put`.
    pub put: AtomicU64,
}

impl Requests {
    /// Go `rec`: a GET or HEAD reads, a PUT or POST writes; `None` is Go's
    /// nil request.
    fn rec(&self, http_method: Option<&str>) {
        match http_method {
            Some("GET" | "HEAD") => {
                self.get.fetch_add(1, Ordering::SeqCst);
            }
            Some("PUT" | "POST") => {
                self.put.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    /// Go `Merge`.
    pub fn merge(&self, other: &Self) {
        self.get.fetch_add(other.get.load(Ordering::SeqCst), Ordering::SeqCst);
        self.put.fetch_add(other.put.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

impl fmt::Display for Requests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{get: {}, put: {}}}", self.get.load(Ordering::SeqCst), self.put.load(Ordering::SeqCst))
    }
}

/// Go `Traffic`: the bytes read from and written to object storage.
#[derive(Debug, Default)]
pub struct Traffic {
    /// Go `Read`.
    pub read: AtomicU64,
    /// Go `Write`.
    pub write: AtomicU64,
}

impl fmt::Display for Traffic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{r: {}, w: {}}}", self.read.load(Ordering::SeqCst), self.write.load(Ordering::SeqCst))
    }
}

/// Go `AccessStats`: object storage access statistics.
#[derive(Debug, Default)]
pub struct AccessStats {
    /// Go `Requests`.
    pub requests: Requests,
    /// Go `Traffic`.
    pub traffic: Traffic,
}

impl AccessStats {
    /// Go `Merge`.
    pub fn merge(&self, other: &Self) {
        self.requests.merge(&other.requests);
        self.traffic.read.fetch_add(other.traffic.read.load(Ordering::SeqCst), Ordering::SeqCst);
        self.traffic.write.fetch_add(other.traffic.write.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Go `RecRequest`, given the request's HTTP method.
    pub fn rec_request(&self, http_method: Option<&str>) {
        self.requests.rec(http_method);
    }

    /// Go `RecRead`.
    pub fn rec_read(&self, n: usize) {
        self.traffic.read.fetch_add(n as u64, Ordering::SeqCst);
    }

    /// Go `RecWrite`.
    pub fn rec_write(&self, n: usize) {
        self.traffic.write.fetch_add(n as u64, Ordering::SeqCst);
    }
}

impl fmt::Display for AccessStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{requests: {}, traffic: {}}}", self.requests, self.traffic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestRequestsRecording`.
    #[test]
    fn requests_recording() {
        let r = Requests::default();
        let check_val = |get: u64, put: u64| {
            assert_eq!(get, r.get.load(Ordering::SeqCst));
            assert_eq!(put, r.put.load(Ordering::SeqCst));
        };
        r.rec(None);
        check_val(0, 0);
        r.rec(Some("GET"));
        check_val(1, 0);
        r.rec(Some("HEAD"));
        check_val(2, 0);
        r.rec(Some("PUT"));
        check_val(2, 1);
        r.rec(Some("POST"));
        check_val(2, 2);
        // not recorded now
        r.rec(Some("DELETE"));
        check_val(2, 2);
    }

    /// Go's `String` forms and `Merge`.
    #[test]
    fn access_stats_print_and_merge_like_go() {
        let stats = AccessStats::default();
        stats.rec_request(Some("GET"));
        stats.rec_request(Some("PUT"));
        stats.rec_read(10);
        stats.rec_write(20);
        assert_eq!(stats.to_string(), "{requests: {get: 1, put: 1}, traffic: {r: 10, w: 20}}");
        let other = AccessStats::default();
        other.merge(&stats);
        other.merge(&stats);
        assert_eq!(other.to_string(), "{requests: {get: 2, put: 2}, traffic: {r: 20, w: 40}}");
    }
}
