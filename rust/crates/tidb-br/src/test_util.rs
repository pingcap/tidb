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

//! Test helpers: the Go master captures and a mock PD client (Go's tests
//! embed `pd.Client` in a mock the same way).

use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value as Json;
use tidb_gocontext::Context;
use tidb_proto::metapb;

use crate::errors::Result;
use crate::pdclient::PdClient;

pub mod cluster;

/// The records of `kind` in `testdata/conn_go.jsonl`.
pub fn go_records(kind: &str) -> Vec<Json> {
    include_str!("../testdata/conn_go.jsonl")
        .lines()
        .map(|l| serde_json::from_str::<Json>(l).unwrap())
        .filter(|r| r["kind"] == kind)
        .collect()
}

/// A PD client answering fixed stores and safe points.
#[derive(Default)]
pub struct MockPdClient {
    pub stores: Mutex<Vec<metapb::Store>>,
    pub gc_safe_point: Mutex<u64>,
    pub service_calls: Mutex<Vec<(String, i64, u64)>>,
    /// The TSO answer; `None` is `(1, 2)`, `Some((-1, _))` the wall clock.
    pub ts: Mutex<Option<(i64, i64)>>,
}

impl MockPdClient {
    pub fn with_stores(stores: Vec<metapb::Store>) -> Self {
        Self { stores: Mutex::new(stores), ..Self::default() }
    }
}

impl PdClient for MockPdClient {
    fn get_cluster_id(&self, _: &Context) -> u64 {
        1
    }
    fn get_all_stores(&self, _: &Context, _: bool) -> Result<Vec<metapb::Store>> {
        Ok(self.stores.lock().unwrap().clone())
    }
    fn get_ts(&self, _: &Context) -> Result<(i64, i64)> {
        match *self.ts.lock().unwrap() {
            None => Ok((1, 2)),
            Some((-1, l)) => {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
                Ok((now.as_millis() as i64, l))
            }
            Some(ts) => Ok(ts),
        }
    }
    fn update_gc_safe_point(&self, _: &Context, _: u64) -> Result<u64> {
        Ok(*self.gc_safe_point.lock().unwrap())
    }
    fn update_service_gc_safe_point(&self, _: &Context, id: &str, ttl: i64, sp: u64) -> Result<u64> {
        self.service_calls.lock().unwrap().push((id.to_owned(), ttl, sp));
        Ok(*self.gc_safe_point.lock().unwrap())
    }
    fn get_keyspace_gc_safe_point(&self, _: &Context, _: u32) -> Result<u64> {
        Ok(*self.gc_safe_point.lock().unwrap())
    }
    fn set_gc_barrier(&self, _: &Context, _: u32, id: &str, ts: u64, ttl: Duration) -> Result<(String, u64, Duration)> {
        self.service_calls.lock().unwrap().push((format!("barrier {id}"), ttl.as_secs() as i64, ts));
        Ok((id.to_owned(), ts, ttl))
    }
    fn delete_gc_barrier(&self, _: &Context, _: u32, id: &str) -> Result<()> {
        self.service_calls.lock().unwrap().push((format!("delete {id}"), 0, 0));
        Ok(())
    }
}
