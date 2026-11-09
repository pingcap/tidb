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


//! Go `pkg/dxf/framework/metering`: what each task reads from and writes to
//! object storage and the cluster (`recorder.go`, `data.go`).
//!
//! Go writes these records through its `Meter` (`metering.go`, the external
//! `metering_sdk`) on a next-gen kernel only: on a classic kernel
//! `RegisterRecorder` hands out an unregistered recorder and
//! `UnregisterRecorder` / `WriteMeterData` do nothing. This port's kernel is
//! classic (the next-gen runtime is register row O15), so the `Meter` itself
//! is not ported.

use std::collections::BTreeMap;

use tidb_dxf::TaskBase;
use tidb_objstore_recording::{AccessStats, Traffic};

/// Go `category`: the metering source name.
const CATEGORY: &str = "dxf";

const GET_REQUESTS_FIELD: &str = "get_requests";
const PUT_REQUESTS_FIELD: &str = "put_requests";
const OBJ_STORE_READ_BYTES_FIELD: &str = "obj_store_read_bytes";
const OBJ_STORE_WRITE_BYTES_FIELD: &str = "obj_store_write_bytes";
const CLUSTER_READ_BYTES_FIELD: &str = "cluster_read_bytes";
const CLUSTER_WRITE_BYTES_FIELD: &str = "cluster_write_bytes";

/// Go `RowCountField`: the number of rows processed.
pub const ROW_COUNT_FIELD: &str = "row_count";
/// Go `DataKVBytesField`: the data KV bytes ingested into the cluster.
pub const DATA_KV_BYTES_FIELD: &str = "data_kv_bytes";
/// Go `IndexKVBytesField`: the index KV bytes ingested into the cluster.
pub const INDEX_KV_BYTES_FIELD: &str = "index_kv_bytes";
/// Go `RequiredSlotsField`: the task's required slots.
pub const REQUIRED_SLOTS_FIELD: &str = "required_slots";
/// Go `MaxNodeCountField`: the most nodes the task used.
pub const MAX_NODE_COUNT_FIELD: &str = "max_node_count";
/// Go `DurationSecondsField`: the task's duration in seconds.
pub const DURATION_SECONDS_FIELD: &str = "duration_seconds";

/// Go's `map[string]any` metering item.
pub type MeterItem = BTreeMap<String, serde_json::Value>;

/// Go `dataValues`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DataValues {
    get_requests: u64,
    put_requests: u64,
    obj_store_read_bytes: u64,
    obj_store_write_bytes: u64,
    cluster_read_bytes: u64,
    cluster_write_bytes: u64,
}

/// Go `Data`: a task's accumulated metering data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Data {
    values: DataValues,
    task_id: i64,
    keyspace: String,
    task_type: String,
}

impl Data {
    /// Go `equals`: the data fields alone, not the task.
    fn equals(&self, other: &Self) -> bool {
        self.values == other.values
    }

    /// Go `calMeterDataItem`: the growth since `other`, `None` when there is
    /// none (the data only grows).
    fn cal_meter_data_item(&self, other: &Self) -> Option<MeterItem> {
        if self.equals(other) {
            return None;
        }
        let mut item = get_base_meter_item(self.task_id, &self.keyspace, &self.task_type);
        let (current, previous) = (&self.values, &other.values);
        for (field, diff) in [
            (GET_REQUESTS_FIELD, current.get_requests.wrapping_sub(previous.get_requests)),
            (PUT_REQUESTS_FIELD, current.put_requests.wrapping_sub(previous.put_requests)),
            (OBJ_STORE_READ_BYTES_FIELD, current.obj_store_read_bytes.wrapping_sub(previous.obj_store_read_bytes)),
            (OBJ_STORE_WRITE_BYTES_FIELD, current.obj_store_write_bytes.wrapping_sub(previous.obj_store_write_bytes)),
            (CLUSTER_READ_BYTES_FIELD, current.cluster_read_bytes.wrapping_sub(previous.cluster_read_bytes)),
            (CLUSTER_WRITE_BYTES_FIELD, current.cluster_write_bytes.wrapping_sub(previous.cluster_write_bytes)),
        ] {
            if diff > 0 {
                item.insert(field.to_owned(), diff.into());
            }
        }
        Some(item)
    }
}

impl std::fmt::Display for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let size = |bytes: u64| tidb_config_configtypes::bytes_size(bytes as f64);
        write!(
            f,
            "{{id: {}, keyspace: {}, type: {}, requests{{get: {}, put: {}}}, obj_store{{r: {}, w: {}}}, cluster{{r: {}, w: {}}}",
            self.task_id,
            self.keyspace,
            self.task_type,
            self.values.get_requests,
            self.values.put_requests,
            size(self.values.obj_store_read_bytes),
            size(self.values.obj_store_write_bytes),
            size(self.values.cluster_read_bytes),
            size(self.values.cluster_write_bytes),
        )
    }
}

/// Go `GetBaseMeterItem`: the fields every metering item carries (on a
/// next-gen kernel the keyspace name is the cluster id).
#[must_use]
pub fn get_base_meter_item(task_id: i64, keyspace: &str, task_type: &str) -> MeterItem {
    MeterItem::from([
        ("version".to_owned(), "1".into()),
        ("source_name".to_owned(), CATEGORY.into()),
        ("task_id".to_owned(), task_id.into()),
        ("cluster_id".to_owned(), keyspace.into()),
        ("task_type".to_owned(), task_type.into()),
    ])
}

/// Go `Recorder`: records a task's metering data.
#[derive(Debug, Default)]
pub struct Recorder {
    task_id: i64,
    keyspace: String,
    task_type: String,
    obj_store_access: AccessStats,
    cluster_traffic: Traffic,
}

impl Recorder {
    /// Go `MergeObjStoreAccess`.
    pub fn merge_obj_store_access(&self, other: &AccessStats) {
        self.obj_store_access.merge(other);
    }

    /// Go `IncClusterReadBytes`.
    pub fn inc_cluster_read_bytes(&self, n: u64) {
        self.cluster_traffic.read.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
    }

    /// Go `IncClusterWriteBytes`.
    pub fn inc_cluster_write_bytes(&self, n: u64) {
        self.cluster_traffic.write.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
    }

    /// Go `currData`.
    fn curr_data(&self) -> Data {
        let load = |value: &std::sync::atomic::AtomicU64| value.load(std::sync::atomic::Ordering::SeqCst);
        Data {
            task_id: self.task_id,
            keyspace: self.keyspace.clone(),
            task_type: self.task_type.clone(),
            values: DataValues {
                get_requests: load(&self.obj_store_access.requests.get),
                put_requests: load(&self.obj_store_access.requests.put),
                obj_store_read_bytes: load(&self.obj_store_access.traffic.read),
                obj_store_write_bytes: load(&self.obj_store_access.traffic.write),
                cluster_read_bytes: load(&self.cluster_traffic.read),
                cluster_write_bytes: load(&self.cluster_traffic.write),
            },
        }
    }
}

/// Go `RegisterRecorder`: on a classic kernel, a recorder no meter reads.
#[must_use]
pub fn register_recorder(_task: &TaskBase) -> std::sync::Arc<Recorder> {
    std::sync::Arc::new(Recorder::default())
}

/// Go `UnregisterRecorder`: nothing to do on a classic kernel.
pub fn unregister_recorder(_task_id: i64) {}

/// Go `WriteMeterData`: nothing to write on a classic kernel.
///
/// # Errors
/// Never on a classic kernel.
pub fn write_meter_data(_ts: i64, _uuid: &str, _items: &[MeterItem]) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(task_id: i64, values: DataValues) -> Data {
        Data { task_id, values, ..Data::default() }
    }

    /// Go `TestDataEquals`.
    #[test]
    fn data_equals() {
        let v = |f: fn(&mut DataValues, u64), n: u64| {
            let mut values = DataValues::default();
            f(&mut values, n);
            values
        };
        let get = |values: &mut DataValues, n| values.get_requests = n;
        let put = |values: &mut DataValues, n| values.put_requests = n;
        let read = |values: &mut DataValues, n| values.obj_store_read_bytes = n;
        let write = |values: &mut DataValues, n| values.obj_store_write_bytes = n;
        let cluster_read = |values: &mut DataValues, n| values.cluster_read_bytes = n;
        let cluster_write = |values: &mut DataValues, n| values.cluster_write_bytes = n;
        let cases = [
            ((data(1, v(get, 1)), data(1, v(get, 1))), true),
            ((data(1, v(get, 1)), data(1, v(get, 2))), false),
            ((data(1, v(put, 1)), data(1, v(put, 1))), true),
            ((data(1, v(put, 1)), data(1, v(put, 2))), false),
            ((data(1, v(read, 1)), data(1, v(read, 2))), false),
            ((data(1, v(write, 1)), data(1, v(write, 2))), false),
            ((data(1, v(cluster_read, 1)), data(1, v(cluster_read, 2))), false),
            ((data(1, v(cluster_write, 1)), data(1, v(cluster_write, 2))), false),
            // we only compare the data fields, not taskID
            ((data(1, v(get, 1)), data(2, v(get, 1))), true),
        ];
        for (i, ((left, right), equal)) in cases.into_iter().enumerate() {
            assert_eq!(equal, left.equals(&right), "case {i} failed");
        }
    }

    /// Go `TestDataCalMeterDataItem`.
    #[test]
    fn data_cal_meter_data_item() {
        let curr = Data {
            values: DataValues {
                get_requests: 10,
                put_requests: 20,
                obj_store_read_bytes: 300,
                obj_store_write_bytes: 400,
                cluster_read_bytes: 500,
                cluster_write_bytes: 600,
            },
            task_id: 1,
            keyspace: "ks".to_owned(),
            task_type: "tt".to_owned(),
        };
        assert_eq!(curr.cal_meter_data_item(&curr), None);
        let item = |fields: &[(&str, u64)]| {
            let mut item = MeterItem::from([
                ("version".to_owned(), "1".into()),
                ("cluster_id".to_owned(), "ks".into()),
                ("source_name".to_owned(), CATEGORY.into()),
                ("task_type".to_owned(), "tt".into()),
                ("task_id".to_owned(), 1_i64.into()),
            ]);
            for (field, value) in fields {
                item.insert((*field).to_owned(), (*value).into());
            }
            item
        };
        assert_eq!(
            curr.cal_meter_data_item(&data(
                0,
                DataValues {
                    get_requests: 5,
                    put_requests: 5,
                    obj_store_read_bytes: 100,
                    obj_store_write_bytes: 100,
                    cluster_read_bytes: 200,
                    cluster_write_bytes: 200,
                },
            )),
            Some(item(&[
                ("get_requests", 5),
                ("put_requests", 15),
                ("obj_store_read_bytes", 200),
                ("obj_store_write_bytes", 300),
                ("cluster_read_bytes", 300),
                ("cluster_write_bytes", 400),
            ]))
        );
        assert_eq!(
            curr.cal_meter_data_item(&data(
                0,
                DataValues {
                    get_requests: 5,
                    put_requests: 20,
                    obj_store_read_bytes: 300,
                    obj_store_write_bytes: 400,
                    cluster_read_bytes: 500,
                    cluster_write_bytes: 600,
                },
            )),
            Some(item(&[("get_requests", 5)]))
        );
    }

    /// Go `TestRecorder`.
    #[test]
    fn recorder() {
        let r = Recorder {
            task_id: 1,
            keyspace: "ks".to_owned(),
            task_type: "tt".to_owned(),
            ..Recorder::default()
        };
        r.obj_store_access.requests.get.fetch_add(100, std::sync::atomic::Ordering::SeqCst);
        r.obj_store_access.requests.put.fetch_add(200, std::sync::atomic::Ordering::SeqCst);
        r.obj_store_access.rec_read(11);
        r.obj_store_access.rec_write(22);
        r.inc_cluster_read_bytes(300);
        r.inc_cluster_write_bytes(400);
        assert_eq!(
            Data {
                task_id: 1,
                keyspace: "ks".to_owned(),
                task_type: "tt".to_owned(),
                values: DataValues {
                    get_requests: 100,
                    put_requests: 200,
                    obj_store_read_bytes: 11,
                    obj_store_write_bytes: 22,
                    cluster_read_bytes: 300,
                    cluster_write_bytes: 400,
                },
            },
            r.curr_data()
        );
    }
}
