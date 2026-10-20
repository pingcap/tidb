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

//! Go `load_info.go`.

use std::collections::HashMap;
use std::time::Duration;

use tidb_proto::diagnosticspb as pb;

use crate::{fmt2, host, item};

/// Go `getCpuLoad`: the load averages, then the CPU time split over one
/// second.
fn get_cpu_load() -> Vec<pb::ServerInfoItem> {
    let mut results = Vec::new();
    if let Ok(avgload) = host::load_avg() {
        results.push(item(
            "cpu",
            "cpu",
            vec![
                ("load1", fmt2(avgload.load1)),
                ("load5", fmt2(avgload.load5)),
                ("load15", fmt2(avgload.load15)),
            ],
        ));
    }
    let Ok(t1s) = host::cpu_times() else {
        return results;
    };
    std::thread::sleep(Duration::from_secs(1));
    let t2s = host::cpu_times();
    let (t1, t2) = match (t1s.as_slice(), t2s.as_deref()) {
        ([t1], Ok([t2])) => (t1, t2),
        _ => return results,
    };
    let total = t2.total() - t1.total();
    results.push(item(
        "cpu",
        "usage",
        vec![
            ("user", fmt2((t2.user - t1.user) / total)),
            ("system", fmt2((t2.system - t1.system) / total)),
            ("idle", fmt2((t2.idle - t1.idle) / total)),
            ("nice", fmt2((t2.nice - t1.nice) / total)),
            ("iowait", fmt2((t2.iowait - t1.iowait) / total)),
            ("irq", fmt2((t2.irq - t1.irq) / total)),
            ("softirq", fmt2((t2.softirq - t1.softirq) / total)),
            ("steal", fmt2((t2.steal - t1.steal) / total)),
            ("guest", fmt2((t2.guest - t1.guest) / total)),
            ("guest_nice", fmt2((t2.guest_nice - t1.guest_nice) / total)),
        ],
    ));
    results
}

/// Go `getMemLoad`.
fn get_mem_load() -> Vec<pb::ServerInfoItem> {
    let mut results = Vec::new();
    if let Ok(virt) = host::virtual_memory() {
        let mem_used = virt.total.wrapping_sub(virt.available);
        let mem_used_percent = mem_used as f64 / virt.total as f64;
        results.push(item(
            "memory",
            "virtual",
            vec![
                ("total", virt.total.to_string()),
                ("used", mem_used.to_string()),
                ("free", virt.available.to_string()),
                ("used-percent", fmt2(mem_used_percent)),
                ("free-percent", fmt2(1.0 - mem_used_percent)),
            ],
        ));
    }
    if let Ok(swap) = host::swap_memory() {
        results.push(item(
            "memory",
            "swap",
            vec![
                ("total", swap.total.to_string()),
                ("used", swap.used.to_string()),
                ("free", swap.free.to_string()),
                ("used-percent", fmt2(swap.used as f64 / swap.total as f64)),
                ("free-percent", fmt2(swap.free as f64 / swap.total as f64)),
            ],
        ));
    }
    results
}

/// Go `getNICLoad`. `bytes-ent` is Go's own key spelling.
fn get_nic_load() -> Vec<pb::ServerInfoItem> {
    let Ok(ics) = host::net_io_counters() else {
        return Vec::new();
    };
    ics.into_iter()
        .map(|ic| {
            item(
                "net",
                &ic.name,
                vec![
                    ("bytes-ent", ic.bytes_sent.to_string()),
                    ("bytes-recv", ic.bytes_recv.to_string()),
                    ("packets-sent", ic.packets_sent.to_string()),
                    ("packets-recv", ic.packets_recv.to_string()),
                    ("errin", ic.errin.to_string()),
                    ("errout", ic.errout.to_string()),
                    ("dropin", ic.dropin.to_string()),
                    ("dropout", ic.dropout.to_string()),
                    ("fifoin", ic.fifoin.to_string()),
                    ("fifoout", ic.fifoout.to_string()),
                ],
            )
        })
        .collect()
}

/// Go `getDiskLoad`: per-second rates over half a second. Go labels these
/// items `net`, and so does this port.
fn get_disk_load() -> Vec<pb::ServerInfoItem> {
    let Ok(snapshot) = host::disk_io_counters() else {
        return Vec::new();
    };
    std::thread::sleep(Duration::from_millis(500));
    let Ok(current) = host::disk_io_counters() else {
        return Vec::new();
    };
    let prev: HashMap<&str, &host::DiskIoCounters> =
        snapshot.iter().map(|s| (s.name.as_str(), s)).collect();
    // Go's unsigned `c-p` wraps when a counter goes backwards.
    let rate = |p: u64, c: u64| fmt2(c.wrapping_sub(p) as f64 / 0.5);
    let mut results = Vec::new();
    for c in &current {
        let Some(p) = prev.get(c.name.as_str()) else {
            continue;
        };
        results.push(item(
            "net",
            &c.name,
            vec![
                ("read_count/s", rate(p.read_count, c.read_count)),
                (
                    "merged_read_count/s",
                    rate(p.merged_read_count, c.merged_read_count),
                ),
                ("write_count/s", rate(p.write_count, c.write_count)),
                (
                    "merged_write_count/s",
                    rate(p.merged_write_count, c.merged_write_count),
                ),
                ("read_bytes/s", rate(p.read_bytes, c.read_bytes)),
                ("write_bytes/s", rate(p.write_bytes, c.write_bytes)),
            ],
        ));
    }
    results
}

/// Go `getLoadInfo`.
pub(crate) fn get_load_info() -> Vec<pb::ServerInfoItem> {
    let mut results = Vec::new();
    results.extend(get_cpu_load());
    results.extend(get_mem_load());
    results.extend(get_nic_load());
    results.extend(get_disk_load());
    results
}
