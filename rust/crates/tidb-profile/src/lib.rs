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

//! Go `pkg/util/profile` (`profile.go`, `flamegraph.go`): a pprof profile as
//! the flamegraph rows of the `performance_schema` profile tables.
//!
//! Go's `ProfileGraph` reads the process's own profiles from `runtime/pprof`.
//! This process profiles its CPU through `tidb-cpuprofile` (pprof-rs). Of
//! Go's other runtime profiles it keeps no samples: `heap`, `allocs`,
//! `mutex`, `block` and `threadcreate` are empty profiles (Go's own `mutex`
//! and `block` profiles are empty until their rates are set), and it runs no
//! goroutines. A name `pprof.Lookup` does not know is Go's error.

mod flamegraph;

use std::io::Read as _;
use std::time::Duration;

use prost::Message as _;
use tidb_datatype::Datum;
use tidb_proto::perftools::profiles as protos;

use flamegraph::{FlamegraphCollector, FlamegraphNode};

/// Go `CPUProfileInterval`: how long the CPU profile samples.
pub static CPU_PROFILE_INTERVAL: std::sync::RwLock<Duration> = std::sync::RwLock::new(Duration::from_secs(30));

/// Go `CPUProfileInterval`'s current value.
#[must_use]
pub fn cpu_profile_interval() -> Duration {
    *CPU_PROFILE_INTERVAL.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go `profile.Parse`: a gzipped or plain `profile.proto`.
fn parse(data: &[u8]) -> Result<protos::Profile, String> {
    let decoded;
    let data = if data.starts_with(&[0x1f, 0x8b]) {
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(data)
            .read_to_end(&mut out)
            .map_err(|error| format!("decompressing profile: {error}"))?;
        decoded = out;
        decoded.as_slice()
    } else {
        data
    };
    protos::Profile::decode(data).map_err(|error| format!("parsing profile: {error}"))
}

/// Go `Collector` is used to collect the profile results.
#[derive(Clone, Copy, Debug, Default)]
pub struct Collector;

impl Collector {
    /// Go `ProfileReaderToDatums` reads data from reader and returns the
    /// flamegraph which is organized by tree form.
    pub fn profile_reader_to_datums(&self, data: &[u8]) -> Result<Vec<Vec<Datum>>, String> {
        let p = parse(data)?;
        Ok(self.profile_to_datums(&p))
    }

    /// Go `profileToFlamegraphNode`.
    fn profile_to_flamegraph_node(p: &protos::Profile) -> FlamegraphNode {
        let mut root = FlamegraphNode::new();
        for sample in &p.sample {
            root.add(p, sample);
        }
        root
    }

    /// Go `profileToDatums`.
    fn profile_to_datums(&self, p: &protos::Profile) -> Vec<Vec<Datum>> {
        let root = Self::profile_to_flamegraph_node(p);
        let mut col = FlamegraphCollector::new(p);
        col.collect(&root);
        col.rows
    }

    /// Go `cpuProfileGraph` returns the CPU profile flamegraph which is
    /// organized by tree form.
    fn cpu_profile_graph(&self) -> Result<Vec<Vec<Datum>>, String> {
        let mut pc = tidb_cpuprofile::Collector::new();
        pc.start_cpu_profile()?;
        std::thread::sleep(cpu_profile_interval());
        let buffer = pc.stop_cpu_profile()?;
        self.profile_reader_to_datums(&buffer)
    }

    /// Go `ProfileGraph` returns the CPU/memory/mutex/allocs/block profile
    /// flamegraph which is organized by tree form.
    pub fn profile_graph(&self, name: &str) -> Result<Vec<Vec<Datum>>, String> {
        if name.trim().eq_ignore_ascii_case("cpu") {
            return self.cpu_profile_graph();
        }
        // Go `pprof.Lookup(name)` and `WriteTo`.
        match name {
            // Go writes the goroutine dump (`debug=2`) and parses it with
            // `ParseGoroutines`; this process runs no goroutine.
            "goroutine" => Ok(Vec::new()),
            "heap" | "allocs" | "mutex" | "block" | "threadcreate" => {
                Ok(self.profile_to_datums(&protos::Profile::default()))
            }
            _ => Err(format!("cannot retrieve {name} profile")),
        }
    }

    /// Go `ParseGoroutines` returns the goroutine list for given string
    /// representation.
    pub fn parse_goroutines(&self, data: &[u8]) -> Result<Vec<Vec<Datum>>, String> {
        let content = String::from_utf8_lossy(data);
        let mut rows = Vec::new();
        for goroutine in content.split("\n\n") {
            let Some(col_index) = goroutine.find(':') else {
                return Err("goroutine incompatible with current go version".to_owned());
            };
            let header = goroutine.get("goroutine".len() + 1..col_index).unwrap_or_default();
            let headers: Vec<&str> = header.trim().splitn(2, ' ').collect();
            if headers.len() != 2 {
                return Err(format!("incompatible goroutine headers: {header}"));
            }
            let id: i64 = headers[0]
                .trim()
                .parse()
                .map_err(|error| format!("invalid goroutine id: {}: {error}", headers[0]))?;
            let state = headers[1].trim_matches(|c| c == '[' || c == ']');
            let stack: Vec<&str> = goroutine[col_index + 1..].trim().split('\n').collect();
            let frames = stack.len() / 2;
            for i in 0..frames {
                let func = stack[i * 2];
                let loc = stack[i * 2 + 1];
                let identifier = if i == 0 {
                    func.to_owned()
                } else if i == frames - 1 {
                    format!("{}{}{func}", tidb_texttree::TREE_LAST_NODE, tidb_texttree::TREE_NODE_IDENTIFIER)
                } else {
                    format!("{}{}{func}", tidb_texttree::TREE_MIDDLE_NODE, tidb_texttree::TREE_NODE_IDENTIFIER)
                };
                rows.push(vec![
                    Datum::new_string_datum(identifier),
                    Datum::Int(id),
                    Datum::new_string_datum(state),
                    Datum::new_string_datum(loc.trim()),
                ]);
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests;
