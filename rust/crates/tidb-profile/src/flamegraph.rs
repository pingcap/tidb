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

//! Go `pkg/util/profile/flamegraph.go`.

use std::collections::HashMap;

use tidb_datatype::Datum;
use tidb_proto::perftools::profiles as protos;

/// Go `flamegraphNode`.
pub(crate) struct FlamegraphNode {
    children: HashMap<u64, FlamegraphNode>,
    cum_value: i64,
}

impl FlamegraphNode {
    /// Go `newFlamegraphNode`.
    pub(crate) fn new() -> Self {
        Self { children: HashMap::new(), cum_value: 0 }
    }

    /// Go `add`: the value from a sample into the flamegraph DAG. Called on
    /// the root node only. Go names a new child after its location's first
    /// line; the collector names rows by location, so the name is not kept.
    pub(crate) fn add(&mut self, _p: &protos::Profile, sample: &protos::Sample) {
        // FIXME (Go): we take the last sample value by default, but some
        // profiles have multiple samples.
        let Some(&value) = sample.value.last() else {
            return;
        };
        if value == 0 {
            return;
        }
        let mut locs: &[u64] = &sample.location_id;
        let mut n = self;
        loop {
            n.cum_value += value;
            let Some((&loc_id, rest)) = locs.split_last() else {
                return;
            };
            locs = rest;
            n = n.children.entry(loc_id).or_insert_with(FlamegraphNode::new);
        }
    }

    /// Go `sortedChildren`: the children by cumulative value, descending,
    /// then location id.
    fn sorted_children(&self) -> Vec<(u64, &FlamegraphNode)> {
        let mut children: Vec<(u64, &FlamegraphNode)> =
            self.children.iter().map(|(loc_id, child)| (*loc_id, child)).collect();
        children.sort_by(|i, j| j.1.cum_value.cmp(&i.1.cum_value).then(i.0.cmp(&j.0)));
        children
    }
}

/// Go `flamegraphCollector`.
pub(crate) struct FlamegraphCollector<'a> {
    profile: &'a protos::Profile,
    locations: HashMap<u64, &'a protos::Location>,
    functions: HashMap<u64, &'a protos::Function>,
    pub(crate) rows: Vec<Vec<Datum>>,
    total: i64,
    root_child: i64,
}

impl<'a> FlamegraphCollector<'a> {
    /// Go `newFlamegraphCollector`.
    pub(crate) fn new(p: &'a protos::Profile) -> Self {
        Self {
            profile: p,
            locations: p.location.iter().map(|loc| (loc.id, loc)).collect(),
            functions: p.function.iter().map(|function| (function.id, function)).collect(),
            rows: Vec::new(),
            total: 0,
            root_child: 0,
        }
    }

    fn string(&self, index: i64) -> &str {
        usize::try_from(index)
            .ok()
            .and_then(|index| self.profile.string_table.get(index))
            .map_or("", String::as_str)
    }

    /// Go `locationName`.
    fn location_name(&self, loc_id: u64) -> (String, String) {
        let line = self.locations.get(&loc_id).and_then(|loc| loc.line.first());
        let Some(line) = line else {
            return ("<unknown>".to_owned(), "<unknown>".to_owned());
        };
        let function = self.functions.get(&line.function_id);
        let (name, filename) =
            function.map_or(("", ""), |function| (self.string(function.name), self.string(function.filename)));
        (name.to_owned(), format!("{filename}:{}", line.line))
    }

    /// Go `collectChild`.
    fn collect_child(
        &mut self,
        node: (u64, &FlamegraphNode),
        depth: i64,
        indent: &str,
        parent_cum_value: i64,
        is_last_child: bool,
    ) {
        let (loc_id, node) = node;
        let (func_name, file_line) = self.location_name(loc_id);
        self.rows.push(vec![
            Datum::new_string_datum(
                tidb_texttree::pretty_identifier(func_name.as_str(), indent, is_last_child).as_bytes(),
            ),
            Datum::new_string_datum(percentage(node.cum_value, self.total)),
            Datum::new_string_datum(percentage(node.cum_value, parent_cum_value)),
            Datum::Int(self.root_child),
            Datum::Int(depth),
            Datum::new_string_datum(file_line),
        ]);
        if node.children.is_empty() {
            return;
        }
        let indent_4_child = tidb_texttree::indent_4_child(indent, is_last_child).to_string();
        let children = node.sorted_children();
        let last = children.len() - 1;
        for (i, child) in children.into_iter().enumerate() {
            self.collect_child(child, depth + 1, &indent_4_child, node.cum_value, i == last);
        }
    }

    /// Go `collect`.
    pub(crate) fn collect(&mut self, root: &FlamegraphNode) {
        self.rows.push(vec![
            Datum::new_string_datum("root"),
            Datum::new_string_datum("100%"),
            Datum::new_string_datum("100%"),
            Datum::Int(0),
            Datum::Int(0),
            Datum::new_string_datum("root"),
        ]);
        if root.children.is_empty() {
            return;
        }
        self.total = root.cum_value;
        let indent_4_child = tidb_texttree::indent_4_child("", false).to_string();
        let children = root.sorted_children();
        let last = children.len() - 1;
        for (i, child) in children.into_iter().enumerate() {
            self.root_child = i as i64 + 1;
            self.collect_child(child, 1, &indent_4_child, root.cum_value, i == last);
        }
    }
}

/// Go `percentage`.
pub(crate) fn percentage(value: i64, total: i64) -> String {
    let ratio = if total != 0 { (value as f64 / total as f64).abs() * 100.0 } else { 0.0 };
    if (99.95..=100.05).contains(&ratio) {
        "100%".to_owned()
    } else if ratio >= 1.0 {
        format!("{}%", tidb_goruntime::go_strconv::format_float(ratio, b'f', 2, 64))
    } else {
        format!("{}%", tidb_goruntime::go_strconv::format_float(ratio, b'g', 2, 64))
    }
}
