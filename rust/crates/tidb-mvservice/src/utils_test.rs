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

//! Go master `pkg/mvservice/utils_test.go`.

use std::collections::HashMap;

use crate::consistenthash::ConsistentHash;
use crate::server_maintainer::ServerInfo;
use crate::utils::{Lessable, PriorityQueue};

fn must_hash(mapping: &[(&str, u32)]) -> impl Fn(&[u8]) -> u32 + Send + Sync + 'static {
    let mapping: HashMap<Vec<u8>, u32> = mapping.iter().map(|(k, v)| (k.as_bytes().to_vec(), *v)).collect();
    move |data: &[u8]| {
        *mapping.get(data).unwrap_or_else(|| panic!("missing hash mapping for {}", String::from_utf8_lossy(data)))
    }
}

fn ring_sorted(c: &ConsistentHash) -> bool {
    c.ring.windows(2).all(|w| w[0].hash <= w[1].hash)
}

#[test]
fn consistent_hash_get_node_wraps() {
    let mut c = ConsistentHash::new(1);
    c.set_hash_func(must_hash(&[("nodeA#0", 10), ("nodeB#0", 20), ("key-low", 5), ("key-mid", 15), ("key-high", 25)]));
    c.add_node("nodeA");
    c.add_node("nodeB");
    assert_eq!(c.get_node(b"key-low"), "nodeA");
    assert_eq!(c.get_node(b"key-mid"), "nodeB");
    assert_eq!(c.get_node(b"key-high"), "nodeA", "wrap");
    assert!(ring_sorted(&c));
}

#[test]
fn consistent_hash_remove_node_updates_state() {
    let mut c = ConsistentHash::new(2);
    c.set_hash_func(must_hash(&[("nodeA#0", 10), ("nodeA#1", 30), ("nodeB#0", 20), ("nodeB#1", 40)]));
    c.add_node("nodeA");
    c.add_node("nodeB");
    assert_eq!(c.node_count(), 2);
    assert_eq!(c.get_virtual_nodes("nodeA"), 2);
    assert_eq!(c.get_virtual_nodes("nodeB"), 2);

    c.remove_node("nodeB");
    assert_eq!(c.node_count(), 1);
    assert_eq!(c.get_virtual_nodes("nodeB"), 0);
    assert_eq!(c.ring.len(), 2);
    assert_eq!(c.get_nodes(), vec!["nodeA".to_owned()]);
    assert!(ring_sorted(&c));
}

#[test]
fn consistent_hash_empty_state() {
    let c = ConsistentHash::new(1);
    assert_eq!(c.get_node(b"any"), "");
    assert_eq!(c.node_count(), 0);
    assert!(c.get_nodes().is_empty());
}

#[test]
fn consistent_hash_rebuild_resets_ring() {
    let mut c = ConsistentHash::new(2);
    c.set_hash_func(must_hash(&[
        ("nodeA#0", 10),
        ("nodeA#1", 30),
        ("nodeB#0", 20),
        ("nodeB#1", 40),
        ("nodeC#0", 15),
        ("nodeC#1", 35),
        ("key", 25),
    ]));
    c.add_node("nodeA");
    c.add_node("nodeB");
    assert_eq!(c.node_count(), 2);
    assert_ne!(c.get_node(b"key"), "");

    c.rebuild(&HashMap::from([("nodeC".to_owned(), ServerInfo { id: "nodeC".to_owned(), ..ServerInfo::default() })]));
    assert_eq!(c.node_count(), 1);
    assert_eq!(c.ring.len(), 2);
    assert_eq!(c.get_node(b"key"), "nodeC");
    assert!(ring_sorted(&c));
}

#[test]
fn consistent_hash_rebuild() {
    let mut c = ConsistentHash::new(2);
    c.set_hash_func(must_hash(&[("nodeA#0", 10), ("nodeA#1", 30), ("nodeB#0", 20), ("nodeB#1", 40), ("key", 35)]));
    c.rebuild(&HashMap::from([
        ("nodeA".to_owned(), ServerInfo { id: "nodeA".to_owned(), ..ServerInfo::default() }),
        ("nodeB".to_owned(), ServerInfo { id: "nodeB".to_owned(), ..ServerInfo::default() }),
    ]));
    assert_eq!(c.node_count(), 2);
    assert_eq!(c.ring.len(), 4);
    assert_ne!(c.get_node(b"key"), "");
    assert!(ring_sorted(&c));
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Node {
    v: String,
    ts: i64,
}

impl Lessable for Node {
    fn less(&self, other: &Self) -> bool {
        self.ts < other.ts
    }
}

fn node(v: &str, ts: i64) -> Node {
    Node { v: v.to_owned(), ts }
}

fn drain(pq: &mut PriorityQueue<Node>) -> Vec<String> {
    let mut pop_res = Vec::new();
    while let Some((_, value)) = pq.pop() {
        pop_res.push(value.v);
    }
    pop_res
}

/// Go `TestPriorityQueue`.
#[test]
fn priority_queue() {
    // Go `time.Time{}.Add(time.Hour * 8192).UnixMilli()`.
    let base = -62_135_596_800_000 + 8192 * 3_600_000;

    // basic ordering and update
    let mut pq = PriorityQueue::default();
    pq.push(node("b", base + 2000));
    pq.push(node("a", base + 1000));
    pq.push(node("c", base + 3000));
    assert_eq!(pq.front().unwrap().1.v, "a");
    let front = pq.front().unwrap().0;
    pq.update(front, node("d", base + 5000));
    pq.push(node("c", base + 500));
    assert_eq!(drain(&mut pq), ["c", "b", "c", "d"]);

    // empty and invalid operations
    let mut pq: PriorityQueue<Node> = PriorityQueue::default();
    assert!(pq.front().is_none());
    assert!(pq.pop().is_none());
    let mut other = PriorityQueue::default();
    let ghost = other.push(node("ghost", base + 1000));
    other.pop();
    pq.update(ghost, node("still-ghost", base + 2000));
    assert_eq!(pq.remove(ghost), None);

    // remove and update after removal
    let mut pq = PriorityQueue::default();
    let item_a = pq.push(node("a", base + 2000));
    let item_b = pq.push(node("b", base + 3000));
    let item_c = pq.push(node("c", base + 1000));
    assert_eq!(pq.remove(item_b).unwrap().v, "b");
    pq.update(item_b, node("b2", base + 500));
    pq.update(item_a, node("a2", base + 4000));
    pq.push(node("d", base + 1500));
    let pop_res = drain(&mut pq);
    assert_eq!(pop_res, ["c", "d", "a2"]);
    assert!(!pop_res.contains(&"b".to_owned()));
    assert!(pq.get(item_c).is_none());
}
