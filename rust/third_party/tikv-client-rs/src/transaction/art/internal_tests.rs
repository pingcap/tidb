// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `art_test.go` / `art_snapshot_test.go` tests that read the
//! tree's nodes and allocator.

use super::node::{leaf, NodeKind};
use super::*;

#[test]
fn test_expansion() {
    // expand leaf
    let mut tree = Art::new();
    let prefix = vec![0u8; 20];
    let key = |suffix: &[u8]| -> Vec<u8> { prefix.iter().chain(suffix).copied().collect() };
    tree.set(&key(&[1, 1, 1, 1]), Some(&[1]), &[]).unwrap();
    {
        let t = read(&tree.tree);
        let an = t.root.child_at_slot(&t.allocator, 0);
        assert_eq!(an.kind, NodeKind::Leaf);
    }
    tree.set(&key(&[1, 1, 1, 2]), Some(&[2]), &[]).unwrap();
    let old_addr = {
        let t = read(&tree.tree);
        let a = &t.allocator;
        let an = t.root.child_at_slot(a, 0);
        assert_eq!(an.kind, NodeKind::Node4);
        assert_eq!(an.node_num(a), 2);
        assert_eq!(an.prefix_len(a), 22);
        assert_eq!([an.key_at(a, 0), an.key_at(a, 1)], [1, 2]);
        assert_eq!(a.leaf_key(an.child_at_slot(a, 0).addr), key(&[1, 1, 1, 1]));
        assert_eq!(a.leaf_key(an.child_at_slot(a, 1).addr), key(&[1, 1, 1, 2]));
        an.addr
    };

    // expand node
    tree.set(&key(&[1, 255, 2]), Some(&[1, 2]), &[]).unwrap();
    let t = read(&tree.tree);
    let a = &t.allocator;
    let an = t.root.child_at_slot(a, 0);
    assert_eq!(an.kind, NodeKind::Node4);
    // the node is expanded: its parent is the newly created node.
    assert_ne!(an.addr, old_addr);
    assert_eq!(an.node_num(a), 2);
    assert_eq!(an.prefix_len(a), 20);
    // the old node4 is a child of the new node4 now.
    let old_an = an.child_at_slot(a, 0);
    assert_eq!(old_an.kind, NodeKind::Node4);
    assert_eq!(old_an.addr, old_addr);
    assert_eq!(old_an.prefix_len(a), 1);
    assert_eq!(old_an.raw_prefix(a)[..1], [1]);
    assert_eq!([an.key_at(a, 0), an.key_at(a, 1)], [1, 255]);
    assert_eq!(a.leaf_key(an.child_at_slot(a, 1).addr), key(&[1, 255, 2]));
    assert!(!leaf::is_deleted(a.node_data(an.child_at_slot(a, 1).addr)));
}

#[test]
fn test_snapshot_iterator_prevent_free_node() {
    for num in [4u8, 16, 48] {
        let mut tree = Art::new();
        for i in 0..num {
            tree.set(&[0, i], Some(&[0, i]), &[]).unwrap();
        }
        let unused = |tree: &Art| {
            let lens = read(&tree.tree).allocator.unused_node_lens();
            match num {
                4 => lens.0,
                16 => lens.1,
                _ => lens.2,
            }
        };
        let it = tree.snapshot().iter(None, None, false);
        assert_eq!(unused(&tree), 0);
        tree.set(&[0, num], Some(&[0, num]), &[]).unwrap();
        assert_eq!(unused(&tree), 1);
        it.close();
        assert_eq!(unused(&tree), 0);
    }
}

/// A node split below a long prefix files the old node under the prefix
/// byte at the mismatch (Go `expandNode`'s `nodeChar`), so every key stays
/// reachable and ordered when the prefix bytes around it differ.
#[test]
fn an_expanded_node_keeps_its_children_under_the_mismatched_prefix_byte() {
    let mut tree = Art::new();
    let key = |suffix: &[u8]| -> Vec<u8> { [0u8; 20].iter().chain(suffix).copied().collect() };
    let keys = [key(&[1, 2, 3, 4]), key(&[1, 2, 3, 5]), key(&[1, 9])];
    for k in &keys {
        tree.set(k, Some(k), &[]).unwrap();
    }
    for k in &keys {
        assert_eq!(tree.get(k).unwrap(), *k);
    }
    let mut it = tree.iter(None, None);
    let mut seen = Vec::new();
    while it.valid() {
        seen.push(it.key().to_vec());
        it.next().unwrap();
    }
    assert_eq!(seen, keys);
}
