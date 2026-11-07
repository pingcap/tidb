// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `internal/unionstore/art/art_node_test.go`.

use super::*;

fn allocator() -> ArtAllocator {
    let mut allocator = ArtAllocator::default();
    allocator.init();
    allocator
}

fn panics(f: impl FnOnce()) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err()
}

#[test]
fn test_alloc_node() {
    let check_node_initialization = |a: &ArtAllocator, n: ArtNode| {
        if matches!(n.kind, NodeKind::Node48 | NodeKind::Node256) {
            for word in 0..4 {
                assert_eq!(n.present(a, word), 0);
            }
        }
        assert_eq!(n.node_num(a), 0);
        assert_eq!(n.prefix_len(a), 0);
        assert_eq!(n.inplace_leaf(a), NULL_ART_NODE);
    };

    let mut a = allocator();
    let cnt = 10_000usize;
    let mut nodes = Vec::new();
    for (kind, modulo) in [
        (NodeKind::Node4, 4usize),
        (NodeKind::Node16, 16),
        (NodeKind::Node48, 48),
        (NodeKind::Node256, 256),
    ] {
        let mut addrs = Vec::with_capacity(cnt);
        for i in 0..cnt {
            let n = match kind {
                NodeKind::Node4 => a.alloc_node4(),
                NodeKind::Node16 => a.alloc_node16(),
                NodeKind::Node48 => a.alloc_node48(),
                _ => a.alloc_node256(),
            };
            assert!(!n.addr.is_null());
            check_node_initialization(&a, n);
            n.set_node_num(&mut a, (i % modulo) as u8);
            n.set_prefix_len(&mut a, (i % MAX_IN_NODE_PREFIX_LEN as usize) as u32);
            addrs.push(n);
        }
        nodes.push((modulo, addrs));
    }

    let mut leafs = Vec::with_capacity(cnt);
    for i in 0..cnt {
        let key = i.to_string().into_bytes();
        let lf = a.alloc_leaf(&key);
        assert!(!lf.addr.is_null());
        assert_eq!(a.leaf_key(lf.addr), key);
        leafs.push(lf);
    }

    // test memory safety by checking the assigned values
    for (modulo, addrs) in nodes {
        for (i, n) in addrs.into_iter().enumerate() {
            assert_eq!(n.node_num(&a), (i % modulo) as u8, "{i}");
            assert_eq!(n.prefix_len(&a), (i % MAX_IN_NODE_PREFIX_LEN as usize) as u32, "{i}");
        }
    }
    for (i, lf) in leafs.into_iter().enumerate() {
        assert_eq!(a.leaf_key(lf.addr), i.to_string().into_bytes());
    }
}

#[test]
fn test_node_prefix() {
    let mut a = allocator();

    let test_fn = |a: &mut ArtAllocator, mut an: ArtNode| {
        // simple match
        let key = [1, 2, 3, 4, 5, 6, 1, 2, 3, 1];
        an.set_prefix(a, &[1, 2, 3, 4, 5, 66, 77, 88, 99], 5);

        assert_eq!(an.match_prefix(a, &key, 0), 5);
        // key is shorter than prefix
        assert_eq!(an.match_prefix(a, &key[..4], 0), 4);
        // key[0+depth] != prefix[0]
        assert_eq!(an.match_prefix(a, &key, 1), 0);
        // key[3+depth] != prefix[3]
        assert_eq!(an.match_prefix(a, &key, 6), 3);
        let shifted: Vec<u8> = std::iter::once(1).chain(key).collect();
        assert_eq!(an.match_prefix(a, &shifted, 1), 5);

        // deep match
        let mut leaf_key = vec![0; MAX_IN_NODE_PREFIX_LEN as usize];
        leaf_key.extend([1, 2, 3, 4, 5]);
        let lf = a.alloc_leaf(&leaf_key);
        an.add_child(a, 2, false, lf);
        // the real prefix is [0, ..., 0, 1], but the node.prefix only stores
        // [0, ..., 0]
        an.set_prefix(a, &leaf_key, MAX_IN_NODE_PREFIX_LEN + 1);
        let mut match_key = vec![0; MAX_IN_NODE_PREFIX_LEN as usize];
        match_key.extend([1, 22, 33, 44, 55]);
        let mut mismatch_key = vec![0; MAX_IN_NODE_PREFIX_LEN as usize];
        mismatch_key.extend([11, 22, 33, 44, 55]);
        assert_eq!(an.match_deep(a, &match_key, 0), MAX_IN_NODE_PREFIX_LEN + 1);
        assert_eq!(an.match_deep(a, &mismatch_key, 0), MAX_IN_NODE_PREFIX_LEN);

        // deep match with depth
        let leaf_key: Vec<u8> = vec![0; 10].into_iter().chain(leaf_key).collect();
        let match_key: Vec<u8> = vec![0; 10].into_iter().chain(match_key).collect();
        let mismatch_key: Vec<u8> = vec![0; 10].into_iter().chain(mismatch_key).collect();
        let lf = a.alloc_leaf(&leaf_key);
        an.replace_child(a, 2, lf);
        an.set_prefix(a, &leaf_key[10..], MAX_IN_NODE_PREFIX_LEN + 1);
        assert_eq!(an.match_deep(a, &match_key, 10), MAX_IN_NODE_PREFIX_LEN + 1);
        assert_eq!(an.match_deep(a, &mismatch_key, 10), MAX_IN_NODE_PREFIX_LEN);
    };

    let n = a.alloc_node4();
    test_fn(&mut a, n);
    let n = a.alloc_node16();
    test_fn(&mut a, n);
    let n = a.alloc_node48();
    test_fn(&mut a, n);
    let n = a.alloc_node256();
    test_fn(&mut a, n);
}

#[test]
fn test_ordered_child() {
    // the keys of node4 and node16 are kept ordered as children are added.
    let mut a = allocator();
    for (kind, cap) in [(NodeKind::Node4, NODE4_CAP), (NodeKind::Node16, NODE16_CAP)] {
        let fresh = |a: &mut ArtAllocator| match kind {
            NodeKind::Node4 => a.alloc_node4(),
            _ => a.alloc_node16(),
        };
        // add children in ascending order
        let mut an = fresh(&mut a);
        for i in 0..cap {
            let child = a.alloc_node4();
            an.add_child(&mut a, i as u8, false, child);
            assert_eq!(an.node_num(&a), (i + 1) as u8);
            for j in 0..=i {
                assert_eq!(an.key_at(&a, j), j as u8);
            }
        }
        // add children in descending order
        let mut an = fresh(&mut a);
        for i in 0..cap {
            let child = a.alloc_node4();
            an.add_child(&mut a, (255 - i) as u8, false, child);
            assert_eq!(an.node_num(&a), (i + 1) as u8);
            for j in 0..=i {
                assert_eq!(an.key_at(&a, j), (255 - i + j) as u8);
            }
        }
    }
}

#[test]
fn test_next_prev_present_idx() {
    // the present bitmap finds the next/prev present child index in node48
    // and node256.
    let mut a = allocator();
    let test_fn = |a: &mut ArtAllocator, mut an: ArtNode| {
        assert_eq!(an.next_present_idx(a, 0), NODE256_CAP);
        assert_eq!(an.prev_present_idx(a, 0), INPLACE_INDEX);

        let mid = 128usize;
        let child = a.alloc_node4();
        an.add_child(a, mid as u8, false, child);
        for i in 0..NODE256_CAP {
            let expected_next = if i > mid { NODE256_CAP } else { mid };
            let expected_prev = if i >= mid { mid as isize } else { INPLACE_INDEX };
            assert_eq!(an.next_present_idx(a, i), expected_next);
            assert_eq!(an.prev_present_idx(a, i), expected_prev);
        }
    };
    let n = a.alloc_node48();
    test_fn(&mut a, n);
    // Go allocates a node48 for its node256 case too.
    let n = a.alloc_node48();
    test_fn(&mut a, n);
}

#[test]
fn test_lcp() {
    let k1 = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    let k2 = [1, 2, 3, 4, 5, 7, 7, 8];
    for i in 0..6u32 {
        assert_eq!(longest_common_prefix(&k1, &k2, i), 5 - i);
    }
}

#[test]
fn test_node_add_child() {
    let mut a = allocator();
    let check = |a: &mut ArtAllocator, mut n: ArtNode| {
        assert_eq!(n.node_num(a), 0);
        let lf = a.alloc_leaf(&[1, 2, 3, 4, 5]);
        n.add_child(a, 1, false, lf);
        assert_eq!(n.node_num(a), 1);
        // addChild panics if the key exists.
        let mut copy = n;
        assert!(panics(|| {
            copy.add_child(a, 1, false, lf);
        }));
        n.add_child(a, 2, false, lf);
        assert_eq!(n.node_num(a), 2);
        // an in-place leaf is not counted in nodeNum
        n.add_child(a, 0, true, lf);
        assert_eq!(n.node_num(a), 2);
    };
    let n = a.alloc_node4();
    check(&mut a, n);
    let n = a.alloc_node16();
    check(&mut a, n);
    let n = a.alloc_node48();
    check(&mut a, n);
    let n = a.alloc_node256();
    check(&mut a, n);
}

#[test]
fn test_node_grow() {
    let mut a = allocator();
    let check = |a: &mut ArtAllocator, mut n: ArtNode| {
        let (capacity, after_kind) = match n.kind {
            NodeKind::Node4 => (NODE4_CAP, NodeKind::Node16),
            NodeKind::Node16 => (NODE16_CAP, NodeKind::Node48),
            NodeKind::Node48 => (NODE48_CAP, NodeKind::Node256),
            _ => unreachable!(),
        };
        let before_kind = n.kind;
        for i in 0..capacity {
            let lf = a.alloc_leaf(&[i as u8]);
            n.add_child(a, i as u8, false, lf);
            assert_eq!(n.kind, before_kind);
        }
        let lf = a.alloc_leaf(&[capacity as u8]);
        n.add_child(a, capacity as u8, false, lf);
        assert_eq!(n.kind, after_kind);
    };
    let n = a.alloc_node4();
    check(&mut a, n);
    let n = a.alloc_node16();
    check(&mut a, n);
    let n = a.alloc_node48();
    check(&mut a, n);
}

#[test]
fn test_replace_child() {
    let mut a = allocator();
    let check = |a: &mut ArtAllocator, mut n: ArtNode| {
        assert_eq!(n.node_num(a), 0);
        let lf = a.alloc_leaf(&[1, 2, 3, 4, 5]);
        n.add_child(a, 1, false, lf);
        assert_eq!(n.node_num(a), 1);
        let new_lf = a.alloc_leaf(&[1, 2, 3, 4, 4]);
        // replaceChild panics if the key does not exist.
        assert!(panics(|| n.replace_child(a, 2, new_lf)));
        n.replace_child(a, 1, new_lf);
        assert_eq!(n.node_num(a), 1);
        let (_, child) = n.find_child(a, 1, false);
        assert_ne!(child.addr, lf.addr);
        assert_eq!(child.addr, new_lf.addr);
    };
    let n = a.alloc_node4();
    check(&mut a, n);
    let n = a.alloc_node16();
    check(&mut a, n);
    let n = a.alloc_node48();
    check(&mut a, n);
    let n = a.alloc_node256();
    check(&mut a, n);
}

#[test]
fn test_minimum_node() {
    let mut a = allocator();
    for kind in [
        NodeKind::Node4,
        NodeKind::Node16,
        NodeKind::Node48,
        NodeKind::Node256,
    ] {
        let mut node = match kind {
            NodeKind::Node4 => a.alloc_node4(),
            NodeKind::Node16 => a.alloc_node16(),
            NodeKind::Node48 => a.alloc_node48(),
            _ => a.alloc_node256(),
        };
        for c in [255u8, 127, 63, 0] {
            let lf = a.alloc_leaf(&[c]);
            node.add_child(&mut a, c, false, lf);
            let min_node = minimum_leaf_node(&a, node);
            assert_eq!(min_node.kind, NodeKind::Leaf);
            assert_eq!(min_node.addr, lf.addr);
        }
        let lf = a.alloc_leaf(&[0]);
        node.add_child(&mut a, 0, true, lf);
        let min_node = minimum_leaf_node(&a, node);
        assert_eq!(min_node.kind, NodeKind::Leaf);
        assert_eq!(min_node.addr, lf.addr);
    }
}

#[test]
fn test_key2_chunk() {
    let key: Vec<u8> = (1..=16).collect();
    for i in 0..key.len() {
        let mut diff_key = key.clone();
        diff_key[i] = 255;
        assert_eq!(longest_common_prefix(&key, &diff_key, 0), i as u32);
    }
}

/// The node sizes are Go's `unsafe.Sizeof` of each struct on 64-bit Go
/// (measured with a Go program over client-go's own types), so a tree takes
/// the arena bytes Go's does.
#[test]
fn node_sizes_are_go_unsafe_sizeof() {
    assert_eq!(
        (NODE4_SIZE, NODE16_SIZE, NODE48_SIZE, NODE256_SIZE, LEAF_SIZE),
        (92, 248, 904, 3144, 12)
    );
    // Each node's last field ends inside its size, with Go's trailing
    // padding to the 4-byte alignment of artNode.
    assert_eq!(NODE4_CHILDREN + NODE4_CAP * ART_NODE_SIZE, NODE4_SIZE);
    assert_eq!(NODE16_CHILDREN + NODE16_CAP * ART_NODE_SIZE, NODE16_SIZE);
    assert_eq!(NODE48_CHILDREN + NODE48_CAP * ART_NODE_SIZE, NODE48_SIZE);
    assert_eq!(NODE256_CHILDREN + NODE256_CAP * ART_NODE_SIZE, NODE256_SIZE);
}
