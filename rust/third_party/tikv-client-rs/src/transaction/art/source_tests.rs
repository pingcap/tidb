use super::*;

const NODE_CAPACITIES: [usize; 4] = [4, 16, 48, 256];

fn decimal_key(number: usize) -> Vec<u8> {
    format!("{number:010}").into_bytes()
}

fn collect_keys(mut iterator: ArtIterator) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    while iterator.valid() {
        keys.push(iterator.key().to_vec());
        iterator.next().unwrap();
    }
    keys
}

fn assert_capacity_iteration(
    tree: &Art,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    start_value: u16,
    end_value: u16,
) {
    let mut iterator = tree.iter(lower, upper);
    let mut handles = Vec::new();
    for value in start_value..=end_value {
        let key = [value as u8];
        assert!(iterator.valid(), "forward value {value}");
        assert_eq!(iterator.key(), key);
        assert_eq!(iterator.value(), Some(key.as_slice()));
        handles.push(iterator.handle());
        iterator.next().unwrap();
    }
    assert!(!iterator.valid());
    assert!(iterator.next().is_err());
    for (offset, handle) in handles.into_iter().enumerate() {
        let key = [(start_value as usize + offset) as u8];
        assert_eq!(tree.key_by_handle(handle), key.as_slice());
        assert_eq!(tree.value_by_handle(handle).as_deref(), Some(key.as_slice()));
    }

    let mut iterator = tree.iter_reverse(upper, lower);
    let mut handles = Vec::new();
    for value in (start_value..=end_value).rev() {
        let key = [value as u8];
        assert!(iterator.valid(), "reverse value {value}");
        assert_eq!(iterator.key(), key);
        assert_eq!(iterator.value(), Some(key.as_slice()));
        handles.push(iterator.handle());
        iterator.next().unwrap();
    }
    assert!(!iterator.valid());
    assert!(iterator.next().is_err());
    for (offset, handle) in handles.into_iter().enumerate() {
        let key = [(end_value as usize - offset) as u8];
        assert_eq!(tree.key_by_handle(handle), key.as_slice());
        assert_eq!(tree.value_by_handle(handle).as_deref(), Some(key.as_slice()));
    }
}

fn common_prefix_from(left: &[u8], right: &[u8], depth: usize) -> usize {
    left[depth..]
        .iter()
        .zip(&right[depth..])
        .take_while(|(left, right)| left == right)
        .count()
}

fn path_compare(left: &[usize], right: &[usize]) -> i32 {
    for (left, right) in left.iter().zip(right) {
        match left.cmp(right) {
            std::cmp::Ordering::Less => return -1,
            std::cmp::Ordering::Greater => return 1,
            std::cmp::Ordering::Equal => {}
        }
    }
    left.len().cmp(&right.len()) as i32
}

#[test]
fn source_test_simple() {
    let mut tree = Art::new();
    for value in 0..256u16 {
        let key = [value as u8];
        assert_eq!(tree.get(&key), Err(StaticError::NotExist));
        tree.set(&key, Some(&key), &[]).unwrap();
        assert_eq!(tree.get(&key).unwrap(), key);
    }
}

#[test]
fn source_test_sub_node() {
    let mut tree = Art::new();
    for key in [b"a".as_slice(), b"aa", b"aaa"] {
        tree.set(key, Some(key), &[]).unwrap();
    }
    for key in [b"a".as_slice(), b"aa", b"aaa"] {
        assert_eq!(tree.get(key).unwrap(), key);
    }
}

#[test]
fn source_benchmark_read_after_write_art_contract() {
    let mut tree = Art::new();
    for value in 0..10_000usize {
        let key = [value as u8];
        tree.set(&key, Some(&key), &[]).unwrap();
        assert_eq!(tree.get(&key).unwrap(), key);
    }
}

#[test]
fn source_test_bench_key() {
    let mut tree = Art::new();
    for number in 0..100_000 {
        let key = decimal_key(number);
        tree.set(&key, Some(&key), &[]).unwrap();
    }
    for number in 0..100_000 {
        let key = decimal_key(number);
        assert_eq!(tree.get(&key).unwrap(), key, "key {number}");
    }
}

#[test]
fn source_test_leaf_with_common_prefix() {
    let mut tree = Art::new();
    for key in [[1, 1, 1], [1, 1, 2]] {
        tree.set(&key, Some(&key), &[]).unwrap();
        assert_eq!(tree.get(&key).unwrap(), key);
    }
}

#[test]
fn source_test_update_inplace() {
    let mut tree = Art::new();
    let key = [0];
    for _ in 0..256 {
        let value = vec![0; 4096];
        tree.set(&key, Some(&value), &[]).unwrap();
        assert_eq!(read(&tree.tree).allocator.vlog_allocator.arena().block_count(), 1);
    }
}

#[test]
fn source_test_flag() {
    let mut tree = Art::new();
    tree.set(&[0], Some(&[0]), &[FlagsOp::SetPresumeKeyNotExists])
        .unwrap();
    assert!(tree.flags(&[0]).unwrap().has_presume_key_not_exists());
    tree.set(&[1], Some(&[1]), &[FlagsOp::SetKeyLocked])
        .unwrap();
    assert!(tree.flags(&[1]).unwrap().has_locked());

    let mut iterator = tree.iter(None, None);
    assert!(iterator.valid());
    assert_eq!(iterator.key(), [0]);
    assert_eq!(iterator.value(), Some(&[0][..]));
    assert!(iterator.flags().has_presume_key_not_exists());
    assert!(!iterator.flags().has_locked());
    iterator.next().unwrap();
    assert!(iterator.valid());
    assert_eq!(iterator.key(), [1]);
    assert_eq!(iterator.value(), Some(&[1][..]));
    assert!(iterator.flags().has_locked());
    assert!(!iterator.flags().has_presume_key_not_exists());
    iterator.next().unwrap();
    assert!(!iterator.valid());
}

#[test]
fn source_test_long_prefix_1() {
    let key1 = [
        109, 68, 66, 115, 0, 0, 0, 0, 0, 250, 0, 0, 0, 0, 0, 0, 0, 104, 68, 66, 58, 49, 0, 0, 0, 0,
        251,
    ];
    let key2 = [
        109, 68, 66, 115, 0, 0, 0, 0, 0, 250, 0, 0, 0, 0, 0, 0, 0, 105, 68, 66, 58, 49, 0, 0, 0, 0,
        251,
    ];
    let mut tree = Art::new();
    tree.set(&key1, Some(&[1]), &[]).unwrap();
    tree.set(&key2, Some(&[2]), &[]).unwrap();
    assert_eq!(tree.get(&key1).unwrap(), [1]);
    assert_eq!(tree.get(&key2).unwrap(), [2]);
    tree.set(&key2, Some(&[3]), &[]).unwrap();
    assert_eq!(tree.get(&key2).unwrap(), [3]);
}

#[test]
fn source_test_long_prefix_2() {
    let keys = [
        vec![
            0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255,
            255, 255, 255,
        ],
        vec![
            0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255,
            255, 255, 254,
        ],
        vec![
            0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255,
            255, 255, 253,
        ],
        vec![0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 76],
        vec![
            0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255,
            255, 255, 252,
        ],
    ];
    let mut tree = Art::new();
    for key in &keys {
        tree.set(key, Some(key), &[]).unwrap();
    }
    tree.set(&keys[3], Some(&keys[3]), &[]).unwrap();
    for key in &keys {
        assert_eq!(tree.get(key).unwrap(), *key);
    }
}

#[test]
fn source_test_flag_only_key() {
    let mut tree = Art::new();
    tree.set(&[0], None, &[FlagsOp::SetAssertNone]).unwrap();
    assert!(!tree.flags(&[0]).unwrap().has_assertion_flags());
    assert_eq!(tree.get(&[0]), Err(StaticError::NotExist));
}

#[test]
fn source_test_search_prefix_mismatch() {
    let mut tree = Art::new();
    for key in [[1, 1, 1, 1, 1, 1], [1, 1, 1, 1, 1, 2]] {
        tree.set(&key, Some(&key), &[]).unwrap();
    }
    assert_eq!(tree.get(&[1, 1, 1, 3, 1, 1]), Err(StaticError::NotExist));
}

#[test]
fn source_test_search_optimistic_mismatch() {
    let mut tree = Art::new();
    let prefix = vec![0; 22];
    let mut first = prefix.clone();
    first.push(1);
    let mut second = prefix.clone();
    second.push(2);
    tree.set(&first, Some(&prefix), &[]).unwrap();
    tree.set(&second, Some(&prefix), &[]).unwrap();
    let mut mismatch = vec![0; 21];
    mismatch.extend([1, 1]);
    assert_eq!(tree.get(&mismatch), Err(StaticError::NotExist));
}

#[test]
fn source_test_discard_values() {
    let mut tree = Art::new();
    tree.set(&[1], Some(&[2]), &[]).unwrap();
    let iterator = tree.iter_with_flags(None, None);
    let handle = iterator.handle();
    assert_eq!(tree.value_by_handle(handle).as_deref(), Some(&[2][..]));
    assert_eq!(tree.key_by_handle(handle), &[1][..]);
    tree.discard_values();
    assert_eq!(tree.value_by_handle(handle), None);
    assert_eq!(tree.key_by_handle(handle), &[1][..]);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tree.get(&[3]))).is_err());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tree.set(&[3], Some(&[4]), &[])
    }))
    .is_err());
}

#[test]
fn source_test_iterate_node_capacity() {
    for capacity in NODE_CAPACITIES {
        let mut tree = Art::new();
        for value in 0..capacity {
            let key = [value as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        assert_capacity_iteration(&tree, None, None, 0, (capacity - 1) as u16);
        let middle = (capacity / 2) as u8;
        assert_capacity_iteration(
            &tree,
            Some(&[middle]),
            None,
            u16::from(middle),
            (capacity - 1) as u16,
        );
        assert_capacity_iteration(&tree, None, Some(&[middle]), 0, u16::from(middle) - 1);
    }
}

#[test]
fn source_test_iter_seek_leaf() {
    for capacity in NODE_CAPACITIES {
        let mut tree = Art::new();
        for value in 0..capacity {
            let key = [value as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        for value in 0..capacity {
            let key = [value as u8];
            let iterator = tree.iter(Some(&key), None);
            assert!(iterator.valid());
            assert_eq!(iterator.key(), key);
        }
    }
}

#[test]
fn source_test_multi_level_iterate() {
    let keys: Vec<Vec<u8>> = (0..20).map(|length| vec![0; length + 1]).collect();
    let mut tree = Art::new();
    for key in &keys {
        tree.set(key, Some(key), &[]).unwrap();
    }
    let mut iterator = tree.iter(None, None);
    let mut handles = Vec::new();
    for key in &keys {
        assert!(iterator.valid());
        assert_eq!(iterator.key(), key);
        assert_eq!(iterator.value(), Some(key.as_slice()));
        handles.push(iterator.handle());
        iterator.next().unwrap();
    }
    assert!(!iterator.valid());
    assert!(iterator.next().is_err());
    for (index, handle) in handles.into_iter().enumerate() {
        assert_eq!(tree.key_by_handle(handle), keys[index].as_slice());
        assert_eq!(tree.value_by_handle(handle).as_deref(), Some(keys[index].as_slice()));
    }

    let mut iterator = tree.iter_reverse(None, None);
    for key in keys.iter().rev() {
        assert!(iterator.valid());
        assert_eq!(iterator.key(), key);
        assert_eq!(iterator.value(), Some(key.as_slice()));
        iterator.next().unwrap();
    }
    assert!(!iterator.valid());
    assert!(iterator.next().is_err());
}

#[test]
fn source_test_seek_meet_leaf() {
    let mut tree = Art::new();
    tree.set(&[1], Some(&[1]), &[]).unwrap();
    let mut iterator = tree.iter(Some(&[1]), Some(&[1, 1]));
    assert_eq!(iterator.key(), [1]);
    assert_eq!(iterator.value(), Some(&[1][..]));
    iterator.next().unwrap();
    assert!(!iterator.valid());
    tree.set(&[2], Some(&[2]), &[]).unwrap();
    let mut iterator = tree.iter_reverse(Some(&[2, 2]), Some(&[1, 1]));
    assert_eq!(iterator.key(), [2]);
    assert_eq!(iterator.value(), Some(&[2][..]));
    iterator.next().unwrap();
    assert!(!iterator.valid());

    let mut tree = Art::new();
    tree.set(&[1, 1], Some(&[1, 1]), &[]).unwrap();
    let mut iterator = tree.iter(Some(&[1, 0, 0]), Some(&[1, 2, 0]));
    assert_eq!(iterator.key(), [1, 1]);
    assert_eq!(iterator.value(), Some(&[1, 1][..]));
    iterator.next().unwrap();
    assert!(!iterator.valid());
}

#[test]
fn source_test_iterate_handle() {
    let mut tree = Art::new();
    let stage = tree.staging();
    tree.set(&[1], Some(&[2]), &[]).unwrap();
    let handle = tree.iter_with_flags(None, None).handle();
    assert_eq!(tree.key_by_handle(handle), &[1][..]);
    assert_eq!(tree.value_by_handle(handle).as_deref(), Some(&[2][..]));
    tree.cleanup(stage);
    assert_eq!(tree.key_by_handle(handle), &[1][..]);
    assert_eq!(tree.value_by_handle(handle), None);
}

#[test]
fn source_test_seek_prefix_mismatch() {
    let short_prefix = vec![1; 10];
    let long_prefix = vec![2; 30];
    let mut keys = Vec::new();
    for (prefix, suffix) in [
        (&short_prefix, 1u8),
        (&short_prefix, 2),
        (&long_prefix, 3),
        (&long_prefix, 4),
    ] {
        let mut key = prefix.clone();
        key.push(suffix);
        keys.push(key);
    }
    let mut tree = Art::new();
    for key in &keys {
        tree.set(key, Some(key), &[]).unwrap();
    }
    let mut lower = short_prefix[..short_prefix.len() - 1].to_vec();
    lower.push(0);
    let mut upper = long_prefix[..long_prefix.len() - 1].to_vec();
    upper.push(3);
    let mut iterator = tree.iter(Some(&lower), Some(&upper));
    for key in &keys {
        assert!(iterator.valid());
        assert_eq!(iterator.key(), key);
        assert_eq!(iterator.value(), Some(key.as_slice()));
        iterator.next().unwrap();
    }
    assert!(!iterator.valid());
}

#[test]
fn source_test_iter_seek_no_result() {
    for child_count in [0usize, 5, 17, 49] {
        let mut tree = Art::new();
        for child in 0..child_count {
            let key = [1, child as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        tree.set(&[1, 100], Some(&[1, 100]), &[]).unwrap();
        tree.set(&[1, 200], Some(&[1, 200]), &[]).unwrap();
        assert!(!tree.iter(Some(&[1, 100, 1]), Some(&[1, 200])).valid());
        assert!(!tree
            .iter_reverse(Some(&[1, 200]), Some(&[1, 100, 1]))
            .valid());
    }
}

#[test]
fn source_test_concurrent_snapshot_iter_no_race() {
    for count in [4u8, 16, 48] {
        let mut tree = Art::new();
        for value in 0..count {
            let key = [0, value];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        let snapshot = Arc::new(tree.snapshot());
        tree.set(&[0, count], Some(&[0, count]), &[]).unwrap();
        let mut joins = Vec::new();
        for _ in 0..100 {
            let snapshot = snapshot.clone();
            joins.push(std::thread::spawn(move || {
                let mut iterator = snapshot.iter(None, None, false);
                let mut seen = 0;
                while iterator.valid() {
                    let key = iterator.key().to_vec();
                    assert_eq!(key, iterator.value());
                    iterator.next().unwrap();
                    seen += 1;
                }
                iterator.close();
                seen
            }));
        }
        for join in joins {
            assert_eq!(join.join().unwrap(), usize::from(count));
        }
    }
}
