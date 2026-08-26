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
        assert_eq!(tree.key_by_handle(handle), Some(key.as_slice()));
        assert_eq!(tree.value_by_handle(handle), Some(key.as_slice()));
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
        assert_eq!(tree.key_by_handle(handle), Some(key.as_slice()));
        assert_eq!(tree.value_by_handle(handle), Some(key.as_slice()));
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
    for _ in 0..256 {
        tree.set(&[0], Some(&vec![0; 4096]), &[]).unwrap();
        let entry = tree.entries.get(&[0][..]).unwrap();
        assert_eq!(entry.history.len(), 1);
        assert_eq!(entry.value.as_ref().unwrap().len(), 4096);
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
fn source_test_expansion_native_mapping() {
    let prefix = vec![0; 20];
    let mut first = prefix.clone();
    first.extend([1, 1, 1, 1]);
    let mut second = prefix.clone();
    second.extend([1, 1, 1, 2]);
    let mut third = prefix;
    third.extend([1, 255, 2]);
    let mut tree = Art::new();
    tree.set(&first, Some(&[1]), &[]).unwrap();
    tree.set(&second, Some(&[2]), &[]).unwrap();
    tree.set(&third, Some(&[1, 2]), &[]).unwrap();
    assert_eq!(tree.get(&first).unwrap(), [1]);
    assert_eq!(tree.get(&second).unwrap(), [2]);
    assert_eq!(tree.get(&third).unwrap(), [1, 2]);
    assert_eq!(collect_keys(tree.iter(None, None)), [first, second, third]);
}

#[test]
fn source_test_discard_values() {
    let mut tree = Art::new();
    tree.set(&[1], Some(&[2]), &[]).unwrap();
    let iterator = tree.iter_with_flags(None, None);
    let handle = iterator.handle();
    assert_eq!(tree.value_by_handle(handle), Some(&[2][..]));
    assert_eq!(tree.key_by_handle(handle), Some(&[1][..]));
    tree.discard_values();
    assert_eq!(tree.value_by_handle(handle), None);
    assert_eq!(tree.key_by_handle(handle), Some(&[1][..]));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tree.get(&[3]))).is_err());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tree.set(&[3], Some(&[4]), &[])
    }))
    .is_err());
}

#[test]
fn source_test_alloc_node_native_storage() {
    let mut tree = Art::new();
    let mut handles = Vec::new();
    for number in 0..10_000 {
        let key = number.to_string().into_bytes();
        tree.set(&key, Some(&key), &[]).unwrap();
        let entry = tree.entries.get(&key).unwrap();
        assert_eq!(entry.flags, KeyFlags::default());
        handles.push((entry.handle, key));
    }
    for (handle, key) in handles {
        assert_eq!(tree.key_by_handle(handle), Some(key.as_slice()));
        assert_eq!(tree.value_by_handle(handle), Some(key.as_slice()));
    }
}

#[test]
fn source_test_node_prefix_native_mapping() {
    for capacity in NODE_CAPACITIES {
        let prefix = vec![0; 25];
        let mut tree = Art::new();
        let mut keys = Vec::new();
        for suffix in 0..capacity {
            let mut key = prefix.clone();
            key.push(suffix as u8);
            tree.set(&key, Some(&key), &[]).unwrap();
            keys.push(key);
        }
        for key in keys {
            assert_eq!(tree.get(&key).unwrap(), key);
        }
        let mut mismatch = prefix;
        mismatch[20] = 1;
        assert_eq!(tree.get(&mismatch), Err(StaticError::NotExist));
    }
}

#[test]
fn source_test_ordered_child_native_mapping() {
    for capacity in [4usize, 16] {
        let mut ascending = Art::new();
        for value in 0..capacity {
            let key = [value as u8];
            ascending.set(&key, Some(&key), &[]).unwrap();
        }
        let expected: Vec<_> = (0..capacity).map(|value| vec![value as u8]).collect();
        assert_eq!(collect_keys(ascending.iter(None, None)), expected);

        let mut descending = Art::new();
        for value in 0..capacity {
            let key = [(255 - value) as u8];
            descending.set(&key, Some(&key), &[]).unwrap();
        }
        let expected: Vec<_> = (0..capacity)
            .rev()
            .map(|value| vec![(255 - value) as u8])
            .collect();
        assert_eq!(collect_keys(descending.iter(None, None)), expected);
    }
}

#[test]
fn source_test_next_prev_present_index_native_mapping() {
    let keys = [0u8, 64, 128, 192, 255];
    let mut tree = Art::new();
    for key in keys {
        tree.set(&[key], Some(&[key]), &[]).unwrap();
    }
    for probe in 0..=255u16 {
        let probe_key = [probe as u8];
        let forward = tree.iter(Some(&probe_key), None);
        let expected_next = keys.into_iter().find(|key| u16::from(*key) >= probe);
        assert_eq!(forward.valid(), expected_next.is_some());
        if let Some(expected) = expected_next {
            assert_eq!(forward.key(), [expected]);
        }

        let upper = (probe < 255).then(|| [(probe + 1) as u8]);
        let reverse = tree.iter_reverse(upper.as_ref().map(<[_; 1]>::as_slice), None);
        let expected_previous = keys.into_iter().rev().find(|key| u16::from(*key) <= probe);
        assert_eq!(reverse.valid(), expected_previous.is_some());
        if let Some(expected) = expected_previous {
            assert_eq!(reverse.key(), [expected]);
        }
    }
}

#[test]
fn source_test_lcp_native_mapping() {
    let first = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    let second = [1, 2, 3, 4, 5, 7, 7, 8];
    for depth in 0..6 {
        assert_eq!(common_prefix_from(&first, &second, depth), 5 - depth);
    }
    let mut tree = Art::new();
    tree.set(&first, Some(&first), &[]).unwrap();
    tree.set(&second, Some(&second), &[]).unwrap();
    assert_eq!(tree.get(&first).unwrap(), first);
    assert_eq!(tree.get(&second).unwrap(), second);
}

#[test]
fn source_test_node_add_child_native_mapping() {
    for capacity in NODE_CAPACITIES {
        let mut tree = Art::new();
        for value in 0..capacity {
            let key = [value as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        let len = tree.len();
        tree.set(&[1], Some(b"replacement"), &[]).unwrap();
        assert_eq!(tree.len(), len);
        assert_eq!(tree.get(&[1]).unwrap(), b"replacement");
        tree.set(b"", Some(b"in-place"), &[]).unwrap();
        assert_eq!(tree.len(), len + 1);
    }
}

#[test]
fn source_test_node_grow_native_mapping() {
    for capacity in [4usize, 16, 48] {
        let mut tree = Art::new();
        for value in 0..=capacity {
            let key = [value as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        assert_eq!(tree.len(), capacity + 1);
        for value in 0..=capacity {
            let key = [value as u8];
            assert_eq!(tree.get(&key).unwrap(), key);
        }
    }
}

#[test]
fn source_test_replace_child_native_mapping() {
    for capacity in NODE_CAPACITIES {
        let mut tree = Art::new();
        for value in 0..capacity {
            let key = [value as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        let len = tree.len();
        tree.set(&[1], Some(b"new leaf"), &[]).unwrap();
        assert_eq!(tree.len(), len);
        assert_eq!(tree.get(&[1]).unwrap(), b"new leaf");
    }
}

#[test]
fn source_test_minimum_node_native_mapping() {
    for _ in NODE_CAPACITIES {
        let mut tree = Art::new();
        for key in [255u8, 127, 63, 0] {
            tree.set(&[key], Some(&[key]), &[]).unwrap();
            assert_eq!(tree.iter(None, None).key(), [key]);
        }
        tree.set(b"", Some(b"minimum"), &[]).unwrap();
        assert_eq!(tree.iter(None, None).key(), b"");
    }
}

#[test]
fn source_test_key_to_chunk_native_mapping() {
    let key: Vec<u8> = (1..=16).collect();
    for index in 0..key.len() {
        let mut different = key.clone();
        different[index] = 255;
        assert_eq!(common_prefix_from(&key, &different, 0), index);
        let mut tree = Art::new();
        tree.set(&key, Some(&key), &[]).unwrap();
        tree.set(&different, Some(&different), &[]).unwrap();
        assert_eq!(tree.get(&key).unwrap(), key);
        assert_eq!(tree.get(&different).unwrap(), different);
    }
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
        assert_eq!(tree.key_by_handle(handle), Some(keys[index].as_slice()));
        assert_eq!(tree.value_by_handle(handle), Some(keys[index].as_slice()));
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
fn source_test_seek_in_existing_node_native_mapping() {
    for count in [4usize, 16, 48, 49] {
        let mut tree = Art::new();
        for index in 0..count {
            let key = [(2 * index) as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        for index in 0..count - 1 {
            let seek = [(2 * index + 1) as u8];
            let iterator = tree.iter(Some(&seek), None);
            assert!(iterator.valid());
            assert_eq!(iterator.key(), [(2 * (index + 1)) as u8]);
        }
    }
}

#[test]
fn source_test_seek_to_index_native_mapping() {
    for _ in NODE_CAPACITIES {
        let mut tree = Art::new();
        tree.set(&[10], Some(&[10]), &[]).unwrap();
        let iterator = tree.iter(Some(&[1]), None);
        assert!(iterator.valid());
        assert_eq!(iterator.key(), [10]);
        assert!(!tree.iter(Some(&[11]), None).valid());
    }
}

#[test]
fn source_test_iterate_handle() {
    let mut tree = Art::new();
    let stage = tree.staging();
    tree.set(&[1], Some(&[2]), &[]).unwrap();
    let handle = tree.iter_with_flags(None, None).handle();
    assert_eq!(tree.key_by_handle(handle), Some(&[1][..]));
    assert_eq!(tree.value_by_handle(handle), Some(&[2][..]));
    tree.cleanup(stage);
    assert_eq!(tree.key_by_handle(handle), Some(&[1][..]));
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
fn source_test_iter_position_compare_native_mapping() {
    assert_eq!(path_compare(&[1, 2, 3], &[1, 2, 3]), 0);
    assert_eq!(path_compare(&[1, 2, 2], &[1, 2, 3]), -1);
    assert_eq!(path_compare(&[1, 2, 4], &[1, 2, 3]), 1);
    assert_eq!(path_compare(&[1, 2, 3], &[1, 2]), 1);
    assert_eq!(path_compare(&[1, 2], &[1, 2, 3]), -1);
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
fn source_test_snapshot_iterator_prevent_free_node_native_mapping() {
    for count in [4u8, 16, 48] {
        let mut tree = Art::new();
        for value in 0..count {
            let key = [0, value];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        let snapshot = tree.snapshot();
        let mut iterator = snapshot.iter(None, None, false);
        tree.set(&[0, count], Some(&[0, count]), &[]).unwrap();
        let mut seen = 0;
        while iterator.valid() {
            let key = iterator.key().to_vec();
            assert_eq!(key, iterator.value());
            iterator.next().unwrap();
            seen += 1;
        }
        assert_eq!(seen, usize::from(count));
        assert_eq!(snapshot.get(&[0, count]), Err(StaticError::NotExist));
        iterator.close();
        assert_eq!(tree.snapshot().get(&[0, count]).unwrap(), [0, count]);
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
