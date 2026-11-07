use super::*;
use crate::error::TransactionTooLargeError;

    #[test]
    fn source_uncovered_combined_key_shapes_node_capacities_and_bounds_are_ordered() {
        let mut tree = Art::new();
        for i in 0..256u16 {
            let key = [i as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
            assert_eq!(tree.get(&key).unwrap(), key);
        }
        for capacity in [4usize, 16, 48, 256] {
            let mut forward = tree.iter(Some(&[0]), Some(&[capacity as u8]));
            for value in 0..capacity as u8 {
                assert!(forward.valid());
                assert_eq!(forward.key(), [value]);
                forward.next().unwrap();
            }
            assert!(!forward.valid());
            assert!(forward.next().is_err());
        }
        let mut empty_bounds = tree.iter(Some(b""), Some(b""));
        assert_eq!(empty_bounds.key(), [0]);
        empty_bounds.next().unwrap();
        assert_eq!(empty_bounds.key(), [1]);
        let mut reverse = tree.iter_reverse(Some(&[8]), Some(&[4]));
        for value in (4..8).rev() {
            assert_eq!(reverse.key(), [value]);
            reverse.next().unwrap();
        }
        assert!(!reverse.valid());
    }

    #[test]
    fn source_uncovered_combined_prefix_flags_handles_and_discard_contracts_hold() {
        let mut tree = Art::new();
        for key in [b"a".as_slice(), b"aa", b"aaa", &[1, 1, 1], &[1, 1, 2]] {
            tree.set(key, Some(key), &[]).unwrap();
            assert_eq!(tree.get(key).unwrap(), key);
        }
        tree.set(b"locked", Some(b"v"), &[FlagsOp::SetKeyLocked])
            .unwrap();
        tree.set(b"flag-only", None, &[FlagsOp::SetAssertNone])
            .unwrap();
        assert!(tree.flags(b"locked").unwrap().has_locked());
        assert!(!tree.flags(b"flag-only").unwrap().has_assertion_flags());
        assert!(tree.get(b"flag-only").is_err());
        tree.set(b"locked", None, &[FlagsOp::DelKeyLocked]).unwrap();
        assert!(!tree.flags(b"locked").unwrap().has_locked());
        tree.set(b"missing", None, &[FlagsOp::SetKeyLocked])
            .unwrap();
        assert!(tree.flags(b"missing").unwrap().has_locked());
        let handle = tree.iter_with_flags(None, None).handle();
        let key = tree.key_by_handle(handle);
        let value = tree.get(&key).unwrap();
        assert_eq!(tree.value_by_handle(handle).as_deref(), Some(value.as_slice()));
        let size = tree.size();
        tree.discard_values();
        assert_eq!(tree.size(), size);
        assert!(tree.value_by_handle(handle).is_none());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tree.get(b"a"))).is_err());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tree.set(
                b"x",
                Some(b"y"),
                &[]
            )))
            .is_err()
        );
    }

    #[test]
    fn cleanup_of_a_write_over_flags_alone_keeps_only_persistent_flags() {
        let mut tree = Art::new();
        let stage = tree.staging();
        tree.set(b"plain", None, &[FlagsOp::SetPresumeKeyNotExists])
            .unwrap();
        tree.set(b"plain", Some(b"v"), &[FlagsOp::SetAssertNotExist])
            .unwrap();
        tree.set(b"locked", None, &[FlagsOp::SetKeyLocked]).unwrap();
        tree.set(b"locked", Some(b"v"), &[FlagsOp::SetAssertNotExist])
            .unwrap();
        tree.cleanup(stage);
        assert!(tree.flags(b"plain").is_err());
        let locked = tree.flags(b"locked").unwrap();
        assert!(locked.has_locked());
        assert!(!locked.has_assertion_flags());
        assert!(tree.get(b"locked").is_err());
    }

    #[test]
    fn source_uncovered_staging_history_persistent_flags_and_handles_match_art() {
        let mut tree = Art::new();
        let stage = tree.staging();
        tree.set(b"ephemeral", Some(b"one"), &[]).unwrap();
        tree.set(b"persistent", Some(b"two"), &[FlagsOp::SetKeyLocked])
            .unwrap();
        let ephemeral = tree
            .iter_with_flags(Some(b"ephemeral"), Some(b"f"))
            .handle();
        tree.cleanup(stage);
        assert!(tree.get(b"ephemeral").is_err());
        assert_eq!(tree.key_by_handle(ephemeral), &b"ephemeral"[..]);
        assert!(tree.value_by_handle(ephemeral).is_none());
        assert!(tree.flags(b"ephemeral").is_err());
        assert!(tree.flags(b"persistent").unwrap().has_locked());
        assert_eq!(tree.len(), 1);

        let non_persistent = tree.staging();
        tree.set(
            b"non-persistent",
            Some(b"value"),
            &[FlagsOp::SetPresumeKeyNotExists],
        )
        .unwrap();
        tree.cleanup(non_persistent);
        tree.set(b"non-persistent", Some(b"new"), &[]).unwrap();
        assert!(!tree
            .flags(b"non-persistent")
            .unwrap()
            .has_presume_key_not_exists());

        let root = tree.staging();
        tree.set(b"history", Some(b"one"), &[]).unwrap();
        let checkpoint = tree.checkpoint();
        tree.set(b"history", Some(b"two"), &[]).unwrap();
        assert_eq!(
            tree.select_value_history(b"history", |v| v == b"one")
                .unwrap(),
            None
        );
        tree.set(b"history", Some(b"three"), &[]).unwrap();
        assert_eq!(
            tree.select_value_history(b"history", |v| v == b"two")
                .unwrap(),
            Some(b"two".to_vec())
        );
        tree.revert_to_checkpoint(&checkpoint);
        assert_eq!(tree.get(b"history").unwrap(), b"two");
        tree.cleanup(root);
    }

    #[test]
    fn source_uncovered_ordinary_iterators_invalidate_but_snapshots_remain_stable() {
        let mut tree = Art::new();
        tree.set(b"a", Some(b"one"), &[]).unwrap();
        let snapshot = tree.snapshot();
        let iterator = tree.iter(None, None);
        tree.set(b"b", Some(b"two"), &[]).unwrap();
        assert!(std::panic::catch_unwind(|| iterator.valid()).is_err());
        assert_eq!(snapshot.get(b"a").unwrap(), b"one");
        assert!(snapshot.get(b"b").is_err());
        let mut snapshot_iterator = snapshot.iter(None, None, false);
        assert_eq!(snapshot_iterator.key(), b"a");
        snapshot_iterator.next().unwrap();
        assert!(!snapshot_iterator.valid());
    }

    #[test]
    fn source_uncovered_limits_hooks_cache_stage_inspection_and_reset_are_source_compatible() {
        let mut tree = Art::new();
        let observed = Arc::new(AtomicU64::new(0));
        let result = observed.clone();
        tree.set_memory_footprint_change_hook(Arc::new(move |memory| {
            result.store(memory, Ordering::Release);
        }));
        tree.set_entry_size_limit(2, u64::MAX);
        assert!(tree.set(b"ab", Some(b"c"), &[]).is_err());
        tree.set_entry_size_limit(u64::MAX, 1);
        assert!(tree.set(b"a", Some(b"b"), &[]).is_err());
        tree.set_entry_size_limit(u64::MAX, u64::MAX);
        let stage = tree.staging();
        tree.set(b"a", Some(b"b"), &[]).unwrap();
        let mut inspected = Vec::new();
        tree.inspect_stage(stage, |key, _, value| {
            inspected.push((key.to_vec(), value.to_vec()))
        });
        assert_eq!(inspected, vec![(b"a".to_vec(), b"b".to_vec())]);
        let _ = tree.get(b"a");
        let _ = tree.get(b"a");
        assert!(tree.cache_hit_count() > 0);
        assert!(tree.cache_miss_count() > 0);
        assert!(tree.memory_hook_is_set());
        assert!(observed.load(Ordering::Acquire) > 0);
        let snapshot_sequence = tree.snapshot_sequence();
        tree.release(stage);
        assert!(tree.snapshot_sequence() > snapshot_sequence);
        tree.reset();
        assert_eq!(tree.len(), 0);
        assert!(!tree.dirty());
    }

    #[test]
    fn source_uncovered_hundred_thousand_decimal_keys_and_long_common_prefixes_are_retrievable() {
        let mut tree = Art::new();
        for number in 0..100_000 {
            let key = format!("{number:010}").into_bytes();
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        for number in [0, 1, 9_999, 42_000, 99_999] {
            let key = format!("{number:010}").into_bytes();
            assert_eq!(tree.get(&key).unwrap(), key);
        }
        let prefix = vec![0; 64];
        let mut first = prefix.clone();
        first.push(1);
        let mut second = prefix;
        second.push(2);
        tree.set(&first, Some(b"one"), &[]).unwrap();
        tree.set(&second, Some(b"two"), &[]).unwrap();
        assert_eq!(tree.get(&first).unwrap(), b"one");
        assert_eq!(tree.get(&second).unwrap(), b"two");
    }

    #[test]
    fn source_uncovered_snapshot_iterators_are_stable_and_shareable_after_tree_writes() {
        let mut tree = Art::new();
        for value in 0..48u8 {
            let key = [0, value];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        let snapshot = Arc::new(tree.snapshot());
        tree.set(&[0, 48], Some(&[0, 48]), &[]).unwrap();
        let mut joins = Vec::new();
        for _ in 0..100 {
            let snapshot = snapshot.clone();
            joins.push(std::thread::spawn(move || {
                let mut iterator = snapshot.iter(None, None, false);
                let mut count = 0;
                while iterator.valid() {
                    let key = iterator.key().to_vec();
                    assert_eq!(key, iterator.value());
                    iterator.next().unwrap();
                    count += 1;
                }
                count
            }));
        }
        for join in joins {
            assert_eq!(join.join().unwrap(), 48);
        }
    }

    /// An iterator seeks its window: keys outside `[lower, upper)` never
    /// appear, and an inverted window is empty in both directions.
    #[test]
    fn an_iterator_window_is_seeked_and_an_inverted_one_is_empty() {
        let mut tree = Art::new();
        for key in [b"a".as_slice(), b"b", b"c", b"d"] {
            tree.set(key, Some(key), &[]).unwrap();
        }
        let keys = |mut iterator: ArtIterator| {
            let mut keys = Vec::new();
            while iterator.valid() {
                keys.push(iterator.key().to_vec());
                iterator.next().unwrap();
            }
            keys
        };
        assert_eq!(keys(tree.iter(Some(b"b"), Some(b"d"))), [b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(keys(tree.iter_reverse(Some(b"d"), Some(b"b"))), [b"c".to_vec(), b"b".to_vec()]);
        assert!(keys(tree.iter(Some(b"d"), Some(b"b"))).is_empty());
        assert!(keys(tree.iter_reverse(Some(b"b"), Some(b"d"))).is_empty());
        let snapshot = tree.snapshot();
        let mut window = snapshot.iter(Some(b"b"), Some(b"c"), false);
        assert_eq!(window.key(), b"b");
        window.next().unwrap();
        assert!(!window.valid());
        assert!(!snapshot.iter(Some(b"c"), Some(b"a"), true).valid());
    }

    /// Go `ART.Mem` is the capacity of the node arena plus the value log's,
    /// whose blocks start at 4 KiB and double, and which only a reset or a
    /// revert gives back.
    #[test]
    fn mem_is_the_capacity_of_the_node_arena_and_the_value_log() {
        let arenas = |tree: &Art| {
            let tree = read(&tree.tree);
            (
                tree.allocator.node_allocator.arena.capacity(),
                tree.allocator.vlog_allocator.arena().capacity(),
            )
        };
        let mut tree = Art::new();
        assert_eq!(tree.memory_footprint(), 0);
        tree.set(b"a", Some(b"one"), &[]).unwrap();
        assert_eq!(arenas(&tree), (4096, 4096));
        assert_eq!(tree.memory_footprint(), 8192);
        // A 5000-byte value outgrows the first 4 KiB block: the next block
        // doubles to 8 KiB.
        tree.set(b"b", Some(&[7; 5000]), &[]).unwrap();
        assert_eq!(arenas(&tree), (4096, 4096 + 8192));
        assert_eq!(tree.memory_footprint(), 16384);
        tree.discard_values();
        assert_eq!(arenas(&tree), (4096, 0));
        tree.reset();
        assert_eq!(tree.memory_footprint(), 0);
    }

    #[test]
    fn source_uncovered_discard_values_releases_storage_and_invalidates_value_readers() {
        let mut tree = Art::new();
        let observed = Arc::new(AtomicU64::new(0));
        let hook_observed = observed.clone();
        tree.set_memory_footprint_change_hook(Arc::new(move |memory| {
            hook_observed.store(memory, Ordering::Release);
        }));
        tree.set(b"key", Some(b"first value"), &[]).unwrap();
        tree.set(b"key", Some(b"replacement value"), &[]).unwrap();
        let mut iterator = tree.iter(None, None);
        let snapshot = tree.snapshot();
        let before = tree.memory_footprint();

        tree.discard_values();

        assert!(tree.memory_footprint() < before);
        assert_eq!(observed.load(Ordering::Acquire), tree.memory_footprint());
        assert_eq!(
            read(&tree.tree).allocator.vlog_allocator.arena().block_count(),
            0
        );
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tree.select_value_history(b"key", |_| true)
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| iterator.value().map(<[u8]>::to_vec))).is_err());
        assert!(std::panic::catch_unwind(|| snapshot.get(b"key")).is_err());
    }

    #[test]
    fn source_uncovered_inspect_stage_follows_reverse_value_log_order() {
        let mut tree = Art::new();
        let stage = tree.staging();
        tree.set(b"b", Some(b"first"), &[]).unwrap();
        tree.set(b"a", Some(b"second"), &[]).unwrap();
        tree.set(b"c", Some(b"third"), &[]).unwrap();
        tree.set(b"a", Some(b"SECOND"), &[]).unwrap();
        tree.set(b"b", Some(b"replacement"), &[]).unwrap();

        let mut inspected = Vec::new();
        tree.inspect_stage(stage, |key, _, value| {
            inspected.push((key.to_vec(), value.to_vec()));
        });
        assert_eq!(
            inspected,
            [
                (b"b".to_vec(), b"replacement".to_vec()),
                (b"c".to_vec(), b"third".to_vec()),
                (b"a".to_vec(), b"SECOND".to_vec()),
            ]
        );
    }

    #[test]
    fn source_uncovered_flags_only_set_enforces_total_buffer_limit() {
        let mut tree = Art::new();
        tree.set_entry_size_limit(u64::MAX, 1);
        let error = tree.set(b"ab", None, &[FlagsOp::SetKeyLocked]).unwrap_err();
        assert!(error.downcast_ref::<TransactionTooLargeError>().is_some());
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.size(), 2);
    }

    #[test]
    fn source_uncovered_finished_snapshot_iterator_next_is_idempotent() {
        let mut tree = Art::new();
        tree.set(b"a", Some(b"value"), &[]).unwrap();
        let mut iterator = tree.snapshot().iter(None, None, false);
        iterator.next().unwrap();
        assert!(!iterator.valid());
        assert!(iterator.next().is_ok());
    }

/// `Mem()` and the footprint hook's calls, step for step, as a Go
/// `kv.Transaction`'s MemDB reports them (captured from a mockstore txn:
/// `txn.Set` / `Staging` / `Cleanup`, `txn.Mem()` and the hook's arguments).
/// Node arena growth never fires the hook; a new value-log block does, and
/// so does Cleanup, after which the value log counts only its used bytes.
#[test]
fn mem_and_its_hook_match_a_go_memdb() {
    let mut tree = Art::new();
    let hooks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = hooks.clone();
    tree.set_memory_footprint_change_hook(Arc::new(move |memory| {
        record.lock().unwrap().push(memory);
    }));
    let hooks = move || hooks.lock().unwrap().clone();
    assert_eq!(tree.memory_footprint(), 0);
    tree.set(b"a", Some(b"one"), &[]).unwrap();
    assert_eq!((tree.memory_footprint(), hooks()), (8192, vec![8192]));
    tree.set(b"b", Some(&[0; 5000]), &[]).unwrap();
    assert_eq!((tree.memory_footprint(), hooks()), (16384, vec![8192, 16384]));
    let stage = tree.staging();
    for i in 0..1000 {
        tree.set(format!("key{i:05}").as_bytes(), Some(&[0; 100]), &[])
            .unwrap();
    }
    assert_eq!(
        (tree.memory_footprint(), tree.len(), tree.size(), hooks()),
        (319488, 1002, 113005, vec![8192, 16384, 32768, 73728, 155648, 319488])
    );
    tree.cleanup(stage);
    assert_eq!(
        (tree.memory_footprint(), tree.len(), tree.size()),
        (66483, 2, 5005)
    );
    assert_eq!(hooks().last(), Some(&66483));
    for i in 0..300 {
        let key = format!("k{}", i * 7919 % 100000);
        tree.set(key.as_bytes(), Some(format!("value-{i}").as_bytes()), &[])
            .unwrap();
    }
    assert_eq!(
        (tree.memory_footprint(), tree.len(), tree.size()),
        (148403, 302, 9358)
    );
    assert_eq!(
        hooks(),
        vec![8192, 16384, 32768, 73728, 155648, 319488, 66483, 82867]
    );
}
