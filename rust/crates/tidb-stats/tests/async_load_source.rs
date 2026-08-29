use std::sync::Arc;

use tidb_model::TableItemID;
use tidb_stats::NeededStatsMap;

fn item(id: i64) -> TableItemID {
    TableItemID {
        table_id: 9,
        id,
        is_index: false,
        is_sync_load_failed: false,
    }
}

#[test]
fn needed_items_only_upgrade_to_full_load() {
    let needed = NeededStatsMap::new();
    needed.insert(item(1), false);
    needed.insert(item(1), true);
    needed.insert(item(1), false);

    assert_eq!(needed.len(), 1);
    assert_eq!(needed.all_items()[0].table_item_id, item(1));
    assert!(needed.all_items()[0].full_load);
    needed.delete(item(1));
    assert!(needed.is_empty());
}

#[test]
fn all_shards_are_safe_under_concurrent_insert_and_delete() {
    let needed = Arc::new(NeededStatsMap::new());
    let threads = (-256..256)
        .map(|id| {
            let needed = Arc::clone(&needed);
            std::thread::spawn(move || needed.insert(item(id), id % 2 == 0))
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().expect("insert worker");
    }
    assert_eq!(needed.len(), 512);

    let threads = (-256..256)
        .map(|id| {
            let needed = Arc::clone(&needed);
            std::thread::spawn(move || needed.delete(item(id)))
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().expect("delete worker");
    }
    assert!(needed.is_empty());
}
