//! Go `br/pkg/utils` `misc_test.go` and `schema_test.go`.

use tidb_proto::backup::File;

use super::*;

/// Go `TestSummaryFiles`.
#[test]
fn summary_files_xor_and_sum() {
    let file = |crc: u64, kvs: u64, bytes: u64| File {
        crc64xor: crc,
        total_kvs: kvs,
        total_bytes: bytes,
        ..Default::default()
    };
    let (crc, kvs, bytes) = summary_files(&[
        file(0xF, 10, 100),
        file(0xF0, 20, 200),
        file(0xF00, 30, 300),
    ]);
    assert_eq!((crc, kvs, bytes), (0xFFF, 60, 600));
}

/// Go `TestIsSysOrTempSysDB`.
#[test]
fn is_sys_or_temp_sys_db_table() {
    for (db, expected) in [
        ("mysql", true),
        ("sys", true),
        ("workload_schema", true),
        ("__TiDB_BR_Temporary_mysql", true),
        ("__TiDB_BR_Temporary_sys", true),
        ("__TiDB_BR_Temporary_workload_schema", true),
        ("test", false),
        ("__TiDB_BR_Temporary_test", false),
    ] {
        assert_eq!(is_sys_or_temp_sys_db(db), expected, "{db}");
    }
}

/// Go `CheckStoreLiveness`: tombstone and a stale heartbeat are refused.
#[test]
fn store_liveness() {
    let mut s = metapb::Store {
        id: 1,
        state: metapb::StoreState::Up as i32,
        ..Default::default()
    };
    check_store_liveness(&s).unwrap();
    s.state = metapb::StoreState::Offline as i32;
    check_store_liveness(&s).unwrap();
    s.state = metapb::StoreState::Tombstone as i32;
    let err = check_store_liveness(&s).unwrap_err().to_string();
    assert!(
        err.starts_with("the store state isn't up, it is Tombstone"),
        "{err}"
    );
    s.state = metapb::StoreState::Up as i32;
    let old = SystemTime::now() - Duration::from_secs(200);
    s.last_heartbeat = old.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
    let err = check_store_liveness(&s).unwrap_err().to_string();
    assert!(
        err.starts_with("the store last heartbeat is too far, at 3m20"),
        "{err}"
    );
}
