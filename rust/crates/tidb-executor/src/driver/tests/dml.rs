//! How a storage failure keeps its SQL identity through the executor's row
//! read and write adapters (`kv_read_error` / `kv_write_error`).

use super::*;

#[test]
fn storage_failures_keep_sql_identity_through_read_and_write_adapters() {
    use tidb_kv::storage::StorageError;
    for storage in [
        StorageError::Sql(crate::MysqlError::from_terror(
            &tidb_kv::kv_errors::ERR_WRITE_CONFLICT.gen_with_stack("write conflict"),
        )),
        StorageError::Backend("backend unavailable".to_owned()),
        StorageError::Backend("Retryable(backend diagnostic text)".to_owned()),
    ] {
        for error in [
            kv_read_error("row read failed", storage.clone().into()),
            kv_write_error(storage.clone().into()),
        ] {
            let error = error.to_mysql_error();
            match &storage {
                StorageError::Sql(expected) => assert_eq!(&error, expected),
                _ => assert_eq!(error.code, 1105, "{error:?}"),
            }
        }
    }
}

#[test]
fn retryable_storage_errors_keep_their_transaction_identity() {
    let retryable = || {
        tidb_table::tables::KvTableError::from(tidb_kv::storage::StorageError::Retryable(
            "region response no longer matches observed route".to_owned(),
        ))
    };

    for error in [
        kv_read_error("row read failed", retryable()),
        kv_write_error(retryable()),
    ] {
        assert!(
            error.equal(&tidb_txnkv::tikv_errors::ERR_REGION_UNAVAILABLE),
            "retryable storage failure lost its transaction identity"
        );
    }

    let ordinary = kv_read_error(
        "row read failed",
        tidb_table::tables::KvTableError::from(tidb_kv::storage::StorageError::Backend(
            "disk error".to_owned(),
        )),
    );
    // A storage failure is a runtime 1105, never a 1064: the client's SQL
    // text was fine.
    assert!(matches!(
        ordinary,
        DriverError::Exec(crate::ExecError::Internal(_))
    ));
}
