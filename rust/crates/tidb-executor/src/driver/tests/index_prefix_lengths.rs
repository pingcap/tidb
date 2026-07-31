//! What `CREATE TABLE`/`CREATE INDEX` does with a declared key-part length,
//! end to end through the driver.
//!
//! Two different answers live here, and the difference is the point.
//!
//! * A length TiDB ITSELF rejects is rejected here with TiDB's own error --
//!   1089, 1170, 1391, 1071. These are pinned because they are the only thing
//!   standing between a caller and a table whose index does not mean what its
//!   definition says; if the validation were ever lost, every one of them
//!   would start SUCCEEDING rather than start failing.
//! * A length TiDB ACCEPTS is refused with the deferral, because this tier
//!   does not yet honour a prefix on the read path. See
//!   [`crate::ddl::index_prefix::prefix_unsupported`].
//!
//! Every case was captured from real TiDB through `gorun` first. Mirrors Go
//! `pkg/ddl/index.go`'s `checkIndexColumn`.

use super::*;
use crate::ddl::index_prefix::prefix_unsupported;

fn create_error(sql: &str) -> DriverError {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(sql, &mut catalog).expect_err("this statement must be refused")
}

/// Captured: `create table e1 (a int, key idx(a(3)))` is refused. Go
/// `ErrIncorrectPrefixKey` (1089), whose message names neither the column nor
/// the length.
#[test]
fn a_length_on_a_type_that_has_no_prefix_is_1089() {
    for sql in [
        "CREATE TABLE t (a INT, KEY idx (a(3)))",
        "CREATE TABLE t (a BIGINT, UNIQUE KEY idx (a(3)))",
        "CREATE TABLE t (a DATETIME, KEY idx (a(3)))",
        // A length longer than the column reports the same error.
        "CREATE TABLE t (a VARCHAR(5), KEY idx (a(10)))",
        "CREATE TABLE t (a CHAR(5), KEY idx (a(6)))",
    ] {
        assert!(
            matches!(create_error(sql), DriverError::IncorrectPrefixKey),
            "{sql}"
        );
    }
}

/// Captured: `create table e2 (a blob, key idx(a))` is refused, because a
/// BLOB/TEXT key part must say how much of the column it covers. Go
/// `ErrBlobKeyWithoutLength` (1170).
#[test]
fn a_blob_or_text_key_part_without_a_length_is_1170() {
    for (sql, column) in [
        ("CREATE TABLE t (a BLOB, KEY idx (a))", "a"),
        ("CREATE TABLE t (a TEXT, KEY idx (a))", "a"),
        ("CREATE TABLE t (a LONGTEXT, UNIQUE KEY idx (a))", "a"),
        ("CREATE TABLE t (x INT, b MEDIUMBLOB, KEY idx (x, b))", "b"),
    ] {
        match create_error(sql) {
            DriverError::BlobKeyWithoutLength(named) => assert_eq!(named, column, "{sql}"),
            other => panic!("{sql}: {other:?}"),
        }
    }
}

/// Captured: `create table e6 (a varchar(20), key idx(a(0)))` is refused. Go
/// `ErrKeyPart0` (1391) -- a DIFFERENT error from 1089, and for a BLOB it is
/// also different from the missing-length 1170.
#[test]
fn a_zero_length_key_part_is_1391() {
    for sql in [
        "CREATE TABLE t (a VARCHAR(20), KEY idx (a(0)))",
        "CREATE TABLE t (a CHAR(20), KEY idx (a(0)))",
        "CREATE TABLE t (a BLOB, KEY idx (a(0)))",
    ] {
        match create_error(sql) {
            DriverError::KeyPart0(column) => assert_eq!(column, "a", "{sql}"),
            other => panic!("{sql}: {other:?}"),
        }
    }
}

/// Go `ErrTooLongKey` (1071) counts BYTES, so the same character count passes
/// in `latin1` and fails in `utf8mb4`. The number Go reports is the
/// multiplied one.
#[test]
fn a_key_part_longer_than_the_limit_is_1071() {
    match create_error("CREATE TABLE t (a VARCHAR(2000), KEY idx (a(1000)))") {
        DriverError::TooLongKey { length, max } => {
            assert_eq!((length, max), (4000, 3072));
        }
        other => panic!("{other:?}"),
    }
    // 1000 latin1 characters are 1000 bytes, so the length is legal -- and
    // the statement is then refused for the OTHER reason, which is what
    // proves the limit and the deferral are distinct answers.
    assert!(matches!(
        create_error(
            "CREATE TABLE t (a VARCHAR(2000) CHARACTER SET latin1, KEY idx (a(1000)))"
        ),
        DriverError::Unsupported(reason) if reason == prefix_unsupported()
    ));
}

/// Go `buildIndexColumns` stores a prefix that covers the WHOLE column as no
/// prefix at all, so this is an ordinary index and the statement SUCCEEDS.
/// It is the one prefix spelling this tier can already serve, and it can
/// serve it precisely because the stored index is not a prefix index.
#[test]
fn a_full_length_char_prefix_is_an_ordinary_index() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE t (a VARCHAR(10), KEY idx (a(10)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO t VALUES ('abcdefghij'), ('x')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    // The index covers the whole column, so an equality still answers
    // exactly -- the read that a real prefix index would get wrong.
    let rows = run_select_on(
        "SELECT a FROM t WHERE a = 'abcdefghij'",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect::<Vec<_>>(),
        vec!["abcdefghij".to_owned()]
    );
}

/// A prefix TiDB accepts is REFUSED rather than built, because building it
/// would make the reads silently wrong: `SELECT a` answered from a 3-byte
/// index entry returns `'abc'` where real TiDB returns `'abcdef'`.
///
/// This is the refusal the cascade measurement is against, so it is pinned
/// too -- it must stay a refusal until the read path exists, not quietly
/// become a success.
#[test]
fn a_real_prefix_is_deferred_not_built() {
    for sql in [
        "CREATE TABLE t (a VARCHAR(20), KEY idx (a(3)))",
        "CREATE TABLE t (a TEXT, KEY idx (a(5)))",
        "CREATE TABLE t (a VARCHAR(20), UNIQUE KEY idx (a(3)))",
        "CREATE TABLE t (a INT, b VARCHAR(20), KEY idx (a, b(4)))",
        "CREATE TABLE t (a VARCHAR(20), PRIMARY KEY (a(3)))",
    ] {
        assert!(
            matches!(create_error(sql), DriverError::Unsupported(reason)
                if reason == prefix_unsupported()),
            "{sql}"
        );
    }
}

/// `CREATE INDEX` and `ALTER TABLE ... ADD INDEX` reach the same rules, so a
/// length illegal in `CREATE TABLE` is illegal there too -- and reported with
/// the same code rather than a generic refusal.
#[test]
fn create_index_validates_the_length_the_same_way() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE t (a INT, b VARCHAR(10), c BLOB)",
        &mut catalog,
    )
    .unwrap();

    assert!(matches!(
        crate::ddl::run_create_index_in(
            "CREATE INDEX i ON t (a(3))",
            &mut catalog,
            "test",
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::IncorrectPrefixKey)
    ));
    assert!(matches!(
        crate::ddl::run_create_index_in(
            "CREATE INDEX i ON t (b(20))",
            &mut catalog,
            "test",
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::IncorrectPrefixKey)
    ));
    assert!(matches!(
        crate::ddl::run_create_index_in("CREATE INDEX i ON t (c)", &mut catalog, "test", &crate::StmtContext::for_query()),
        Err(DriverError::BlobKeyWithoutLength(ref column)) if column == "c"
    ));
    assert!(matches!(
        crate::ddl::run_create_index_in("CREATE INDEX i ON t (b(0))", &mut catalog, "test", &crate::StmtContext::for_query()),
        Err(DriverError::KeyPart0(ref column)) if column == "b"
    ));
    // A full-length prefix is an ordinary index here too, so it SUCCEEDS.
    crate::ddl::run_create_index_in(
        "CREATE INDEX i ON t (b(10))",
        &mut catalog,
        "test",
        &crate::StmtContext::for_query(),
    )
    .unwrap();
}
