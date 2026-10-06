//! JSON_STORAGE_FREE always answers 0 for parsed documents (TiDB's binary
//! form reserves no free space) and JSON_STORAGE_SIZE answers the binary
//! payload length plus its one-byte root type code — both from
//! `builtin_ext/json2.rs`, mirroring `builtinJSONStorage*Sig`.

use tidb_session::Session;

use crate::support::try_tagged_integer_rows_70 as try_sql;

#[test]
fn storage_semantics() {
    let mut session = Session::new();

    // Parsed documents reserve no free space.
    assert_eq!(
        try_sql(&mut session, "select json_storage_free('{\"a\": 1}')"),
        "i:0"
    );

    // The size is the binary payload length plus the type byte.
    let size = try_sql(&mut session, "select json_storage_size('{\"a\": 1}')");
    let value: i64 = size.trim_start_matches("i:").parse().expect("int size");
    assert!(value > 1 && value < 64, "plausible binary size: {size}");

    // SQL NULL propagates.
    assert_eq!(try_sql(&mut session, "select json_storage_size(null)"), "Null");
}
