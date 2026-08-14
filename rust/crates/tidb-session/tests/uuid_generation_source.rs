//! Current Go `pkg/expression/builtin_miscellaneous.go` UUID generation
//! contracts.

use tidb_datatype::{Datum, FieldTypeCode};
use tidb_session::{Session, StmtOutput};

fn text(value: &Datum) -> String {
    value
        .sql_string()
        .expect("UUID generators return ASCII text")
}

fn assert_uuid_shape(value: &str, version: char) {
    assert_eq!(value.len(), 36);
    assert_eq!(&value[8..9], "-");
    assert_eq!(&value[13..14], "-");
    assert_eq!(&value[18..19], "-");
    assert_eq!(&value[23..24], "-");
    assert_eq!(value.as_bytes()[14] as char, version);
    assert!(matches!(value.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
    assert!(value
        .bytes()
        .filter(|byte| *byte != b'-')
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
}

#[test]
fn uuid_generators_are_reachable_unique_and_versioned() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT UUID(), UUID(), UUID_V4(), UUID_V4(), UUID_V7(), UUID_V7(), \
                    UUID_VERSION(UUID()), UUID_VERSION(UUID_V4()), UUID_VERSION(UUID_V7()), \
                    UUID_TIMESTAMP(UUID()), UUID_TIMESTAMP(UUID_V4()), UUID_TIMESTAMP(UUID_V7())",
        )
        .unwrap()
    else {
        panic!("UUID generation did not return a result set")
    };

    for (_, field_type) in &columns[..6] {
        assert_eq!(field_type.code(), FieldTypeCode::VarString);
        assert_eq!(field_type.flen(), 36);
    }
    let row = &rows[0];
    let generated: Vec<String> = row[..6].iter().map(text).collect();
    for value in &generated[..2] {
        assert_uuid_shape(value, '1');
    }
    for value in &generated[2..4] {
        assert_uuid_shape(value, '4');
    }
    for value in &generated[4..6] {
        assert_uuid_shape(value, '7');
    }
    assert_ne!(generated[0], generated[1]);
    assert_ne!(generated[2], generated[3]);
    assert!(generated[4] < generated[5]);
    assert_eq!(&row[6..9], &[Datum::Int(1), Datum::Int(4), Datum::Int(7)]);
    assert!(matches!(row[9], Datum::Decimal(_)));
    assert_eq!(row[10], Datum::Null);
    assert!(matches!(row[11], Datum::Decimal(_)));
}
