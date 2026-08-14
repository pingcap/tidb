//! Current Go `pkg/expression/builtin_info.go` contract for
//! `TIDB_ENCODE_SQL_DIGEST`.

use tidb_datatype::{Datum, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_session::{Session, StmtOutput};

fn digest(sql: &str) -> Datum {
    Datum::new_string(tidb_parser::normalize_digest(sql).1.to_string())
}

#[test]
fn tidb_encode_sql_digest_uses_the_parser_normalizer() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT TIDB_ENCODE_SQL_DIGEST('select * from b where id = 1'), \
                    TIDB_ENCODE_SQL_DIGEST('select * from b where id = ''1'''), \
                    TIDB_ENCODE_SQL_DIGEST('select * from b where id =2'), \
                    TIDB_ENCODE_SQL_DIGEST('select a from b where id = 1'), \
                    TIDB_ENCODE_SQL_DIGEST(123), \
                    TIDB_ENCODE_SQL_DIGEST(NULL)",
        )
        .unwrap()
    else {
        panic!("SQL digest encoding did not return a result set")
    };

    for (_, field_type) in &columns {
        assert_eq!(field_type.code(), FieldTypeCode::VarString);
        assert_eq!(field_type.flen(), UNSPECIFIED_LENGTH);
    }
    let expected = digest("select * from b where id = 1");
    assert_eq!(rows[0][0], expected);
    assert_eq!(rows[0][1], expected);
    assert_eq!(rows[0][2], expected);
    assert_eq!(rows[0][3], digest("select a from b where id = 1"));
    assert_ne!(rows[0][3], expected);
    assert_eq!(rows[0][4], digest("123"));
    assert_eq!(rows[0][5], Datum::Null);
}
