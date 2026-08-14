//! Native result domains and metadata for temporal builtins.

use crate::*;

#[test]
fn current_clock_builtins_return_native_temporal_values() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session.run("SET timestamp = 1700000000").unwrap();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns(
            "SELECT NOW(3), UTC_TIMESTAMP(6), CURDATE(), UTC_DATE(), \
                    CURTIME(3), UTC_TIME(6)",
        )
        .unwrap()
    else {
        panic!("current-clock builtins did not return rows")
    };

    for (index, flen, decimal) in [(0, 23, 3), (1, 26, 6)] {
        assert_eq!(
            columns[index].1.code(),
            tidb_datatype::FieldTypeCode::Datetime
        );
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (flen, decimal)
        );
        assert!(matches!(rows[0][index], Datum::Time(_)));
    }
    for index in [2, 3] {
        assert_eq!(columns[index].1.code(), tidb_datatype::FieldTypeCode::Date);
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (10, 0)
        );
        assert!(matches!(rows[0][index], Datum::Time(_)));
    }
    for (index, flen, decimal) in [(4, 12, 3), (5, 15, 6)] {
        assert_eq!(
            columns[index].1.code(),
            tidb_datatype::FieldTypeCode::Duration
        );
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (flen, decimal)
        );
        assert!(matches!(rows[0][index], Datum::Duration(_)));
    }
}
