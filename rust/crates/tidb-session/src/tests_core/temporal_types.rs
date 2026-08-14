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

#[test]
fn date_constructors_return_native_dates() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns(
            "SELECT LAST_DAY('2024-02-10'), MAKEDATE(2024, 60), \
                    FROM_DAYS(TO_DAYS('2024-02-29'))",
        )
        .unwrap()
    else {
        panic!("date constructors did not return rows")
    };
    for (column, value) in columns.iter().zip(&rows[0]) {
        assert_eq!(column.1.code(), tidb_datatype::FieldTypeCode::Date);
        assert_eq!((column.1.flen(), column.1.decimal()), (10, 0));
        assert!(matches!(value, Datum::Time(_)));
        assert_eq!(value.sql_string().unwrap(), "2024-02-29");
    }
}

#[test]
fn duration_constructors_preserve_source_scale() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns(
            "SELECT SEC_TO_TIME(CAST(1.25 AS DECIMAL(10,2))), \
                    MAKETIME(1, 2, CAST(3.456 AS DECIMAL(10,3)))",
        )
        .unwrap()
    else {
        panic!("duration constructors did not return rows")
    };
    for (index, flen, decimal, value) in [(0, 13, 2, "00:00:01.25"), (1, 14, 3, "01:02:03.456")] {
        assert_eq!(
            columns[index].1.code(),
            tidb_datatype::FieldTypeCode::Duration
        );
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (flen, decimal)
        );
        assert!(matches!(rows[0][index], Datum::Duration(_)));
        assert_eq!(rows[0][index].sql_string().unwrap(), value);
    }
}
