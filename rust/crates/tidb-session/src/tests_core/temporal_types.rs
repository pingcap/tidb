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

#[test]
fn str_to_date_uses_the_format_to_choose_its_native_domain() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns(
            "SELECT STR_TO_DATE('2024-02-29', '%Y-%m-%d'), \
                    STR_TO_DATE('12:34:56.123456', '%H:%i:%s.%f'), \
                    STR_TO_DATE('2024-02-29 12:34:56.123456', '%Y-%m-%d %H:%i:%s.%f')",
        )
        .unwrap()
    else {
        panic!("STR_TO_DATE did not return rows")
    };
    for (index, code, flen, decimal) in [
        (0, tidb_datatype::FieldTypeCode::Date, 10, 0),
        (1, tidb_datatype::FieldTypeCode::Duration, 17, 6),
        (2, tidb_datatype::FieldTypeCode::Datetime, 26, 6),
    ] {
        assert_eq!(columns[index].1.code(), code);
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (flen, decimal)
        );
    }
    assert!(matches!(rows[0][0], Datum::Time(_)));
    assert!(matches!(rows[0][1], Datum::Duration(_)));
    assert!(matches!(rows[0][2], Datum::Time(_)));

    session.run("CREATE TABLE f (format VARCHAR(20))").unwrap();
    session.run("INSERT INTO f VALUES ('%Y-%m-%d')").unwrap();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns("SELECT STR_TO_DATE('2024-02-29', format) FROM f")
        .unwrap()
    else {
        panic!("dynamic STR_TO_DATE did not return rows")
    };
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (26, 6));
    assert!(matches!(rows[0][0], Datum::Time(_)));

    session.run("UPDATE f SET format = '%H:%i:%s'").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT STR_TO_DATE('12:34:56', format) FROM f")
        .unwrap()
    else {
        panic!("dynamic time-only STR_TO_DATE did not return rows")
    };
    assert_eq!(rows[0][0], Datum::Null);
    session.run("SET sql_mode = ''").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT STR_TO_DATE('12:34:56', format) FROM f")
        .unwrap()
    else {
        panic!("relaxed dynamic STR_TO_DATE did not return rows")
    };
    assert!(matches!(rows[0][0], Datum::Time(_)));
    assert_eq!(
        rows[0][0].sql_string().unwrap(),
        "0000-00-00 12:34:56.000000"
    );
}

#[test]
fn temporal_difference_and_zone_conversion_return_native_values() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns(
            "SELECT TIMEDIFF('2024-01-02 00:00:00.123', '2024-01-01 23:59:59.120'), \
                    CONVERT_TZ('2024-01-01 00:00:00.123', '+00:00', '+08:00'), \
                    CONVERT_TZ('bad.prefix.12', '+00:00', '+08:00')",
        )
        .unwrap()
    else {
        panic!("temporal conversion builtins did not return rows")
    };
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Duration);
    assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (14, 3));
    assert!(matches!(rows[0][0], Datum::Duration(_)));
    assert_eq!(rows[0][0].sql_string().unwrap(), "00:00:01.003");
    assert_eq!(columns[1].1.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((columns[1].1.flen(), columns[1].1.decimal()), (23, 3));
    assert!(matches!(rows[0][1], Datum::Time(_)));
    assert_eq!(rows[0][1].sql_string().unwrap(), "2024-01-01 08:00:00.123");
    assert_eq!(columns[2].1.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((columns[2].1.flen(), columns[2].1.decimal()), (22, 2));
    assert_eq!(rows[0][2], Datum::Null);
}
