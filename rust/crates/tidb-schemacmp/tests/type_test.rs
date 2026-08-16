// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/util/schemacmp/type_test.go` near 1:1.

use tidb_datatype::{FieldType, FieldTypeBuilder, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_mysql::types::{
    AutoIncrementFlag, BinaryFlag, MultipleKeyFlag, NoDefaultValueFlag, NotNullFlag, TypeBit,
    TypeDate, TypeDatetime, TypeDuration, TypeEnum, TypeJSON, TypeLong, TypeLongBlob, TypeLonglong,
    TypeMediumBlob, TypeNewDecimal, TypeSet, TypeString, TypeTimestamp, TypeVarchar, TypeYear,
    UniqueKeyFlag, UnsignedFlag, ZerofillFlag,
};
use tidb_schemacmp::{IncompatibleError, Lattice, Typ, Value};

const BINARY: &str = "binary";

fn build_type(
    tp: u8,
    flag: usize,
    flen: i64,
    dec: i64,
    charset: &str,
    collate: &str,
    elems: &[&str],
) -> FieldType {
    FieldTypeBuilder::new()
        .with_code(FieldTypeCode::from_mysql_type(tp))
        .flags_set(u32::try_from(flag).expect("every MySQL column flag fits in 32 bits"))
        .flen_set(flen)
        .decimal_set(dec)
        .charset_set(charset)
        .collation_set(collate)
        .elems(elems.iter().copied())
        .build()
}

// The Go test file's fixture list, in declaration order.

/// INT
fn type_int() -> FieldType {
    build_type(TypeLong, 0, 11, 0, BINARY, BINARY, &[])
}

/// INT NOT NULL
fn type_int_not_null() -> FieldType {
    build_type(
        TypeLong,
        NoDefaultValueFlag | NotNullFlag,
        10,
        0,
        BINARY,
        BINARY,
        &[],
    )
}

/// INT AUTO_INCREMENT UNIQUE
fn type_int_auto_increment_unique() -> FieldType {
    build_type(
        TypeLong,
        AutoIncrementFlag | UniqueKeyFlag,
        11,
        0,
        BINARY,
        BINARY,
        &[],
    )
}

/// INT NOT NULL, KEY
fn type_int_not_null_key() -> FieldType {
    build_type(
        TypeLong,
        NoDefaultValueFlag | MultipleKeyFlag | NotNullFlag,
        11,
        0,
        BINARY,
        BINARY,
        &[],
    )
}

/// INT(1)
fn type_int1() -> FieldType {
    build_type(TypeLong, 0, 1, 0, BINARY, BINARY, &[])
}

/// INT(22)
fn type_int22() -> FieldType {
    build_type(TypeLong, 0, 22, 0, BINARY, BINARY, &[])
}

/// BIT(4)
fn type_bit4() -> FieldType {
    build_type(TypeBit, UnsignedFlag, 4, 0, BINARY, BINARY, &[])
}

/// BIGINT(22) ZEROFILL
fn type_big_int22_zero_fill() -> FieldType {
    build_type(
        TypeLonglong,
        ZerofillFlag | UnsignedFlag,
        22,
        0,
        BINARY,
        BINARY,
        &[],
    )
}

/// DECIMAL(16, 8) DEFAULT 2.5
fn type_decimal16_8() -> FieldType {
    build_type(TypeNewDecimal, 0, 16, 8, BINARY, BINARY, &[])
}

/// DECIMAL
fn type_decimal() -> FieldType {
    build_type(TypeNewDecimal, 0, 11, 0, BINARY, BINARY, &[])
}

/// DATE
fn type_date() -> FieldType {
    build_type(TypeDate, BinaryFlag, 10, 0, BINARY, BINARY, &[])
}

/// DATETIME(3)
fn type_date_time3() -> FieldType {
    build_type(TypeDatetime, BinaryFlag, 23, 3, BINARY, BINARY, &[])
}

/// TIMESTAMP
fn type_timestamp() -> FieldType {
    build_type(TypeTimestamp, BinaryFlag, 19, 0, BINARY, BINARY, &[])
}

/// TIME(6)
fn type_time6() -> FieldType {
    build_type(TypeDuration, BinaryFlag, 17, 6, BINARY, BINARY, &[])
}

/// YEAR(4)
fn type_year4() -> FieldType {
    build_type(
        TypeYear,
        ZerofillFlag | UnsignedFlag,
        4,
        0,
        BINARY,
        BINARY,
        &[],
    )
}

/// CHAR(123)
fn type_char123() -> FieldType {
    build_type(
        TypeString,
        0,
        123,
        0,
        tidb_mysql::charset::UTF8MB4Charset,
        tidb_mysql::charset::UTF8MB4DefaultCollation,
        &[],
    )
}

/// VARCHAR(10) CHARSET utf8 COLLATE utf8_bin
fn type_varchar10_utf8_bin() -> FieldType {
    build_type(
        TypeVarchar,
        0,
        10,
        0,
        tidb_mysql::charset::UTF8Charset,
        "utf8_bin",
        &[],
    )
}

/// VARCHAR(10) CHARSET utf8 COLLATE utf8_general_ci
fn type_varchar10_utf8_general_ci() -> FieldType {
    build_type(
        TypeVarchar,
        0,
        10,
        0,
        tidb_mysql::charset::UTF8Charset,
        "utf8_general_ci",
        &[],
    )
}

/// VARCHAR(10) CHARSET utf8mb4 COLLATE utf8mb4_bin
fn type_varchar10_utf8mb4_bin() -> FieldType {
    build_type(
        TypeVarchar,
        0,
        10,
        0,
        tidb_mysql::charset::UTF8MB4Charset,
        tidb_mysql::charset::UTF8MB4DefaultCollation,
        &[],
    )
}

/// VARCHAR(10) CHARSET latin1 COLLATE latin1_bin
fn type_varchar10_latin1_bin() -> FieldType {
    build_type(TypeVarchar, 0, 10, 0, "latin1", "latin1_bin", &[])
}

/// VARCHAR(65432) CHARSET ascii
fn type_varchar65432_charset_ascii() -> FieldType {
    build_type(TypeVarchar, 0, 65432, 0, "ascii", "ascii_bin", &[])
}

/// BINARY(69)
fn type_binary69() -> FieldType {
    build_type(TypeString, BinaryFlag, 69, 0, BINARY, BINARY, &[])
}

/// VARBINARY(420)
fn type_var_binary420() -> FieldType {
    build_type(TypeVarchar, BinaryFlag, 420, 0, BINARY, BINARY, &[])
}

/// LONGBLOB
fn type_long_blob() -> FieldType {
    build_type(
        TypeLongBlob,
        BinaryFlag,
        0xffff_ffff,
        0,
        BINARY,
        BINARY,
        &[],
    )
}

/// MEDIUMTEXT
fn type_medium_text() -> FieldType {
    build_type(
        TypeMediumBlob,
        0,
        0xffff_ffff,
        0,
        tidb_mysql::charset::UTF8MB4Charset,
        tidb_mysql::charset::UTF8MB4DefaultCollation,
        &[],
    )
}

/// ENUM('tidb', 'tikv', 'tiflash', 'golang', 'rust')
fn type_enum5() -> FieldType {
    build_type(
        TypeEnum,
        0,
        UNSPECIFIED_LENGTH,
        0,
        tidb_mysql::charset::UTF8MB4Charset,
        tidb_mysql::charset::UTF8MB4DefaultCollation,
        &["tidb", "tikv", "tiflash", "golang", "rust"],
    )
}

/// ENUM('tidb', 'tikv')
fn type_enum2() -> FieldType {
    build_type(
        TypeEnum,
        0,
        UNSPECIFIED_LENGTH,
        0,
        tidb_mysql::charset::UTF8MB4Charset,
        tidb_mysql::charset::UTF8MB4DefaultCollation,
        &["tidb", "tikv"],
    )
}

/// SET('tidb', 'tikv', 'tiflash', 'golang', 'rust')
fn type_set5() -> FieldType {
    build_type(
        TypeSet,
        0,
        UNSPECIFIED_LENGTH,
        0,
        tidb_mysql::charset::UTF8MB4Charset,
        tidb_mysql::charset::UTF8MB4DefaultCollation,
        &["tidb", "tikv", "tiflash", "golang", "rust"],
    )
}

/// SET('tidb', 'tikv')
fn type_set2() -> FieldType {
    build_type(
        TypeSet,
        0,
        UNSPECIFIED_LENGTH,
        0,
        tidb_mysql::charset::UTF8MB4Charset,
        tidb_mysql::charset::UTF8MB4DefaultCollation,
        &["tidb", "tikv"],
    )
}

/// JSON
fn type_json() -> FieldType {
    build_type(TypeJSON, BinaryFlag, 0xffff_ffff, 0, BINARY, BINARY, &[])
}

fn unwrap_field_type(value: Value) -> FieldType {
    let Value::FieldType(field_type) = value else {
        panic!("expected a field type, got {value:?}");
    };
    *field_type
}

// Go `TestTypeUnwrap`.
#[test]
fn test_type_unwrap() {
    let test_cases = [
        type_int(),
        type_int_not_null(),
        type_int_auto_increment_unique(),
        type_int_not_null_key(),
        type_int1(),
        type_int22(),
        type_bit4(),
        type_big_int22_zero_fill(),
        type_decimal16_8(),
        type_decimal(),
        type_date(),
        type_date_time3(),
        type_timestamp(),
        type_time6(),
        type_year4(),
        type_char123(),
        type_varchar10_utf8_bin(),
        type_varchar10_utf8_general_ci(),
        type_varchar10_utf8mb4_bin(),
        type_varchar10_latin1_bin(),
        type_varchar65432_charset_ascii(),
        type_binary69(),
        type_var_binary420(),
        type_long_blob(),
        type_medium_text(),
        type_enum5(),
        type_enum2(),
        type_set5(),
        type_set2(),
        type_json(),
    ];

    for tc in &test_cases {
        let tt = Typ::new(tc);
        assert_eq!(&unwrap_field_type(tt.unwrap()), tc);
    }
}

struct Case {
    a: FieldType,
    b: FieldType,
    compare_result: i32,
    compare_error: &'static str,
    join: Option<FieldType>,
    join_error: &'static str,
}

fn assert_regexp(pattern: &str, error: &IncompatibleError) {
    let re = regex::Regex::new(pattern).expect("the Go test pattern compiles");
    assert!(
        re.is_match(&error.to_string()),
        "error {error:?} does not match {pattern:?}"
    );
}

// Go `TestTypeCompareJoin`.
#[test]
fn test_type_compare_join() {
    let test_cases = vec![
        Case {
            a: type_int(),
            b: type_int22(),
            compare_result: -1,
            compare_error: "",
            join: Some(type_int22()),
            join_error: "",
        },
        Case {
            a: type_int1(),
            b: type_int(),
            compare_result: -1,
            compare_error: "",
            join: Some(type_int()),
            join_error: "",
        },
        Case {
            a: type_int(),
            b: type_int_not_null(),
            compare_result: 1,
            compare_error: "",
            join: Some(type_int()),
            join_error: "",
        },
        Case {
            // utf8 < utf8mb4 (both charset and collation are ordered).
            a: type_varchar10_utf8_bin(),
            b: type_varchar10_utf8mb4_bin(),
            compare_result: -1,
            compare_error: "",
            join: Some(type_varchar10_utf8mb4_bin()),
            join_error: "",
        },
        Case {
            // latin1 < utf8mb4 (both charset and collation are ordered).
            a: type_varchar10_latin1_bin(),
            b: type_varchar10_utf8mb4_bin(),
            compare_result: -1,
            compare_error: "",
            join: Some(type_varchar10_utf8mb4_bin()),
            join_error: "",
        },
        Case {
            // latin1 and utf8 are not comparable, but their join is utf8mb4.
            a: type_varchar10_latin1_bin(),
            b: type_varchar10_utf8_bin(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: incompatible charset.*",
            join: Some(type_varchar10_utf8mb4_bin()),
            join_error: "",
        },
        Case {
            // Only collations with the same suffix can be ordered/joined.
            a: type_varchar10_utf8_general_ci(),
            b: type_varchar10_utf8mb4_bin(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: incompatible collation.*",
            join: None,
            join_error: r"at tuple index \d+: incompatible collation.*",
        },
        Case {
            // Cannot join DEFAULT NULL with AUTO_INCREMENT.
            a: type_int(),
            b: type_int_auto_increment_unique(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: distinct singletons.*", // TODO(from Go): Improve error messages.
            join: None,
            join_error: r"at tuple index \d+: distinct singletons.*",
        },
        Case {
            // INT NOT NULL <join> INT AUTO_INC UNIQUE = INT AUTO_INC,
            // but an AUTO_INC column must be defined with a key, so the join
            // is invalid.
            a: type_int_not_null(),
            b: type_int_auto_increment_unique(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: combining contradicting orders.*",
            join: None,
            join_error: r"auto type but not defined as a key",
        },
        Case {
            // INT NOT NULL KEY <join> INT AUTO_INC UNIQUE = INT AUTO_INC KEY,
            a: type_int_not_null_key(),
            b: type_int_auto_increment_unique(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: combining contradicting orders.*",
            join: Some(build_type(
                TypeLong,
                AutoIncrementFlag | MultipleKeyFlag,
                11,
                0,
                BINARY,
                BINARY,
                &[],
            )),
            join_error: "",
        },
        Case {
            // DECIMAL of differet Flen/Decimal cannot be compared
            a: type_decimal16_8(),
            b: type_decimal(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: distinct singletons.*",
            join: None,
            join_error: r"at tuple index \d+: distinct singletons.*",
        },
        Case {
            a: type_varchar65432_charset_ascii(),
            b: type_var_binary420(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: distinct singletons.*",
            join: None,
            join_error: r"at tuple index \d+: distinct singletons.*",
        },
        Case {
            a: type_enum5(),
            b: type_enum2(),
            compare_result: 1,
            compare_error: "",
            join: Some(type_enum5()),
            join_error: "",
        },
        Case {
            a: type_set2(),
            b: type_set5(),
            compare_result: -1,
            compare_error: "",
            join: Some(type_set5()),
            join_error: "",
        },
        Case {
            a: type_set5(),
            b: type_enum5(),
            compare_result: 0,
            compare_error: r"at tuple index \d+: incompatible mysql type.*",
            join: None,
            join_error: r"at tuple index \d+: incompatible mysql type.*",
        },
    ];

    for tc in &test_cases {
        let a = Typ::new(&tc.a);
        let b = Typ::new(&tc.b);
        let cmp = a.compare(&b);
        if !tc.compare_error.is_empty() {
            assert_regexp(tc.compare_error, &cmp.unwrap_err());
        } else {
            assert_eq!(cmp.unwrap(), tc.compare_result);
        }

        let cmp = b.compare(&a);
        if !tc.compare_error.is_empty() {
            assert_regexp(tc.compare_error, &cmp.unwrap_err());
        } else {
            assert_eq!(cmp.unwrap(), -tc.compare_result);
        }

        let wrapped_join = a.join(&b);
        if !tc.join_error.is_empty() {
            assert_regexp(tc.join_error, &wrapped_join.unwrap_err());
        } else {
            let wrapped_join = wrapped_join.unwrap();
            assert_eq!(
                &unwrap_field_type(wrapped_join.unwrap()),
                tc.join.as_ref().expect("a join expectation"),
            );
            let cmp = wrapped_join.compare(&a).unwrap();
            assert!(cmp >= 0);

            let cmp = wrapped_join.compare(&b).unwrap();
            assert!(cmp >= 0);
        }
    }
}
