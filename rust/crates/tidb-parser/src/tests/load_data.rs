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

//! Exact `LOAD DATA` cases owned by the original Go parser and AST tests.

use super::*;

#[test]
fn parser_test_load_data_rows_match_go() {
    // Every LOAD DATA row in `pkg/parser/parser_test.go:715-794`. Keep the
    // count assertion so an upstream test addition cannot silently disappear
    // from this source-domain port.
    let cases = [
        ("load data local infile '/tmp/t.csv' into table t1 fields terminated by ',' optionally enclosed by '\"' ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES"),
        ("load data infile '/tmp/t.csv' into table t", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t`"),
        ("load data infile '/tmp/t.csv' into table t character set utf8", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` CHARACTER SET utf8"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"),
        ("load data infile '/tmp/t.csv' into table t lines starting by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` LINES STARTING BY 'ab'"),
        ("load data infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy'"),
        ("load data infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy'"),
        ("load data infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""),
        ("load data local infile '/tmp/t.csv' into table t", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t`"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data local infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"),
        ("load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` CHARACTER SET utf8 FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab'"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy'"),
        ("load data local infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""),
        ("load data infile '/tmp/t.csv' into table t (a,b)", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t columns terminated by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` CHARACTER SET utf8 FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` CHARACTER SET utf8 FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy' (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t (a,b) fields terminated by 'ab'", false, ""),
        ("load data local infile '/tmp/t.csv' into table t ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` IGNORE 1 LINES"),
        ("load data local infile '/tmp/t.csv' into table t ignore -1 lines", false, ""),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b) ignore 1 lines", false, ""),
        ("load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy' IGNORE 1 LINES"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' ignore 1 lines (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' IGNORE 1 LINES (`a`,`b`)"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by ''", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY ''"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by X'6B6B';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by X'6B6B' enclosed by X'0D';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk' ENCLOSED BY '\r'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by X'6B6B' enclosed by X'0D0D';", false, ""),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by B'110101101101011';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by B'110101101101011' enclosed by B'1101';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` FIELDS TERMINATED BY 'kk' ENCLOSED BY '\r'"),
        ("load data local infile '~/1.csv' into table `t_ascii` fields terminated by B'110101101101011' enclosed by B'110100001101';", false, ""),
        ("load data local infile '~/1.csv' into table `t_ascii` lines starting by B'110101101101011' terminated by B'110101101101011';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` LINES STARTING BY 'kk' TERMINATED BY 'kk'"),
        ("load data local infile '~/1.csv' into table `t_ascii` lines starting by X'6B6B' terminated by X'6B6B';", true, "LOAD DATA LOCAL INFILE '~/1.csv' IGNORE INTO TABLE `t_ascii` LINES STARTING BY 'kk' TERMINATED BY 'kk'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' escaped by '' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY ''"),
        ("load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' escaped by '' enclosed by 'b' SET b = CAST(CONV(MID(@var1, 3, LENGTH(@var1)-3), 2, 10) AS UNSIGNED)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '' SET `b`=CAST(CONV(MID(@`var1`, 3, LENGTH(@`var1`)-3), 2, 10) AS UNSIGNED)"),
        ("load data infile '/tmp/t.csv' into table t fields enclosed by ''", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ENCLOSED BY ''"),
        ("load data infile '/tmp/t.csv' into table t fields enclosed by 'a'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ENCLOSED BY 'a'"),
        ("load data infile '/tmp/t.csv' into table t fields enclosed by 'aa'", false, ""),
        ("load data infile '/tmp/t.csv' into table t fields escaped by ''", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ESCAPED BY ''"),
        ("load data infile '/tmp/t.csv' into table t fields escaped by 'a'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS ESCAPED BY 'a'"),
        ("load data infile '/tmp/t.csv' into table t fields escaped by 'aa'", false, ""),
        ("LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @dummy, column2, @dummy, column3)", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`dummy`,`column2`,@`dummy`,`column3`)"),
        ("LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @var1) SET column2 = @var1/100", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`var1`) SET `column2`=@`var1`/100"),
        ("LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @var1, @var2) SET column2 = @var1/100, column3 = DEFAULT, column4=CURRENT_TIMESTAMP, column5=@var2+1", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`var1`,@`var2`) SET `column2`=@`var1`/100, `column3`=DEFAULT, `column4`=CURRENT_TIMESTAMP(), `column5`=@`var2`+1"),
        ("LOAD DATA INFILE '/tmp/t.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("LOAD DATA LOCAL INFILE '/tmp/t.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("LOAD DATA LOCAL INFILE '/tmp/t.csv' REPLACE INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' REPLACE INTO TABLE `t1` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"),
        ("load data infile 's3://bucket-name/t.csv' into table t", true, "LOAD DATA INFILE 's3://bucket-name/t.csv' INTO TABLE `t`"),
        ("load data infile '/tmp/t.csv' into table t fields defined null by 'nil'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS DEFINED NULL BY 'nil'"),
        ("load data infile '/tmp/t.csv' into table t fields defined null by X'00'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS DEFINED NULL BY '\x00'"),
        ("load data infile '/tmp/t.csv' into table t fields defined null by 'NULL' optionally enclosed ignore 1 lines", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS DEFINED NULL BY 'NULL' OPTIONALLY ENCLOSED IGNORE 1 LINES"),
        ("load data infile '/tmp/t.csv' format 'delimited data' into table t (column1, @var1) SET column2 = @var1/100", true, "LOAD DATA INFILE '/tmp/t.csv' FORMAT 'delimited data' INTO TABLE `t` (`column1`,@`var1`) SET `column2`=@`var1`/100"),
        ("load data local infile '/tmp/t.sql' format 'sql file' replace into table t (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.sql' FORMAT 'sql file' REPLACE INTO TABLE `t` (`a`,`b`)"),
        ("load data infile '/tmp/t.parquet' format 'parquet' into table t (column1, @var1) SET column2 = @var1/100", true, "LOAD DATA INFILE '/tmp/t.parquet' FORMAT 'parquet' INTO TABLE `t` (`column1`,@`var1`) SET `column2`=@`var1`/100"),
        ("load data infile '/tmp/t.csv' into table t with detached", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` WITH detached"),
        ("load data infile '/tmp/t.csv' into table `t` with threads=10", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` WITH threads=10"),
        ("load data infile '/tmp/t.csv' into table `t` with threads=10, detached", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` WITH threads=10, detached"),
    ];
    assert_eq!(cases.len(), 73, "original LOAD DATA parser rows drifted");

    for (sql, accepted, expected) in cases {
        match (parse(sql), accepted) {
            (Ok(statement), true) => assert_eq!(statement.restore(), expected, "restore {sql}"),
            (Err(_), false) => {}
            (Ok(statement), false) => panic!(
                "Go rejects but Rust accepted {sql}: {}",
                statement.restore()
            ),
            (Err(error), true) => panic!("Go accepts but Rust rejected {sql}: {error:?}"),
        }
    }
}

#[test]
fn ast_test_load_data_restore_rows_match_go() {
    // Every row in `pkg/parser/ast/dml_test.go:415-509`'s
    // `TestLoadDataRestore`, retained separately from the broad parser table.
    let cases = [
        ("load data low_priority infile '/a.csv' into table `t`", "LOAD DATA LOW_PRIORITY INFILE '/a.csv' INTO TABLE `t`"),
        ("load data infile '/a.csv' format 'sql file' into table `t`", "LOAD DATA INFILE '/a.csv' FORMAT 'sql file' INTO TABLE `t`"),
        ("load data infile '/a.csv' format 'sql file' into table `t` character set utf8mb4", "LOAD DATA INFILE '/a.csv' FORMAT 'sql file' INTO TABLE `t` CHARACTER SET utf8mb4"),
        ("load data infile '/a.csv' format 'sql file' into table `t` character set gbk", "LOAD DATA INFILE '/a.csv' FORMAT 'sql file' INTO TABLE `t` CHARACTER SET gbk"),
        ("load data infile '/a.csv' into table `t` ignore 0 lines", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` IGNORE 0 LINES"),
        ("load data infile '/a.csv' into table `t` ignore 11 lines", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` IGNORE 11 LINES"),
        ("load data infile '/a.csv' into table `t` fields terminated by 'a\\t'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS TERMINATED BY 'a\t'"),
        ("load data infile '/a.csv' into table `t` FIELDS OPTIONALLY ENCLOSED BY 'a'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS OPTIONALLY ENCLOSED BY 'a'"),
        ("load data infile '/a.csv' into table `t` fields enclosed by 'a'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS ENCLOSED BY 'a'"),
        ("load data infile '/a.csv' into table `t` fields escaped by 'a'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS ESCAPED BY 'a'"),
        ("load data infile '/a.csv' into table `t` fields defined null by 'a'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS DEFINED NULL BY 'a'"),
        ("load data infile '/a.csv' into table `t` fields defined null by 'a' optionally enclosed", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS DEFINED NULL BY 'a' OPTIONALLY ENCLOSED"),
        ("load data infile '/a.csv' into table `t` fields defined null by 'a' optionally enclosed  optionally  enclosed  by  'a'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS OPTIONALLY ENCLOSED BY 'a' DEFINED NULL BY 'a' OPTIONALLY ENCLOSED"),
        ("load data infile '/a.csv' into table `t` fields optionally  enclosed  by  'a'  defined null by 'a' optionally enclosed", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` FIELDS OPTIONALLY ENCLOSED BY 'a' DEFINED NULL BY 'a' OPTIONALLY ENCLOSED"),
        ("load data infile '/a.csv' into table `t` lines starting by 'a'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` LINES STARTING BY 'a'"),
        ("load data infile '/a.csv' into table `t` lines terminated by '\\n'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` LINES TERMINATED BY '\n'"),
        ("load data infile '/a.csv' into table `t` lines starting by 'a' terminated by '\\n'", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` LINES STARTING BY 'a' TERMINATED BY '\n'"),
        ("load data infile '/a.csv' into table `t` with detached", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` WITH detached"),
        ("load data infile '/a.csv' into table `t` with batch_size=999,detached", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` WITH batch_size=999, detached"),
        ("load data infile '/a.csv' into table `t` with detached, batch_size=999", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` WITH detached, batch_size=999"),
        ("load data infile '/a.csv' into table `t` with detached, thread=-100, batch_size=999", "LOAD DATA INFILE '/a.csv' INTO TABLE `t` WITH detached, thread=-100, batch_size=999"),
        ("load data infile '/a.csv' format 'sql' into table `t` with detached, batch_size=999", "LOAD DATA INFILE '/a.csv' FORMAT 'sql' INTO TABLE `t` WITH detached, batch_size=999"),
    ];
    assert_eq!(
        cases.len(),
        22,
        "original LoadData AST restore rows drifted"
    );
    for (sql, expected) in cases {
        assert_eq!(r(sql), expected, "restore {sql}");
    }
}
