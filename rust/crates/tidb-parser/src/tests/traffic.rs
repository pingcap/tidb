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

//! Tests mapped from Go's traffic parser and statistics AST suites.

use super::*;
use tidb_ast::{
    AdminStmt, RefreshStatsMode, StatsObject, Stmt, TrafficCaptureOption, TrafficReplayOption,
    TrafficStmt,
};

#[test]
fn traffic_capture_restore_shape_and_rejections_match_go() {
    for (sql, restored) in [
        (
            "traffic capture to '/tmp' duration='1s' encryption_method='aes' compress=true",
            "TRAFFIC CAPTURE TO '/tmp' DURATION = '1s' ENCRYPTION_METHOD = 'aes' COMPRESS = TRUE",
        ),
        (
            "traffic capture to '/tmp' duration '1s' encryption_method 'aes' compress true",
            "TRAFFIC CAPTURE TO '/tmp' DURATION = '1s' ENCRYPTION_METHOD = 'aes' COMPRESS = TRUE",
        ),
        (
            "traffic capture to '/tmp' encryption_method='aes' duration='1s'",
            "TRAFFIC CAPTURE TO '/tmp' ENCRYPTION_METHOD = 'aes' DURATION = '1s'",
        ),
        (
            "traffic capture to '/tmp' duration='1m'",
            "TRAFFIC CAPTURE TO '/tmp' DURATION = '1m'",
        ),
        (
            "traffic capture to '/tmp' duration=''",
            "TRAFFIC CAPTURE TO '/tmp' DURATION = ''",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }

    let statement = parse("traffic capture to '/tmp' compress=false duration='1s'")
        .expect("TRAFFIC CAPTURE parses");
    assert!(matches!(
        statement,
        Stmt::Admin(admin)
            if matches!(admin.as_ref(), AdminStmt::Traffic(traffic)
                if matches!(traffic.as_ref(), TrafficStmt::Capture { dir, options }
                    if dir == "/tmp"
                        && matches!(options.as_slice(), [
                            TrafficCaptureOption::Compress(false),
                            TrafficCaptureOption::Duration(value),
                        ] if value == "1s")))
    ));

    for sql in [
        "traffic capture to '/tmp' duration='1'",
        "traffic capture to '/tmp' duration=1s",
        "traffic capture to '/tmp' compress='true'",
        "traffic capture duration='1m'",
        "traffic capture",
    ] {
        assert!(parse(sql).is_err(), "invalid Go case accepted: {sql}");
    }
}

#[test]
fn traffic_replay_restore_shape_and_source_token_rules_match_go() {
    for (sql, restored) in [
        (
            "traffic replay from '/tmp' user='root' password='123456' speed=1.0 read_only=true",
            "TRAFFIC REPLAY FROM '/tmp' USER = 'root' PASSWORD = '123456' SPEED = 1.0 READONLY = TRUE",
        ),
        (
            "traffic replay from '/tmp' user 'root' password '123456' speed 1.0 read_only true",
            "TRAFFIC REPLAY FROM '/tmp' USER = 'root' PASSWORD = '123456' SPEED = 1.0 READONLY = TRUE",
        ),
        (
            "traffic replay from '/tmp' speed 1.0 user='root'",
            "TRAFFIC REPLAY FROM '/tmp' SPEED = 1.0 USER = 'root'",
        ),
        (
            "traffic replay from '/tmp' speed=1",
            "TRAFFIC REPLAY FROM '/tmp' SPEED = 1",
        ),
        (
            "traffic replay from '/tmp' speed=0.5",
            "TRAFFIC REPLAY FROM '/tmp' SPEED = 0.5",
        ),
        // The hand parser consumes one token for SPEED and READ_ONLY rather
        // than re-imposing the old yacc token class. These are source
        // behavior, confirmed against `godump`, not inferred extensions.
        (
            "traffic replay from '/tmp' speed=x read_only=garbage",
            "TRAFFIC REPLAY FROM '/tmp' SPEED = x READONLY = FALSE",
        ),
        (
            "traffic replay from '/tmp' speed='1.2' read_only='true'",
            "TRAFFIC REPLAY FROM '/tmp' SPEED = 1.2 READONLY = TRUE",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }

    let statement = parse("traffic replay from '/tmp' user='root' read_only=false")
        .expect("TRAFFIC REPLAY parses");
    assert!(matches!(
        statement,
        Stmt::Admin(admin)
            if matches!(admin.as_ref(), AdminStmt::Traffic(traffic)
                if matches!(traffic.as_ref(), TrafficStmt::Replay { dir, options }
                    if dir == "/tmp"
                        && matches!(options.as_slice(), [
                            TrafficReplayOption::User(user),
                            TrafficReplayOption::ReadOnly(false),
                        ] if user == "root")))
    ));

    for sql in [
        "traffic replay from '/tmp' speed=-1",
        "traffic replay speed=1",
        "traffic replay",
    ] {
        assert!(parse(sql).is_err(), "invalid Go case accepted: {sql}");
    }
}

#[test]
fn traffic_job_commands_and_invalid_operations_match_go() {
    assert_eq!(r("show traffic jobs"), "SHOW TRAFFIC JOBS");
    assert_eq!(r("cancel traffic jobs"), "CANCEL TRAFFIC JOBS");
    assert!(matches!(
        parse("show traffic jobs"),
        Ok(Stmt::Admin(admin))
            if matches!(admin.as_ref(), AdminStmt::Traffic(traffic)
                if matches!(traffic.as_ref(), TrafficStmt::ShowJobs))
    ));
    assert!(matches!(
        parse("cancel traffic jobs"),
        Ok(Stmt::Admin(admin))
            if matches!(admin.as_ref(), AdminStmt::Traffic(traffic)
                if matches!(traffic.as_ref(), TrafficStmt::CancelJobs))
    ));

    for sql in [
        "show traffic jobs duration='1m'",
        "show traffic",
        "cancel traffic jobs duration='1m'",
        "cancel traffic",
        "traffic test",
        "traffic",
    ] {
        assert!(parse(sql).is_err(), "invalid Go case accepted: {sql}");
    }
}

#[test]
fn traffic_secure_text_redacts_url_credentials_and_password() {
    for (sql, secured) in [
        (
            "traffic capture to 's3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true' duration='1m'",
            "TRAFFIC CAPTURE TO 's3://bucket/prefix?access-key=xxxxxx&force-path-style=true&secret-access-key=xxxxxx' DURATION = '1m'",
        ),
        (
            "traffic replay from 's3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true' user='root' password='123456'",
            "TRAFFIC REPLAY FROM 's3://bucket/prefix?access-key=xxxxxx&force-path-style=true&secret-access-key=xxxxxx' USER = 'root' PASSWORD = 'xxxxxx'",
        ),
    ] {
        let Stmt::Admin(admin) = parse(sql).expect("traffic statement parses") else {
            panic!("expected admin statement");
        };
        let AdminStmt::Traffic(traffic) = admin.as_ref() else {
            panic!("expected traffic statement");
        };
        assert_eq!(traffic.secure_text(), secured, "{sql}");
    }
}

#[test]
fn refresh_stats_restore_and_shape_match_go() {
    for (sql, restored) in [
        ("REFRESH STATS *.*", "REFRESH STATS *.*"),
        ("refresh stats *.*", "REFRESH STATS *.*"),
        ("REFRESH STATS db1.*", "REFRESH STATS `db1`.*"),
        ("REFRESH STATS db1.t1", "REFRESH STATS `db1`.`t1`"),
        ("REFRESH STATS table1", "REFRESH STATS `table1`"),
        (
            "REFRESH STATS table1, table2",
            "REFRESH STATS `table1`, `table2`",
        ),
        (
            "REFRESH STATS *.*, db1.*, db2.t1, table1, table2",
            "REFRESH STATS *.*, `db1`.*, `db2`.`t1`, `table1`, `table2`",
        ),
        ("REFRESH STATS table1 full", "REFRESH STATS `table1` FULL"),
        (
            "REFRESH STATS table1 cluster",
            "REFRESH STATS `table1` CLUSTER",
        ),
        (
            "REFRESH STATS db1.* lite cluster",
            "REFRESH STATS `db1`.* LITE CLUSTER",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }

    let statement = parse("refresh stats db1.* lite cluster").expect("REFRESH STATS parses");
    assert!(matches!(
        statement,
        Stmt::Admin(admin)
            if matches!(admin.as_ref(), AdminStmt::RefreshStats(refresh)
                if refresh.mode == Some(RefreshStatsMode::Lite)
                    && refresh.cluster_wide
                    && matches!(refresh.objects.as_slice(), [StatsObject::Database(name)] if name == "db1"))
    ));

    for sql in [
        "refresh",
        "refresh stats",
        "refresh stats db.",
        "refresh stats *",
        "refresh stats t,",
        "refresh stats t cluster full",
    ] {
        assert!(parse(sql).is_err(), "invalid stats target accepted: {sql}");
    }
}

#[test]
fn refresh_stats_dedup_matches_go() {
    for (sql, restored) in [
        (
            "REFRESH STATS table1, db1.t1, *.*, db2.t2",
            "REFRESH STATS *.*",
        ),
        (
            "REFRESH STATS db1.t1, db2.t1, db1.*, db2.t2",
            "REFRESH STATS `db2`.`t1`, `db1`.*, `db2`.`t2`",
        ),
        (
            "REFRESH STATS db1.t1, db1.T1, db2.t1",
            "REFRESH STATS `db1`.`t1`, `db2`.`t1`",
        ),
        (
            "REFRESH STATS table1, table1, table2",
            "REFRESH STATS `table1`, `table2`",
        ),
        (
            "REFRESH STATS db1.*, DB1.*, db2.t1",
            "REFRESH STATS `db1`.*, `db2`.`t1`",
        ),
        (
            "REFRESH STATS `a.b`.`c`, `a`.`b.c`",
            "REFRESH STATS `a.b`.`c`, `a`.`b.c`",
        ),
    ] {
        let Stmt::Admin(admin) = parse(sql).expect("REFRESH STATS parses") else {
            panic!("expected admin statement");
        };
        let AdminStmt::RefreshStats(mut refresh) = *admin else {
            panic!("expected refresh stats statement");
        };
        refresh.dedup();
        assert_eq!(
            Stmt::Admin(Box::new(AdminStmt::RefreshStats(refresh))).restore(),
            restored,
            "{sql}"
        );
    }
}
