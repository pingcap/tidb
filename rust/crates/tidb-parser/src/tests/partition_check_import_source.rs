// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Direct source rows for Go's CHECK/IMPORT PARTITION actions.

use super::*;

/// Exact Go `TestDDL` CHECK PARTITION rows at `pkg/parser/parser_test.go:3208-3210`.
#[test]
fn alter_check_partition_restores_go_rows() {
    for (sql, expected) in [
        (
            "alter table t check partition all",
            "ALTER TABLE `t` CHECK PARTITION ALL",
        ),
        (
            "alter table t check partition p",
            "ALTER TABLE `t` CHECK PARTITION `p`",
        ),
        (
            "alter table t check partition p1, p2",
            "ALTER TABLE `t` CHECK PARTITION `p1`,`p2`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// Exact Go `TestDDL` IMPORT PARTITION rows at `pkg/parser/parser_test.go:6677-6681`.
#[test]
fn alter_import_partition_tablespace_restores_go_rows() {
    for (sql, expected) in [
        (
            "alter table t import partition p0 tablespace",
            "ALTER TABLE `t` IMPORT PARTITION `p0` TABLESPACE",
        ),
        (
            "alter table t import partition p0, p1 tablespace",
            "ALTER TABLE `t` IMPORT PARTITION `p0`,`p1` TABLESPACE",
        ),
        (
            "alter table t import partition all tablespace",
            "ALTER TABLE `t` IMPORT PARTITION ALL TABLESPACE",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    for sql in [
        "alter table t import partition all, p0 tablespace",
        "alter table t import partition p0, all tablespace",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
