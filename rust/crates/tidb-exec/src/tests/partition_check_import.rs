// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Execution boundaries for CHECK/IMPORT PARTITION actions.

use super::*;

#[test]
fn partition_check_and_import_reject_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table partition_action_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into partition_action_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "alter table partition_action_boundary check partition p1"
        ),
        "Unsupported(\"ALTER TABLE CHECK PARTITION\")"
    );
    assert_eq!(
        step(
            &mut db,
            "alter table partition_action_boundary import partition p1 tablespace",
        ),
        "Unsupported(\"ALTER TABLE IMPORT PARTITION TABLESPACE\")"
    );

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from partition_action_boundary"),
        "RS:"
    );
}
