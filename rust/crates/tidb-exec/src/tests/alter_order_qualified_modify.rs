// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Execution boundaries for source-owned ALTER TABLE ordering leaves.

use super::*;

#[test]
fn alter_order_and_qualified_modify_reject_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table alter_order_boundary (id int, c int, c1 int)",
    );
    step(&mut db, "begin");
    step(&mut db, "insert into alter_order_boundary values (1, 2, 3)");

    assert_eq!(
        step(&mut db, "alter table alter_order_boundary order by c"),
        "Unsupported(\"ALTER TABLE ORDER BY\")"
    );
    assert_eq!(
        step(
            &mut db,
            "alter table alter_order_boundary modify other.alter_order_boundary.c1 bigint",
        ),
        "Unsupported(\"ALTER TABLE qualified MODIFY COLUMN\")"
    );

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id, c, c1 from alter_order_boundary"),
        "RS:"
    );
}
