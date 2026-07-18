// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Execution boundaries for the source-owned ordinary SHOW family leaves.

use super::*;

#[test]
fn show_builtins_and_full_tables_are_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_builtins_full_boundary (id int)");
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into show_builtins_full_boundary values (1)",
    );

    assert_eq!(
        step(&mut db, "show builtins"),
        "Unsupported(\"SHOW BUILTINS\")"
    );
    assert_eq!(
        step(&mut db, "show full tables like '%lmn'"),
        "Unsupported(\"SHOW TABLES\")"
    );

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_builtins_full_boundary"),
        "RS:"
    );
}
