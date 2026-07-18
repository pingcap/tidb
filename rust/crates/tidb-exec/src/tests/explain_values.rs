// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Executor boundary for the source-owned EXPLAIN parenthesized VALUES leaf.

use super::*;

#[test]
fn explain_parenthesized_values_is_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table explain_values_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into explain_values_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "explain format = traditional ((values row ()) order by 1)",
        ),
        "Unsupported(\"EXPLAIN\")"
    );

    step(&mut db, "rollback");
    assert_eq!(
        step(
            &mut db,
            "select id from explain_values_boundary order by id",
        ),
        "RS:"
    );
}
