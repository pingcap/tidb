// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Execution boundary for the source-owned `SHOW ENGINES` catalog leaf.

use super::*;

#[test]
fn show_engines_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_engines_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_engines_boundary values (1)");

    assert_eq!(
        step(&mut db, "show engines"),
        "Unsupported(\"SHOW ENGINES\")"
    );

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from show_engines_boundary"), "RS:");
}
