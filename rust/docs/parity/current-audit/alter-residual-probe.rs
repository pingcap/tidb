// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Diagnostic for shared allocator effects outside catalog-image rollback.
//! This records observed behavior, not whole-package acceptance.

fn main() {
    let mut session = tidb_session::Session::new();
    for sql in [
        "CREATE TABLE alter_ids (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)",
        "INSERT INTO alter_ids(v) VALUES (1)",
        "ALTER TABLE alter_ids AUTO_INCREMENT=1000000, ADD COLUMN id INT",
        "INSERT INTO alter_ids(v) VALUES (2)",
        "SELECT id,v FROM alter_ids ORDER BY v",
    ] {
        println!("{sql}\n  {:?}", session.run(sql));
    }
}
