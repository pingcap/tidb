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

//! Direct port of `pkg/meta/metadef` unit tests from Go master
//! (`db_test.go` and `system_test.go`).

use super::*;
use crate::{
    is_mem_db, is_reserved_id, is_system_db, is_system_related_db, RESERVED_GLOBAL_ID_LOWER_BOUND,
    system::RESERVED_GLOBAL_ID_UPPER_BOUND,
};

// Go pkg/meta/metadef/db_test.go TestIsMemDB.
#[test]
fn is_mem_db_go_test() {
    assert!(is_mem_db("information_schema"));
    assert!(is_mem_db("performance_schema"));
    assert!(is_mem_db("metrics_schema"));
    assert!(!is_mem_db("mysql"));
}

// Go pkg/meta/metadef/db_test.go TestIsSystemRelatedDB.
#[test]
fn is_system_related_db_go_test() {
    assert!(is_system_related_db("mysql"));
    assert!(is_system_related_db("sys"));
    assert!(is_system_related_db("workload_schema"));
    // Upper-case name does not match the lower-form input these predicates take.
    assert!(!is_system_related_db("INFORMATION_SCHEMA"));
}

// Go pkg/meta/metadef/db_test.go TestIsSystemDB.
#[test]
fn is_system_db_go_test() {
    assert!(is_system_db("mysql"));
    assert!(!is_system_db("sys"));
}

// Go pkg/meta/metadef/system_test.go TestIsReservedID.
#[test]
fn is_reserved_id_go_test() {
    assert!(is_reserved_id(RESERVED_GLOBAL_ID_UPPER_BOUND));
    assert!(is_reserved_id(RESERVED_GLOBAL_ID_LOWER_BOUND + 1));
    assert!(!is_reserved_id(RESERVED_GLOBAL_ID_LOWER_BOUND));
    assert!(!is_reserved_id(123));
}
