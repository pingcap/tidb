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

//! Direct Go `parseUseStmt` / `UseStmt.Restore` coverage from
//! `pkg/parser/parser_test.go:2439-2441` and the checked integration row
//! `tests/integrationtest/t/executor/explainfor.test:240`.
//!
//! `parseUseStmt` accepts `isIdentLike`, which includes non-reserved keyword
//! tokens such as `PLAN_CACHE`. It does not accept a reserved grammar word
//! such as `SELECT`; that narrower boundary is intentional and keeps this
//! fix local to the `USE` database-name slot.

use super::*;

#[test]
fn use_non_reserved_keyword_source_row_restores_like_go() {
    assert_eq!(r("use plan_cache"), "USE `plan_cache`");
    assert_eq!(r("use `select`"), "USE `select`");
    assert_eq!(r("use app"), "USE `app`");
}

#[test]
fn use_reserved_keyword_remains_rejected() {
    assert!(parse("use select").is_err());
}
