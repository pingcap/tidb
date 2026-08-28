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

#![allow(missing_docs)]

//! GO PORT of `pkg/ddl/bdr/bdr_test.go` (items 33-35 of the pkg/ddl.part1
//! slice, read from `origin/master`).
//!
//! The Go tests pin the three pure BDR denial predicates of
//! `pkg/ddl/bdr/bdr.go`: `IsAddColumnDenied` (bdr.go:27), which for
//! `BDRRolePrimary` rejects an add-column whose option list is anything
//! beyond nullable / not-null-with-default (comment and generated options
//! are discounted from the option count); `IsModifyColumnDenied`
//! (bdr.go:68), which rejects any type change and any option list beyond
//! default-only or default+comment; and `IsDenied` (bdr.go:101), which walks
//! `model.ActionBDRMap` (SafeDDL / UnmanagementDDL classes) and forces a
//! denial for a unique add-index on the primary role. Non-primary roles are
//! never denied by the first two.
//!
//! None of the three functions is transcreated anywhere in this workspace:
//! `tidb-model` carries the source-shaped `ACTION_BDR_MAP` data, but the
//! denial logic over it is not ported, so there is no Rust surface to drive.
//! They stay documentary `#[ignore]`s.

/// GO PORT of `pkg/ddl/bdr/bdr_test.go:29 TestIsAddColumnDenied`.
///
/// Ten (role, options, expected) cases: no options (implicitly nullable)
/// allowed; `NULL` allowed; `DEFAULT` alone allowed; `NOT NULL`+`DEFAULT`
/// allowed; comment and/or generated options alone allowed (they are
/// discounted, leaving tpLen 0); `CHECK` denied; and every non-primary role
/// denied nothing.
#[test]
#[ignore = "go-parity-gap: pkg/ddl/bdr/bdr.go:27 IsAddColumnDenied is not transcreated in this workspace (tidb-model has only the ActionBDRMap data)"]
fn bdr_is_add_column_denied() {}

/// GO PORT of `pkg/ddl/bdr/bdr_test.go:106 TestIsModifyColumnDenied`.
///
/// Nine (role, new type, old type, options, expected) cases: a type change
/// (bigint vs varchar) denied for primary; same type with `DEFAULT` alone or
/// `DEFAULT`+comment allowed; same type with only a comment denied; and
/// every non-primary role denied nothing.
#[test]
#[ignore = "go-parity-gap: pkg/ddl/bdr/bdr.go:68 IsModifyColumnDenied is not transcreated in this workspace"]
fn bdr_is_modify_column_denied() {}

/// GO PORT of `pkg/ddl/bdr/bdr_test.go:173 TestIsDenied`.
///
/// A 3xN role/action matrix (CreateSchema, DropSchema, CreateTable,
/// DropTable, AddColumn, DropColumn, AddIndex, DropIndex, AddForeignKey,
/// DropForeignKey, TruncateTable, ModifyColumn, RebaseAutoID, RenameTable,
/// SetDefaultValue, ShardRowID, ModifyTableComment, ...): primary denies
/// everything not in ActionBDRMap's SafeDDL/UnmanagementDDL classes;
/// secondary denies everything the map knows; none denies for
/// BDRRoleNone — plus the unique add-index denial on the primary role.
#[test]
#[ignore = "go-parity-gap: pkg/ddl/bdr/bdr.go:101 IsDenied is not transcreated; the SafeDDL/UnmanagementDDL classification over tidb-model's ACTION_BDR_MAP has no Rust consumer to drive"]
fn bdr_is_denied() {}
