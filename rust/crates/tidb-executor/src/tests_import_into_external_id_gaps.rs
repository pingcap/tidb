// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/import_into_test.go`: `IMPORT INTO`
//! statement-level behavior around Security-Enhanced Mode and S3 external IDs.
//! The statement parses on this tier (`tidb-ast`'s `ImportIntoStmt` mirrors Go
//! `ast.ImportIntoStmt`, with the same `WITH` option surface in
//! `tidb-ast/src/stmt/load_data.rs`), but everything the two tests pin happens
//! inside `pkg/executor/importer` (`Plan`/`LoadDataController`) plus SEM V2
//! enforcement, neither of which exists here.

/// Go `pkg/executor/import_into_test.go:91::TestClassicS3ExternalID`
/// (classic-kernel only): an explicit `EXTERNAL-ID` query parameter on an
/// `s3://`/`oss://` data source survives into the import plan unchanged,
/// whether SEM is enabled or not -- SEM V2's forbidden-feature list
/// `import_with_external_id` (`import_into_test.go:69`) blocks the *defaulted*
/// keyspace external ID, never an explicit one. The hook is the
/// `NewImportPlan` failpoint (`pkg/executor/importer/import.go:541
/// NewImportPlan`); the ID key is `s3like.S3ExternalID` ("external-id",
/// `pkg/objstore/s3like/store.go:55`).
#[test]
#[ignore = "go-parity-gap: pkg/executor/importer Plan/LoadDataController and the NewImportPlan failpoint hook are unported; SEM V2 feature gates have no Rust surface"]
fn import_into_keeps_an_explicit_s3_external_id_with_and_without_sem() {}

/// Go `pkg/executor/import_into_test.go:312::TestImportIntoValidateColAssignmentsWithEncodeCtx`:
/// column-assignment expressions must be evaluable with ONLY the optional eval
/// properties the import encode session provides, checked recursively
/// (`pkg/executor/import_into.go:159
/// ValidateImportIntoColAssignmentsWithEncodeCtx`, recursion at :183
/// `checkExprWithProvidedProps`). Constants, `@`-vars, `concat`, and
/// `getvar('var1')` pass; `setvar`, `current_user`, `current_role`,
/// `connection_id`, `tidb_is_ddl_owner`, `sleep`, `last_insert_id` are
/// rejected with "FUNCTION <name> is not supported in IMPORT INTO column
/// assignment, index <i>". The Rust `tidb-expr` crate has the property model
/// (`OptionalEvalPropKey` in `exprctx.rs`, `RequireOptionalEvalProps` in
/// `expropt/mod.rs`) but no encode-session entry point, so the pinning edge is
/// missing.
#[test]
#[ignore = "go-parity-gap: ValidateImportIntoColAssignmentsWithEncodeCtx (import_into.go:159) needs the litkv encode session to build expressions against; unported on this tier"]
fn import_into_column_assignments_reject_functions_needing_unprovided_eval_props() {}
