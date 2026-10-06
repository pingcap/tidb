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

//! Port of `pkg/domain/domain_utils_test.go` (origin/master):
//! `TestErrorCode` (:25) and `TestServerIDConstant` (:30).
//!
//! `TestErrorCode` pins that the domain-class terrors
//! `ErrInfoSchemaExpired` / `ErrInfoSchemaChanged` carry the catalog's MySQL
//! error codes through the terror→SQLError conversion. The transcreation
//! carries them as `tidb_domain::schema_checker::SchemaCheckError`
//! (`schema_checker.rs:164-204`, the named boundary for
//! `domain.go:3012-3016`), whose `code()` is documented as what
//! `dbterror.ClassDomain.NewStd*` assigns.
//!
//! The empty TestServerIDConstant placeholder is retired; its original
//! inequality remains an obligation in placeholder-macro-cleanup-validation.json.

#![cfg(test)]

use tidb_domain::schema_checker::SchemaCheckError;

/// Go `pkg/domain/domain_utils_test.go:25::TestErrorCode`.
///
/// Go: `require.Equal(t, errno.ErrInfoSchemaExpired,
/// int(terror.ToSQLError(ErrInfoSchemaExpired).Code))` and the same for
/// `ErrInfoSchemaChanged` — the error instances must carry the catalog
/// constants (8027 / 8028).
#[test]
fn error_code() {
    assert_eq!(SchemaCheckError::InfoSchemaExpired.code(), 8027);
    assert_eq!(SchemaCheckError::InfoSchemaChanged.code(), 8028);
}
