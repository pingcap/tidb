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
//! `TestServerIDConstant` pins `lostConnectionToPDTimeout < serverIDTTL` —
//! two unexported `domain.go` constants — and is an ignored gap:
//! `domain.go` is not transcreated.

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

/// Go `pkg/domain/domain_utils_test.go:30::TestServerIDConstant`:
/// `require.Less(t, lostConnectionToPDTimeout, serverIDTTL)`.
// go-parity-gap: both constants live in pkg/domain/domain.go (unported);
// there is no Rust symbol to pin the inequality against yet.
#[test]
#[ignore = "go-parity-gap: domain.go's lostConnectionToPDTimeout/serverIDTTL \
           constants are not transcreated"]
fn server_id_constant() {
    // Go pins `lostConnectionToPDTimeout < serverIDTTL`
    // (domain.go:2612-2621: serverIDTTL = 12h, lostConnectionToPDTimeout =
    // 6h — "Must be SHORTER than serverIDTTL"), so a PD-connection loss is
    // noticed before the server-ID TTL lapses.
}
