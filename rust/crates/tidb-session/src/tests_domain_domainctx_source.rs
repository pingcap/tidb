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

//! Port of `pkg/domain/domainctx_test.go` (origin/master): `TestDomainCtx`
//! (:25) and `TestGetRUVersionWithoutController` (:36).
//!
//! Both bind the `Domain` root: `GetDomain` (domainctx.go:23) is a typed
//! downcast over Go's context-value idiom — the crate doc of
//! `tidb_domain` records it as screened and deliberately declined until
//! `domain.go` lands — and `GetRUVersion` (domain.go:1779) is a `Domain`
//! method.

#![cfg(test)]

/// Go `pkg/domain/domainctx_test.go:25::TestDomainCtx`: after
/// `BindDomainAndSchValidator(nil, nil)` on a mock context, `GetDomain`
/// returns nil; after binding `&Domain{}`, it returns non-nil.
// go-parity-gap: domainctx.go's GetDomain downcast needs the Domain type;
// screened and declined (tidb_domain crate doc).
#[test]
#[ignore = "go-parity-gap: domainctx.go GetDomain needs the unported Domain \
           type"]
fn domain_ctx() {}

/// Go `pkg/domain/domainctx_test.go:36::TestGetRUVersionWithoutController`:
/// a `NewMockDomain()` without a ResourceGroupsController answers
/// `GetRUVersion()` with `rmclient.DefaultRUVersion`.
// go-parity-gap: Domain.GetRUVersion (domain.go:1779) is not transcreated.
#[test]
#[ignore = "go-parity-gap: Domain.GetRUVersion is not transcreated"]
fn get_ru_version_without_controller() {}
