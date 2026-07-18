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

#![allow(dead_code)]
#![allow(missing_docs)]

#[path = "../src/auth_identity.rs"]
mod auth_identity;

use auth_identity::{
    AuthPluginHandoff, AuthPluginHandoffError, IdentityCatalog, IdentityLookupPolicy,
    IdentityLookupRequest, IdentityLookupResult, MatchedIdentity, PrivilegeRowAdmission,
    DEFAULT_AUTH_PLUGIN,
};

#[test]
fn lookup_request_preserves_match_inputs_and_resolution_setting() {
    let request = IdentityLookupRequest::new("alice", "db.example", true);
    assert_eq!(request.username(), "alice");
    assert_eq!(request.remote_host(), "db.example");
    assert!(request.skip_name_resolve());

    let resolved = IdentityLookupRequest::new("alice", "192.0.2.1", false);
    assert!(!resolved.skip_name_resolve());
}

#[test]
fn matched_identity_is_canonical_but_not_authenticated() {
    let result = IdentityLookupResult::Matched(MatchedIdentity::new("alice", "%"));
    assert!(result.is_matched());
    let identity = result.identity().expect("canonical row");
    assert_eq!(identity.username(), "alice");
    assert_eq!(identity.host(), "%");
}

#[test]
fn not_found_is_explicit_and_has_no_identity() {
    let result = IdentityLookupResult::NotFound;
    assert!(!result.is_matched());
    assert_eq!(result.identity(), None);
}

#[test]
fn catalog_uses_most_specific_host_match_first() {
    let catalog = IdentityCatalog::new([
        MatchedIdentity::new("alice", "%"),
        MatchedIdentity::new("alice", "192.168.1.%"),
        MatchedIdentity::new("alice", "192.168.1.1"),
        MatchedIdentity::new("alice", "localhost"),
    ]);

    let exact = IdentityLookupRequest::new("alice", "192.168.1.1", true);
    assert_eq!(
        catalog
            .resolve(&exact, &[])
            .identity()
            .map(MatchedIdentity::host),
        Some("192.168.1.1")
    );

    let wildcard = IdentityLookupRequest::new("alice", "192.168.1.2", true);
    assert_eq!(
        catalog
            .resolve(&wildcard, &[])
            .identity()
            .map(MatchedIdentity::host),
        Some("192.168.1.%")
    );

    let loopback = IdentityLookupRequest::new("alice", "127.0.0.1", true);
    assert_eq!(
        catalog
            .resolve(&loopback, &[])
            .identity()
            .map(MatchedIdentity::host),
        Some("localhost")
    );
}

#[test]
fn catalog_supports_ipv4_network_and_injected_reverse_lookup() {
    let catalog = IdentityCatalog::new([
        MatchedIdentity::new("alice", "%"),
        MatchedIdentity::new("alice", "10.0.0.0/255.255.255.0"),
    ]);

    let network = IdentityLookupRequest::new("alice", "10.0.0.7", true);
    assert_eq!(
        catalog
            .resolve(&network, &[])
            .identity()
            .map(MatchedIdentity::host),
        Some("10.0.0.0/255.255.255.0")
    );

    let reverse_catalog = IdentityCatalog::new([MatchedIdentity::new("alice", "db.example")]);
    let reverse = IdentityLookupRequest::new("alice", "203.0.113.7", false);
    assert_eq!(
        reverse_catalog
            .resolve(&reverse, &["db.example"])
            .identity()
            .map(MatchedIdentity::host),
        Some("db.example")
    );
    assert!(!reverse_catalog.resolve(&reverse, &[]).is_matched());
    assert_eq!(
        reverse_catalog
            .resolve(
                &IdentityLookupRequest::new("alice", "203.0.113.7", true),
                &["db.example"]
            )
            .identity()
            .map(MatchedIdentity::host),
        None
    );
}

#[test]
fn catalog_matches_binary_wildcards_and_escaped_markers() {
    let one_byte = IdentityCatalog::new([MatchedIdentity::new("alice", "db_")]);
    assert!(one_byte
        .resolve(&IdentityLookupRequest::new("alice", "db1", true), &[])
        .is_matched());
    assert!(!one_byte
        .resolve(&IdentityLookupRequest::new("alice", "db12", true), &[])
        .is_matched());

    let escaped = IdentityCatalog::new([MatchedIdentity::new("alice", r"db\_%")]);
    assert!(escaped
        .resolve(&IdentityLookupRequest::new("alice", "db_name", true), &[])
        .is_matched());
    assert!(!escaped
        .resolve(&IdentityLookupRequest::new("alice", "dbXname", true), &[])
        .is_matched());
}

#[test]
fn skip_with_grant_bypasses_rows_but_stays_pre_auth() {
    let catalog = IdentityCatalog::new([MatchedIdentity::new("alice", "localhost")]);
    let request = IdentityLookupRequest::new("missing", "remote.example", true);

    let bypassed = catalog.resolve_with_policy(IdentityLookupPolicy::new(true), &request, &[]);
    assert_eq!(
        bypassed,
        IdentityLookupResult::Bypassed(MatchedIdentity::new("missing", "remote.example"))
    );
    assert!(!bypassed.is_matched());
    assert!(bypassed.is_admitted());
    assert_eq!(
        bypassed.identity().map(MatchedIdentity::username),
        Some("missing")
    );

    let denied = catalog.resolve_with_policy(IdentityLookupPolicy::new(false), &request, &[]);
    assert_eq!(denied, IdentityLookupResult::NotFound);
    assert!(!denied.is_admitted());
}

#[test]
fn exact_privilege_row_check_does_not_rematch_wildcards() {
    let catalog = IdentityCatalog::new([
        MatchedIdentity::new("alice", "%"),
        MatchedIdentity::new("alice", "localhost"),
    ]);

    let wildcard = MatchedIdentity::new("alice", "%");
    assert_eq!(
        catalog.exact_row(&wildcard),
        PrivilegeRowAdmission::Exact(wildcard.clone())
    );
    assert!(catalog.exact_row(&wildcard).is_admitted());

    // ConnectionVerification requires the canonical user/host strings. A
    // concrete host must be resolved first; this method must not treat `%` as
    // a second wildcard lookup.
    let unresolved = MatchedIdentity::new("alice", "192.168.1.7");
    assert_eq!(
        catalog.exact_row(&unresolved),
        PrivilegeRowAdmission::NotFound
    );

    let missing = MatchedIdentity::new("missing", "%");
    assert_eq!(catalog.exact_row(&missing).identity(), None);
}

#[test]
fn auth_plugin_handoff_preserves_row_metadata_without_verifying_passwords() {
    let identity = MatchedIdentity::new("alice", "%");
    let admission = PrivilegeRowAdmission::Exact(identity.clone());
    let handoff = AuthPluginHandoff::from_row(&admission, "custom_auth", true)
        .expect("exact row can hand off plugin metadata");
    assert_eq!(handoff.identity(), &identity);
    assert_eq!(handoff.auth_plugin(), "custom_auth");
    assert!(handoff.has_stored_authentication());

    let missing = AuthPluginHandoff::from_row(
        &PrivilegeRowAdmission::NotFound,
        "mysql_native_password",
        false,
    );
    assert_eq!(missing, Err(AuthPluginHandoffError::MissingPrivilegeRow));

    let bypass = AuthPluginHandoff::for_bypass(&MatchedIdentity::new("missing", "remote"));
    assert_eq!(bypass.auth_plugin(), DEFAULT_AUTH_PLUGIN);
    assert!(!bypass.has_stored_authentication());
}
