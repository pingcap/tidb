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

//! Complete lockdown inventory for `pkg/server/internal/parse/parse.go`.
//!
//! Go is authoritative. Every production function and syntactic control-flow
//! outcome has exactly one verdict below. The attributed upstream tests and
//! support helpers likewise name checked-in Rust receipts. Hash, declaration,
//! inventory-cardinality, receipt, and compile-symbol gates make drift or a
//! disappeared PORTED symbol fail the crate tests.

use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Ported,
    Declined,
    Unreachable,
}

type Row = (&'static str, Verdict, &'static str);

const GO_SOURCE: &str = include_str!("../../../../pkg/server/internal/parse/parse.go");
const GO_PARSE_TEST: &str = include_str!("../../../../pkg/server/internal/parse/parse_test.go");
const GO_HANDSHAKE_TEST: &str =
    include_str!("../../../../pkg/server/internal/parse/handshake_test.go");
const GO_CONN_TEST: &str = include_str!("../../../../pkg/server/conn_test.go");
const GO_PARSE_BUILD: &str = include_str!("../../../../pkg/server/internal/parse/BUILD.bazel");
const GO_SERVER_BUILD: &str = include_str!("../../../../pkg/server/BUILD.bazel");
const GO_SOURCE_SHA256: &str = "f99c3f11f808ab3d477f5edcdaf7174b5f408972ffe29cb58fdf3b4a341394fe";
const GO_PARSE_TEST_SHA256: &str =
    "db8cca5f9854acf5cd5b44ba9e127b98221c6c03c8dc2ee13190e89860715cfd";
const GO_HANDSHAKE_TEST_SHA256: &str =
    "c4c2265dec1ccfdafd34f3cf4479fe98c6244d768ba6be743ddd70abeafead4a";
const GO_CONN_TEST_SHA256: &str =
    "5dfd914af38d426e3aa33045ba09a6236daae6489c57f397c5fe56909e11962b";
const GO_PARSE_BUILD_SHA256: &str =
    "6f3235ead971c5421f853fa405bd9565ce9a3e00cfe077b0c3a3ecf4c6d8c2d5";
const GO_SERVER_BUILD_SHA256: &str =
    "e0fd6f1afa925aed5276bec68d2e14b7a5094ed0602fa6b140f7fe857cd796d8";
const EXPECTED_FUNCTIONS: &str = "StmtFetchCmd\nHandshakeResponseHeader\nHandshakeResponseBody\nparseAttrs\ndecodeConnAttrs\napplyConnAttrsPolicyAndMetrics\nnormalizeConnectAttrsLimit\nupdateConnectAttrsLongestSeen";

const FUNCTIONS: &[Row] = &[
    (
        "StmtFetchCmd",
        Verdict::Ported,
        "tidb_protocol::decode_prepared_statement_fetch",
    ),
    (
        "HandshakeResponseHeader",
        Verdict::Ported,
        "parse_response_header_into",
    ),
    (
        "HandshakeResponseBody",
        Verdict::Ported,
        "parse_response_body_into_with_attrs_state",
    ),
    ("parseAttrs", Verdict::Ported, "handshake::parse_attrs"),
    (
        "decodeConnAttrs",
        Verdict::Ported,
        "handshake::decode_conn_attrs",
    ),
    (
        "applyConnAttrsPolicyAndMetrics",
        Verdict::Ported,
        "handshake::apply_conn_attrs_policy_and_metrics",
    ),
    (
        "normalizeConnectAttrsLimit",
        Verdict::Ported,
        "handshake::normalize_connect_attrs_limit",
    ),
    (
        "updateConnectAttrsLongestSeen",
        Verdict::Ported,
        "handshake::update_connect_attrs_longest_seen",
    ),
];

// Keys are `<Go function>:L<source line>:<locus>:<outcome>`. Both outcomes of
// every if, loop, short-circuit, and deferred recovery locus are classified.
const BRANCHES: &[Row] = &[
    ("StmtFetchCmd:L41:if:true", Verdict::Ported, "stmt_fetch_rejects_every_non_eight_byte_packet"),
    ("StmtFetchCmd:L41:if:false", Verdict::Ported, "stmt_fetch_caps_the_requested_row_count_at_the_go_boundary"),
    ("HandshakeResponseHeader:L54:if:true", Verdict::Ported, "header_and_body_mutation_order_matches_go_on_failure"),
    ("HandshakeResponseHeader:L54:if:false", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L77:defer:normal", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L77:defer:unwind", Verdict::Ported, "header_and_body_mutation_order_matches_go_on_failure"),
    ("HandshakeResponseBody:L79:if:true", Verdict::Ported, "header_and_body_mutation_order_matches_go_on_failure"),
    ("HandshakeResponseBody:L79:if:false", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L88:if:true", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L88:if:false", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L92:if:true", Verdict::Ported, "null_auth_and_single_byte_no_auth_marker_preserve_go_semantics"),
    ("HandshakeResponseBody:L92:if:false", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L96:if:true", Verdict::Ported, "header_and_body_mutation_order_matches_go_on_failure"),
    ("HandshakeResponseBody:L96:if:false", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L100:if:true", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L100:if:false", Verdict::Ported, "null_auth_and_single_byte_no_auth_marker_preserve_go_semantics"),
    ("HandshakeResponseBody:L105:if:true", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L105:if:false", Verdict::Ported, "every_auth_encoding_width_and_mode_matches_go"),
    ("HandshakeResponseBody:L116:if:true", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L116:if:false", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L117:if:true", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L117:if:false", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L124:if:true", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L124:if:false", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L128:if:true", Verdict::Ported, "unterminated_final_auth_plugin_is_ignored"),
    ("HandshakeResponseBody:L128:if:false", Verdict::Ported, "unterminated_final_auth_plugin_is_ignored"),
    ("HandshakeResponseBody:L134:if:true", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("HandshakeResponseBody:L134:if:false", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L135:if:true", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L135:if:false", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("HandshakeResponseBody:L140:if:true", Verdict::Ported, "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors"),
    ("HandshakeResponseBody:L140:if:false", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("HandshakeResponseBody:L144:if:true", Verdict::Ported, "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors"),
    ("HandshakeResponseBody:L144:if:false", Verdict::Ported, "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors"),
    ("HandshakeResponseBody:L145:if:true", Verdict::Ported, "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors"),
    ("HandshakeResponseBody:L145:if:false", Verdict::Ported, "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors"),
    ("HandshakeResponseBody:L149:if:true", Verdict::Ported, "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors"),
    ("HandshakeResponseBody:L149:if:false", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("HandshakeResponseBody:L158:if:true", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("HandshakeResponseBody:L158:if:false", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L162:if:true", Verdict::Ported, "raw_attribute_bytes_null_lengths_and_warning_order_are_preserved"),
    ("HandshakeResponseBody:L162:if:false", Verdict::Ported, "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors"),
    ("HandshakeResponseBody:L170:if:true", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("HandshakeResponseBody:L170:if:false", Verdict::Ported, "optional_database_plugin_and_zstd_fields_follow_source_order"),
    ("parseAttrs:L202:if:true", Verdict::Ported, "zero_attribute_limit_skips_decoding"),
    ("parseAttrs:L202:if:false", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("parseAttrs:L207:if:true", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("parseAttrs:L207:if:false", Verdict::Ported, "raw_attribute_bytes_null_lengths_and_warning_order_are_preserved"),
    ("decodeConnAttrs:L218:for:enter", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("decodeConnAttrs:L218:for:exit", Verdict::Ported, "zero_attribute_limit_skips_decoding"),
    ("decodeConnAttrs:L220:if:true", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("decodeConnAttrs:L220:if:false", Verdict::Ported, "raw_attribute_bytes_null_lengths_and_warning_order_are_preserved"),
    ("decodeConnAttrs:L226:if:true", Verdict::Ported, "malformed_attribute_rows_are_ignored_after_the_frame_is_valid"),
    ("decodeConnAttrs:L226:if:false", Verdict::Ported, "raw_attribute_bytes_null_lengths_and_warning_order_are_preserved"),
    ("decodeConnAttrs:L237:if:true", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("decodeConnAttrs:L237:if:false", Verdict::Ported, "standard_underscore_attributes_do_not_warn"),
    ("decodeConnAttrs:L237:logical:short-circuit", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("decodeConnAttrs:L237:logical:rhs", Verdict::Ported, "standard_underscore_attributes_do_not_warn"),
    ("decodeConnAttrs:L238:if:true", Verdict::Ported, "standard_underscore_attributes_do_not_warn"),
    ("decodeConnAttrs:L238:if:false", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("applyConnAttrsPolicyAndMetrics:L255:range:enter", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("applyConnAttrsPolicyAndMetrics:L255:range:exit", Verdict::Ported, "zero_attribute_limit_skips_decoding"),
    ("applyConnAttrsPolicyAndMetrics:L258:if:true", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("applyConnAttrsPolicyAndMetrics:L258:if:false", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("applyConnAttrsPolicyAndMetrics:L259:if:true", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("applyConnAttrsPolicyAndMetrics:L259:if:false", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("applyConnAttrsPolicyAndMetrics:L265:if:true", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("applyConnAttrsPolicyAndMetrics:L265:if:false", Verdict::Unreachable, "the only assignment of truncated=true is followed by continue, and totalSize is monotonic, so no later item reaches L265 with truncated=true"),
    ("applyConnAttrsPolicyAndMetrics:L274:if:true", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("applyConnAttrsPolicyAndMetrics:L274:if:false", Verdict::Ported, "standard_underscore_attributes_do_not_warn"),
    ("applyConnAttrsPolicyAndMetrics:L278:if:true", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("applyConnAttrsPolicyAndMetrics:L278:if:false", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("normalizeConnectAttrsLimit:L291:if:true", Verdict::Ported, "negative_attribute_limit_normalizes_to_sixty_four_kib"),
    ("normalizeConnectAttrsLimit:L291:if:false", Verdict::Ported, "attribute_policy_warnings_and_metrics_match_go_boundaries"),
    ("updateConnectAttrsLongestSeen:L303:if:true", Verdict::Ported, "sixty_four_kib_metric_boundary_does_not_update_longest_seen"),
    ("updateConnectAttrsLongestSeen:L303:if:false", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("updateConnectAttrsLongestSeen:L306:for:enter", Verdict::Ported, "concurrent_longest_seen_updates_converge_on_the_maximum"),
    ("updateConnectAttrsLongestSeen:L306:for:exit", Verdict::Ported, "concurrent_longest_seen_updates_converge_on_the_maximum"),
    ("updateConnectAttrsLongestSeen:L308:if:true", Verdict::Ported, "warning_combination_duplicate_keys_and_longest_seen_cas_match_go"),
    ("updateConnectAttrsLongestSeen:L308:if:false", Verdict::Ported, "concurrent_longest_seen_updates_converge_on_the_maximum"),
    ("updateConnectAttrsLongestSeen:L311:if:true", Verdict::Ported, "concurrent_longest_seen_updates_converge_on_the_maximum"),
    ("updateConnectAttrsLongestSeen:L311:if:false", Verdict::Ported, "concurrent_longest_seen_updates_converge_on_the_maximum"),
];

const TEST_SUPPORT: &[Row] = &[
    (
        "parse_test.go:TestParseStmtFetchCmd",
        Verdict::Ported,
        "stmt_fetch_rejects_every_non_eight_byte_packet",
    ),
    (
        "parse_test.go:TestParseAttrsUnderscoreWarning",
        Verdict::Ported,
        "standard_underscore_attributes_do_not_warn",
    ),
    (
        "handshake_test.go:TestAuthSwitchRequest",
        Verdict::Ported,
        "optional_database_plugin_and_zstd_fields_follow_source_order",
    ),
    (
        "conn_test.go:TestReadHandshakeGatewayTLSAttrStarter",
        Verdict::Ported,
        "raw_attribute_bytes_null_lengths_and_warning_order_are_preserved",
    ),
    (
        "conn_test.go:TestMalformHandshakeHeader",
        Verdict::Ported,
        "header_and_body_mutation_order_matches_go_on_failure",
    ),
    (
        "conn_test.go:TestParseHandshakeResponse",
        Verdict::Ported,
        "every_auth_encoding_width_and_mode_matches_go",
    ),
    (
        "conn_test.go:encodeLengthEncodedIntForHandshake",
        Verdict::Ported,
        "fn lenenc",
    ),
    (
        "conn_test.go:buildHandshakeResponsePacket",
        Verdict::Ported,
        "fn response_with_attrs",
    ),
    (
        "conn_test.go:TestHandshakeResponseCompatibilityAndFailurePaths",
        Verdict::Ported,
        "null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors",
    ),
    (
        "conn_test.go:TestIssue1768",
        Verdict::Ported,
        "null_auth_and_single_byte_no_auth_marker_preserve_go_semantics",
    ),
    (
        "conn_test.go:TestParseHandshakeAttrsTruncation",
        Verdict::Ported,
        "attribute_policy_warnings_and_metrics_match_go_boundaries",
    ),
    (
        "internal/parse/BUILD.bazel:go_library+go_test",
        Verdict::Ported,
        "[[test]]",
    ),
    (
        "server/BUILD.bazel:conn_test.go membership",
        Verdict::Ported,
        "name = \"all\"",
    ),
];

const RUST_RECEIPTS: &str = concat!(
    include_str!("../tests/parse_go_source.rs"),
    include_str!("../../tidb-protocol/tests/prepared_statement_protocol_source.rs"),
    include_str!("../Cargo.toml"),
);

fn hash(source: &str) -> String {
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

fn top_level_function_names(source: &str) -> Vec<&str> {
    source
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("func ")?;
            if rest.starts_with('(') {
                return None;
            }
            rest.split_once('(').map(|(name, _)| name)
        })
        .collect()
}

#[test]
fn parse_go_owner_and_attributed_tests_do_not_drift() {
    assert_eq!(hash(GO_SOURCE), GO_SOURCE_SHA256, "parse.go changed");
    assert_eq!(GO_SOURCE.lines().count(), 315);
    assert_eq!(GO_SOURCE.len(), 9_322);
    assert_eq!(
        hash(GO_PARSE_TEST),
        GO_PARSE_TEST_SHA256,
        "parse_test.go changed"
    );
    assert_eq!(
        hash(GO_HANDSHAKE_TEST),
        GO_HANDSHAKE_TEST_SHA256,
        "handshake_test.go changed"
    );
    assert_eq!(
        hash(GO_CONN_TEST),
        GO_CONN_TEST_SHA256,
        "conn_test.go changed"
    );
    assert_eq!(
        hash(GO_PARSE_BUILD),
        GO_PARSE_BUILD_SHA256,
        "internal/parse/BUILD.bazel changed"
    );
    assert_eq!(
        hash(GO_SERVER_BUILD),
        GO_SERVER_BUILD_SHA256,
        "server/BUILD.bazel changed"
    );
    assert_eq!(
        top_level_function_names(GO_SOURCE),
        EXPECTED_FUNCTIONS.lines().collect::<Vec<_>>()
    );
}

#[test]
fn every_function_branch_and_test_support_artifact_has_exactly_one_verdict() {
    assert_eq!(FUNCTIONS.len(), 8);
    assert_eq!(BRANCHES.len(), 82);
    assert_eq!(TEST_SUPPORT.len(), 13);
    for rows in [FUNCTIONS, BRANCHES, TEST_SUPPORT] {
        let mut keys = rows.iter().map(|row| row.0).collect::<Vec<_>>();
        keys.sort_unstable();
        assert!(keys.windows(2).all(|pair| pair[0] != pair[1]));
        for (key, verdict, evidence) in rows {
            assert!(!key.is_empty());
            assert!(!evidence.is_empty());
            assert!(matches!(
                verdict,
                Verdict::Ported | Verdict::Declined | Verdict::Unreachable
            ));
        }
    }
    for (key, verdict, marker) in TEST_SUPPORT {
        if *verdict == Verdict::Ported {
            assert!(
                RUST_RECEIPTS.contains(marker),
                "{key} lost Rust receipt {marker}"
            );
        }
    }
}

#[test]
fn every_ported_parse_owner_symbol_still_compiles() {
    let _ = tidb_protocol::decode_prepared_statement_fetch;
    let _ = tidb_protocol::MAX_STMT_FETCH_SIZE;
    let _ = crate::parse_response_header_into;
    let _ = crate::parse_response_body_into_with_attrs_state;
    let _ = crate::handshake::parse_attrs;
    let _ = crate::handshake::decode_conn_attrs;
    let _ = crate::handshake::apply_conn_attrs_policy_and_metrics;
    let _ = crate::handshake::normalize_connect_attrs_limit;
    let _ = crate::handshake::update_connect_attrs_longest_seen;
}

#[test]
fn inventory_contains_no_unclassified_or_placeholder_verdicts() {
    let inventory = include_str!("parse_go_inventory.rs");
    let forbidden = ["TO".to_owned() + "DO", "UNCLASS".to_owned() + "IFIED"];
    for marker in forbidden {
        assert!(!inventory.contains(&marker));
    }
    assert_eq!(
        FUNCTIONS
            .iter()
            .filter(|row| row.1 == Verdict::Declined)
            .count(),
        0
    );
    assert_eq!(
        BRANCHES
            .iter()
            .filter(|row| row.1 == Verdict::Unreachable)
            .count(),
        1
    );
}
