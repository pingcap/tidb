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

//! Push-down request support translated from `pkg/kv/checker.go`.
//!
//! The public boundary deliberately accepts raw `i64` values, like Go's
//! `kv.Client.IsRequestTypeSupported`. Expression subtypes then use Go's
//! observable `int64` to TIPB `int32` narrowing before matching. A closed Rust
//! enum at this boundary would be unable to represent unknown wire values and
//! would hide that conversion behavior.

/// Coprocessor select request identity from `pkg/kv/kv.go`.
pub const REQ_TYPE_SELECT: i64 = 101;
/// Coprocessor index request identity from `pkg/kv/kv.go`.
pub const REQ_TYPE_INDEX: i64 = 102;
/// DAG request identity from `pkg/kv/kv.go`.
pub const REQ_TYPE_DAG: i64 = 103;
/// Analyze request identity from `pkg/kv/kv.go`.
pub const REQ_TYPE_ANALYZE: i64 = 104;
/// Checksum request identity from `pkg/kv/kv.go`.
pub const REQ_TYPE_CHECKSUM: i64 = 105;

/// Basic request subtype identity from `pkg/kv/kv.go`.
pub const REQ_SUB_TYPE_BASIC: i64 = 0;
/// Descending request subtype identity from `pkg/kv/kv.go`.
pub const REQ_SUB_TYPE_DESC: i64 = 10_000;
/// Group-by request subtype identity from `pkg/kv/kv.go`.
pub const REQ_SUB_TYPE_GROUP_BY: i64 = 10_001;
/// Top-N request subtype identity from `pkg/kv/kv.go`.
pub const REQ_SUB_TYPE_TOP_N: i64 = 10_002;
/// Signature request subtype identity from `pkg/kv/kv.go`.
pub const REQ_SUB_TYPE_SIGNATURE: i64 = 10_003;
/// Analyze-index request subtype identity from `pkg/kv/kv.go`.
pub const REQ_SUB_TYPE_ANALYZE_IDX: i64 = 10_004;
/// Analyze-column request subtype identity from `pkg/kv/kv.go`.
pub const REQ_SUB_TYPE_ANALYZE_COL: i64 = 10_005;

/// Checks whether a raw KV request type/subtype pair supports push-down.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct RequestTypeSupportedChecker;

impl RequestTypeSupportedChecker {
    /// Returns the source-defined support decision for a raw protocol pair.
    ///
    /// `REQ_TYPE_ANALYZE` intentionally accepts every subtype. The other
    /// expression-bearing requests narrow `sub_type` to TIPB's generated
    /// `int32` representation exactly as `tipb.ExprType(subType)` does in Go.
    #[must_use]
    pub const fn is_request_type_supported(self, req_type: i64, sub_type: i64) -> bool {
        match req_type {
            REQ_TYPE_SELECT | REQ_TYPE_INDEX => match sub_type {
                REQ_SUB_TYPE_GROUP_BY | REQ_SUB_TYPE_BASIC | REQ_SUB_TYPE_TOP_N => true,
                _ => Self::support_expr(sub_type as i32),
            },
            REQ_TYPE_DAG => Self::support_expr(sub_type as i32),
            REQ_TYPE_ANALYZE => true,
            _ => false,
        }
    }

    // These numeric identities come from the TIPB revision pinned by go.mod:
    // github.com/pingcap/tipb v0.0.0-20260623093813-5f9928e91afe,
    // go-tipb/expression.pb.go. Keeping the mapping local avoids inventing a
    // protocol crate before a real protobuf consumer exists.
    const fn support_expr(expr_type: i32) -> bool {
        matches!(
            expr_type,
            // Encoded scalar values.
            0 | 1 | 2 | 3 | 4 | 5 | 6 |
            // MySQL-specific values supported by the Go source.
            101 | 102 | 103 | 104 | 107 | 121 |
            // ColumnRef.
            201 |
            // Aggregate functions.
            3001 | 3002 | 3003 | 3004 | 3005 | 3006 | 3007 | 3008 | 3009 | 3010 | 3020 | 3021 |
            // Window functions.
            4001 | 4002 | 4003 | 4004 | 4005 | 4006 | 4007 | 4008 | 4009 | 4010 | 4011 |
            // ReqSubTypeDesc and ReqSubTypeSignature. Desc also shares the
            // numeric identity of TIPB ScalarFunc.
            10_000 | 10_003
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RequestTypeSupportedChecker, REQ_SUB_TYPE_ANALYZE_COL, REQ_SUB_TYPE_ANALYZE_IDX,
        REQ_SUB_TYPE_BASIC, REQ_SUB_TYPE_DESC, REQ_SUB_TYPE_GROUP_BY, REQ_SUB_TYPE_SIGNATURE,
        REQ_SUB_TYPE_TOP_N, REQ_TYPE_ANALYZE, REQ_TYPE_CHECKSUM, REQ_TYPE_DAG, REQ_TYPE_INDEX,
        REQ_TYPE_SELECT,
    };

    const SUPPORTED_TIPB_EXPR_TYPES: [i64; 38] = [
        0, 1, 2, 3, 4, 5, 6, 101, 102, 103, 104, 107, 121, 201, 3001, 3002, 3003, 3004, 3005, 3006,
        3007, 3008, 3009, 3010, 3020, 3021, 4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008, 4009,
        4010, 4011, 10_000,
    ];
    const UNSUPPORTED_TIPB_EXPR_TYPES: [i64; 15] = [
        105, 106, 108, 151, 3011, 3012, 3013, 3014, 3015, 3016, 3017, 3018, 3019, 3022, 3023,
    ];

    #[test]
    fn every_pinned_tipb_expr_type_has_the_source_disposition() {
        let checker = RequestTypeSupportedChecker;

        // The pinned generated enum has 53 identities: every one is assigned
        // to exactly one source-derived disposition below.
        assert_eq!(
            SUPPORTED_TIPB_EXPR_TYPES.len() + UNSUPPORTED_TIPB_EXPR_TYPES.len(),
            53
        );
        for supported in SUPPORTED_TIPB_EXPR_TYPES {
            assert!(
                checker.is_request_type_supported(REQ_TYPE_DAG, supported),
                "TIPB ExprType {supported} must be supported"
            );
        }
        for unsupported in UNSUPPORTED_TIPB_EXPR_TYPES {
            assert!(
                !checker.is_request_type_supported(REQ_TYPE_DAG, unsupported),
                "TIPB ExprType {unsupported} must remain unsupported"
            );
        }

        // Signature is a supported request subtype, but is not an identity in
        // the generated TIPB ExprType enum.
        assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_SIGNATURE));
    }

    #[test]
    fn every_request_and_special_subtype_arm_matches_the_source() {
        let checker = RequestTypeSupportedChecker;

        for request_type in [REQ_TYPE_SELECT, REQ_TYPE_INDEX] {
            for subtype in [
                REQ_SUB_TYPE_BASIC,
                REQ_SUB_TYPE_GROUP_BY,
                REQ_SUB_TYPE_TOP_N,
                REQ_SUB_TYPE_DESC,
                REQ_SUB_TYPE_SIGNATURE,
            ] {
                assert!(
                    checker.is_request_type_supported(request_type, subtype),
                    "request={request_type}, subtype={subtype}"
                );
            }
            for subtype in [REQ_SUB_TYPE_ANALYZE_IDX, REQ_SUB_TYPE_ANALYZE_COL] {
                assert!(!checker.is_request_type_supported(request_type, subtype));
            }
            assert!(!checker.is_request_type_supported(request_type, 9999));
        }

        assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_BASIC));
        assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_DESC));
        assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_SIGNATURE));
        for subtype in [
            REQ_SUB_TYPE_GROUP_BY,
            REQ_SUB_TYPE_TOP_N,
            REQ_SUB_TYPE_ANALYZE_IDX,
            REQ_SUB_TYPE_ANALYZE_COL,
        ] {
            assert!(!checker.is_request_type_supported(REQ_TYPE_DAG, subtype));
        }

        // checker.go deliberately ignores the subtype for Analyze requests.
        for subtype in [
            0,
            REQ_SUB_TYPE_ANALYZE_IDX,
            REQ_SUB_TYPE_ANALYZE_COL,
            -1,
            i64::MAX,
        ] {
            assert!(checker.is_request_type_supported(REQ_TYPE_ANALYZE, subtype));
        }
        for request_type in [REQ_TYPE_CHECKSUM, 0, 106, i64::MIN, i64::MAX] {
            for subtype in [0, REQ_SUB_TYPE_BASIC, 3001, 9999, i64::MIN, i64::MAX] {
                assert!(!checker.is_request_type_supported(request_type, subtype));
            }
        }
    }

    #[test]
    fn raw_i64_subtypes_use_go_int32_narrowing() {
        let checker = RequestTypeSupportedChecker;
        const INT32_MODULUS: i64 = 1_i64 << 32;

        for supported in [1, 3001, 4011, REQ_SUB_TYPE_SIGNATURE] {
            assert!(checker.is_request_type_supported(REQ_TYPE_DAG, supported));
            assert!(checker.is_request_type_supported(REQ_TYPE_DAG, supported + INT32_MODULUS));
            assert!(checker.is_request_type_supported(REQ_TYPE_DAG, supported - INT32_MODULUS));
        }
        for unsupported in [
            105,
            3011,
            REQ_SUB_TYPE_ANALYZE_IDX,
            REQ_SUB_TYPE_ANALYZE_COL,
            9999,
        ] {
            assert!(!checker.is_request_type_supported(REQ_TYPE_DAG, unsupported));
            assert!(!checker.is_request_type_supported(REQ_TYPE_DAG, unsupported + INT32_MODULUS));
            assert!(!checker.is_request_type_supported(REQ_TYPE_DAG, unsupported - INT32_MODULUS));
        }

        // Select/Index-only subtypes compare raw int64 first. A wrapped Basic
        // alias remains accepted because it narrows to ExprType Null; wrapped
        // GroupBy and TopN aliases fall through to unsupported ExprTypes.
        for request_type in [REQ_TYPE_SELECT, REQ_TYPE_INDEX] {
            assert!(
                checker.is_request_type_supported(request_type, REQ_SUB_TYPE_BASIC + INT32_MODULUS)
            );
            for subtype in [REQ_SUB_TYPE_GROUP_BY, REQ_SUB_TYPE_TOP_N] {
                assert!(!checker.is_request_type_supported(request_type, subtype + INT32_MODULUS));
            }
        }

        // i64::MIN narrows to the supported Null identity; i64::MAX narrows to
        // -1 and is rejected. These pin the full raw protocol boundary.
        assert!(checker.is_request_type_supported(REQ_TYPE_DAG, i64::MIN));
        assert!(!checker.is_request_type_supported(REQ_TYPE_DAG, i64::MAX));
    }
}
