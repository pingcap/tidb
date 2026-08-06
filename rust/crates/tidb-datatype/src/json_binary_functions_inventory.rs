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

//! LOCKDOWN INVENTORY: `pkg/types/json_binary_functions.go` ->
//! `binary_json.rs` + `binary_json_ops.rs`.
//!
//! The Go file is the atomic owner even though native Rust boundaries place its
//! scalar rules and container rules in two modules. Every one of the 46 Go
//! functions has exactly one verdict below. Every source-owned test, benchmark,
//! fuzz target, and support declaration in the two adjacent Go test files is
//! classified; declarations owned by `json_binary.go` are explicitly declined
//! here so this lockdown cannot steal their completion claim.
//!
//! PORTED rows name a live Rust symbol. DECLINED rows are closed safety or
//! ownership decisions, not pending work. UNREACHABLE rows identify Go states
//! excluded by a Rust type. The full-source SHA gate catches expression and
//! branch drift, the declaration gates make added functions/tests legible, and
//! the symbol gate makes removing a PORTED landing fail at compile time.
//!
//! Direct Go probes measured the non-obvious boundaries pinned by this unit:
//! duplicate path arguments preserve duplicate results; PeekBytesAsJSON reports
//! required length from a truncated prefix; a walk callback stops globally and
//! propagates its exact error; `$.*[0]` in ArrayInsert is an unchanged document;
//! adjacent object runs after a scalar merge together; empty merge-preserve is
//! `[]`; empty merge-patch panics; and NaN compares greater in both directions.

use sha2::{Digest, Sha256};

use crate::{
    compare_binary_json, contains_binary_json, decode_escaped_unicode, merge_binary_json,
    merge_patch_binary_json, overlaps_binary_json, peek_binary_json_len, quote_json_string,
    unquote_json_string, unquote_string, BinaryJSON, BinaryJSONError, JSONModifyType,
    JSONPathExpression, JSONSearchMode,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Ported,
    Declined,
    Unreachable,
}

type Row = (&'static str, Verdict, &'static str);

const GO_SOURCE_SHA256: &str = "578522e49701af013a1f91a3947f1c4d3231f49cbbb2d60d9edd4bcd24ae082b";
const GO_SOURCE_BYTE_COUNT: usize = 42_200;
const GO_SOURCE_LINE_COUNT: usize = 1_417;
const GO_FUNCTION_COUNT: usize = 46;
const GO_TEST_SUPPORT_COUNT: usize = 29;
const GO_BRANCH_RULE_COUNT: usize = 144;

const FUNCTIONS: &[Row] = &[
    (
        "(bj BinaryJSON) Type",
        Verdict::Ported,
        "BinaryJSON::type_name",
    ),
    (
        "(bj BinaryJSON) Unquote",
        Verdict::Ported,
        "BinaryJSON::unquote",
    ),
    ("UnquoteString", Verdict::Ported, "unquote_string"),
    ("unquoteJSONString", Verdict::Ported, "unquote_json_string"),
    (
        "decodeOneEscapedUnicode",
        Verdict::Ported,
        "decode_escaped_unicode",
    ),
    ("quoteJSONString", Verdict::Ported, "quote_json_string"),
    (
        "(bj BinaryJSON) Extract",
        Verdict::Ported,
        "BinaryJSON::extract",
    ),
    (
        "(bj BinaryJSON) extractOne",
        Verdict::Ported,
        "BinaryJSON::extract",
    ),
    (
        "(bj BinaryJSON) extractTo",
        Verdict::Ported,
        "BinaryJSON::extract",
    ),
    (
        "jsonFinished",
        Verdict::Ported,
        "BinaryJSON::extract traversal stop",
    ),
    (
        "(bj BinaryJSON) objectSearchKey",
        Verdict::Ported,
        "BinaryJSON::object_get",
    ),
    (
        "buildBinaryJSONArray",
        Verdict::Ported,
        "BinaryJSON::from_node",
    ),
    (
        "buildBinaryJSONElements",
        Verdict::Ported,
        "BinaryJSON::from_node",
    ),
    (
        "buildBinaryJSONObject",
        Verdict::Ported,
        "BinaryJSON::from_node",
    ),
    (
        "(bj BinaryJSON) Modify",
        Verdict::Ported,
        "BinaryJSON::modify",
    ),
    (
        "(bj BinaryJSON) ArrayInsert",
        Verdict::Ported,
        "BinaryJSON::array_insert",
    ),
    (
        "(bj BinaryJSON) Remove",
        Verdict::Ported,
        "BinaryJSON::remove",
    ),
    (
        "(bm *binaryModifier) set",
        Verdict::Ported,
        "BinaryJSON::modify Set",
    ),
    (
        "(bm *binaryModifier) replace",
        Verdict::Ported,
        "BinaryJSON::modify Replace",
    ),
    (
        "(bm *binaryModifier) insert",
        Verdict::Ported,
        "BinaryJSON::modify Insert",
    ),
    (
        "(bm *binaryModifier) doInsert",
        Verdict::Ported,
        "modify_node",
    ),
    (
        "(bm *binaryModifier) remove",
        Verdict::Ported,
        "BinaryJSON::remove",
    ),
    (
        "(bm *binaryModifier) doRemove",
        Verdict::Ported,
        "remove_node",
    ),
    (
        "(bm *binaryModifier) rebuild",
        Verdict::Ported,
        "BinaryJSON::from_node",
    ),
    (
        "(bm *binaryModifier) rebuildTo",
        Verdict::Ported,
        "BinaryJSON::from_node",
    ),
    (
        "compareFloat64PrecisionLoss",
        Verdict::Ported,
        "compare_binary_json_number",
    ),
    (
        "compareInt64",
        Verdict::Ported,
        "compare_binary_json_number",
    ),
    (
        "compareFloat64",
        Verdict::Ported,
        "compare_binary_json_number",
    ),
    (
        "compareUint64",
        Verdict::Ported,
        "compare_binary_json_number",
    ),
    (
        "compareInt64Uint64",
        Verdict::Ported,
        "compare_binary_json_number",
    ),
    (
        "compareFloat64Int64",
        Verdict::Ported,
        "compare_binary_json_number",
    ),
    (
        "compareFloat64Uint64",
        Verdict::Ported,
        "compare_binary_json_number",
    ),
    ("CompareBinaryJSON", Verdict::Ported, "compare_binary_json"),
    (
        "MergePatchBinaryJSON",
        Verdict::Ported,
        "merge_patch_binary_json",
    ),
    ("mergePatchBinaryJSON", Verdict::Ported, "merge_patch_node"),
    ("MergeBinaryJSON", Verdict::Ported, "merge_binary_json"),
    (
        "getAdjacentObjects",
        Verdict::Ported,
        "merge_binary_json object grouping",
    ),
    (
        "mergeBinaryArray",
        Verdict::Ported,
        "merge_binary_json array flattening",
    ),
    ("mergeBinaryObject", Verdict::Ported, "merge_preserve_node"),
    ("PeekBytesAsJSON", Verdict::Ported, "peek_binary_json_len"),
    (
        "ContainsBinaryJSON",
        Verdict::Ported,
        "contains_binary_json",
    ),
    (
        "OverlapsBinaryJSON",
        Verdict::Ported,
        "overlaps_binary_json",
    ),
    (
        "(bj BinaryJSON) GetElemDepth",
        Verdict::Ported,
        "BinaryJSON::element_depth",
    ),
    (
        "(bj BinaryJSON) Search",
        Verdict::Ported,
        "BinaryJSON::search",
    ),
    (
        "(bj BinaryJSON) extractToCallback",
        Verdict::Ported,
        "select_walk_roots_with",
    ),
    (
        "(bj BinaryJSON) Walk",
        Verdict::Ported,
        "BinaryJSON::walk_with",
    ),
];

const GO_TESTS: &[Row] = &[
    (
        "json_binary_functions_test.go::TestDecodeEscapedUnicode",
        Verdict::Ported,
        "test_decode_escaped_unicode",
    ),
    (
        "json_binary_functions_test.go::TestUnquoteJSONString",
        Verdict::Ported,
        "test_unquote_json_string",
    ),
    (
        "json_binary_functions_test.go::BenchmarkDecodeEscapedUnicode",
        Verdict::Ported,
        "benches/json.rs::BenchmarkDecodeEscapedUnicode",
    ),
    (
        "json_binary_functions_test.go::BenchmarkMergePatchBinary",
        Verdict::Ported,
        "benches/json.rs::BenchmarkMergePatchBinary",
    ),
    (
        "json_binary_functions_test.go::BenchmarkMergeBinary",
        Verdict::Ported,
        "benches/json.rs::BenchmarkMergeBinary",
    ),
    (
        "json_binary_functions_test.go::TestBinaryCompare",
        Verdict::Ported,
        "test_binary_compare_and_opaque",
    ),
    (
        "json_binary_test.go::TestBinaryJSONMarshalUnmarshal",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go marshalTo and unmarshal",
    ),
    (
        "json_binary_test.go::TestBinaryJSONExtract",
        Verdict::Ported,
        "test_binary_json_extract_source_rows",
    ),
    (
        "json_binary_test.go::TestBinaryJSONType",
        Verdict::Ported,
        "test_binary_json_type_unquote_keys_and_depth",
    ),
    (
        "json_binary_test.go::TestBinaryJSONUnquote",
        Verdict::Ported,
        "test_binary_json_type_unquote_keys_and_depth",
    ),
    (
        "json_binary_test.go::TestQuoteString",
        Verdict::Ported,
        "test_unquote_json_string",
    ),
    (
        "json_binary_test.go::TestBinaryJSONModify",
        Verdict::Ported,
        "test_binary_json_modify_and_remove_source_rows",
    ),
    (
        "json_binary_test.go::TestBinaryJSONRemove",
        Verdict::Ported,
        "test_binary_json_modify_and_remove_source_rows",
    ),
    (
        "json_binary_test.go::TestCompareBinary",
        Verdict::Ported,
        "test_binary_compare_and_opaque",
    ),
    (
        "json_binary_test.go::TestBinaryJSONMerge",
        Verdict::Ported,
        "test_binary_json_merge_and_contains_source_rows",
    ),
    (
        "json_binary_test.go::mustParseBinaryFromString",
        Verdict::Declined,
        "support helper for json_binary.go parsing; Rust tests use BinaryJSON::parse",
    ),
    (
        "json_binary_test.go::BenchmarkBinaryMarshal",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go marshalTo",
    ),
    (
        "json_binary_test.go::TestBinaryJSONContains",
        Verdict::Ported,
        "test_binary_json_merge_and_contains_source_rows",
    ),
    (
        "json_binary_test.go::TestBinaryJSONCopy",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go Copy",
    ),
    (
        "json_binary_test.go::TestGetKeys",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go GetKeys",
    ),
    (
        "json_binary_test.go::TestBinaryJSONDepth",
        Verdict::Ported,
        "test_binary_json_type_unquote_keys_and_depth",
    ),
    (
        "json_binary_test.go::TestParseBinaryFromString",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go ParseBinaryJSONFromString",
    ),
    (
        "json_binary_test.go::TestCreateBinary",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go CreateBinaryJSON",
    ),
    (
        "json_binary_test.go::TestFunctions",
        Verdict::Ported,
        "test_unquote_json_string and test_binary_json_peek_and_hash_source_contract",
    ),
    (
        "json_binary_test.go::TestBinaryJSONExtractCallback",
        Verdict::Ported,
        "test_binary_json_walk_and_search_source_rows",
    ),
    (
        "json_binary_test.go::TestBinaryJSONWalk",
        Verdict::Ported,
        "test_binary_json_walk_and_search_source_rows",
    ),
    (
        "json_binary_test.go::TestBinaryJSONOpaque",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go opaque access and marshal",
    ),
    (
        "json_binary_test.go::TestHashValue",
        Verdict::Declined,
        "owned by pkg/types/json_binary.go HashValue",
    ),
    (
        "json_binary_test.go::FuzzJSONExtract",
        Verdict::Ported,
        "fuzz_json_extract_source_seeds_do_not_produce_invalid_values",
    ),
];

// Each row is one observable source branch or one closed representation boundary.
// Several syntactic loops share one row when they implement the same rule for
// every element; the source hash prevents changing loop structure silently.
const BRANCHES: &[Row] = &[
    ("Type.object_array", Verdict::Ported, "test_binary_json_type_unquote_keys_and_depth"),
    ("Type.literal_null_boolean", Verdict::Ported, "test_binary_json_type_unquote_keys_and_depth"),
    ("Type.signed_unsigned_double_string", Verdict::Ported, "test_binary_json_type_unquote_keys_and_depth"),
    ("Type.opaque_blob_bit_other", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Type.date_datetime_timestamp_duration", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Type.unknown_panics", Verdict::Declined, "Go says panic(msg); Rust returns InvalidBinary rather than panic on a persisted unknown code"),
    ("Unquote.string", Verdict::Ported, "test_binary_json_type_unquote_keys_and_depth"),
    ("Unquote.non_string_uses_display", Verdict::Ported, "test_binary_json_type_unquote_keys_and_depth"),
    ("Unquote.invalid_utf8_string", Verdict::Declined, "Go hack.String can expose invalid UTF-8; Rust reports InvalidBinary at the UTF-8 boundary"),
    ("UnquoteString.short", Verdict::Ported, "test_unquote_json_string"),
    ("UnquoteString.double_quoted", Verdict::Ported, "test_unquote_json_string"),
    ("UnquoteString.unquoted", Verdict::Ported, "test_unquote_json_string"),
    ("unquote.plain_byte", Verdict::Ported, "test_unquote_json_string"),
    ("unquote.trailing_backslash_error", Verdict::Ported, "test_unquote_json_string"),
    ("unquote.quote_backspace_formfeed_newline_return_tab_slash", Verdict::Ported, "test_unquote_json_string"),
    ("unquote.unicode_single", Verdict::Ported, "test_unquote_json_string"),
    ("unquote.unicode_surrogate_pair", Verdict::Ported, "test_unquote_json_string"),
    ("unquote.unicode_bad_length_hex_or_pair", Verdict::Ported, "test_unquote_json_string"),
    ("unquote.unknown_escape_drops_slash", Verdict::Ported, "test_unquote_json_string"),
    ("decode.length_over_eight", Verdict::Ported, "test_decode_escaped_unicode"),
    ("decode.invalid_hex", Verdict::Ported, "test_decode_escaped_unicode"),
    ("decode.decoded_length_two_or_four", Verdict::Ported, "test_decode_escaped_unicode"),
    ("decode.invalid_decoded_length", Verdict::Ported, "test_decode_escaped_unicode"),
    ("decode.valid_bmp", Verdict::Ported, "test_decode_escaped_unicode"),
    ("decode.valid_surrogate_pair", Verdict::Ported, "test_decode_escaped_unicode"),
    ("decode.lone_surrogate", Verdict::Ported, "test_decode_escaped_unicode"),
    ("quote.seven_escaped_ascii_classes", Verdict::Ported, "test_unquote_json_string"),
    ("quote.other_ascii_including_controls_is_raw", Verdict::Ported, "test_unquote_json_string"),
    ("quote.valid_utf8", Verdict::Ported, "test_unquote_json_string"),
    ("quote.invalid_utf8_replacement", Verdict::Unreachable, "Rust &str proves invalid UTF-8 cannot reach quote_json_string"),
    ("quote.identifier_unquoted", Verdict::Ported, "test_unquote_json_string"),
    ("quote.non_identifier_or_escaped_quoted", Verdict::Ported, "test_unquote_json_string"),
    ("Extract.no_match", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("Extract.single_exact_match", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("Extract.single_multi_capable_autowrap", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("Extract.multiple_paths_array", Verdict::Ported, "test_extract_duplicate_paths_preserve_source_multiplicity"),
    ("extractTo.path_complete_identity_dedup", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.duplicate_arguments_not_deduped", Verdict::Ported, "test_extract_duplicate_paths_preserve_source_multiplicity"),
    ("extractTo.array_exact_asterisk_range", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.nonarray_zero_last_autowrap", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.nonarray_range_autowrap_boundary", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.nonarray_asterisk_no_match", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.object_asterisk_sorted", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.object_key_hit_miss", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.recursive_self_array_object_scalar", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("extractTo.one_stops_after_first", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("objectSearchKey.hit_miss_byte_order", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("build.array.literal_inline_nonliteral_offset", Verdict::Ported, "test_binary_json_extract_source_rows"),
    ("build.object.key_order_offsets_values", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("build.object.key_over_uint16", Verdict::Ported, "special_scalars_survive_every_container_operation"),
    ("Modify.path_value_count_mismatch", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("Modify.wildcard_or_range_error", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("Modify.left_to_right", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("Modify.insert_replace_set_dispatch", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("Modify.depth_limit", Verdict::Ported, "special_scalars_survive_every_container_operation"),
    ("ArrayInsert.root_or_nonindex_error", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("ArrayInsert.parent_missing_or_nonarray_unchanged", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("ArrayInsert.wildcard_parent_can_be_unchanged", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("ArrayInsert.negative_and_past_end_index", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("ArrayInsert.before_cell_and_append", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("Remove.root_error", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("Remove.multiselection_error", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("Remove.hit_miss_array_object_nested", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("modifier.set.hit_replace_miss_insert", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("modifier.replace.hit_and_miss", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("modifier.insert.hit_and_miss", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doInsert.parent_missing", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doInsert.nonarray_autowrap", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doInsert.array_append", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doInsert.nonobject_noop", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doInsert.object_before_middle_or_end", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doRemove.parent_missing_or_wrong_shape", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doRemove.array_exact_index_only", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("doRemove.object_key_hit_miss", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("rebuild.replacement_unmodified_scalar_container", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("rebuild.array_and_object_metadata", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("rebuild.literal_inline_and_payload_offset", Verdict::Ported, "test_binary_json_modify_and_remove_source_rows"),
    ("compareFloatPrecision.within_strict_epsilon", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("compareFloatPrecision.less_or_greater", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("compareInt.signed_less_equal_greater", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("compareUint.less_equal_greater", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("compareIntUint.negative_or_cast_compare", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.precedence_equal_or_different", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.null", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.false_true_reverse_subtraction", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.all_nine_numeric_type_pairs", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.nan_greater_both_directions", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.string_bytes", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.array_first_difference_or_length", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.object_count_key_value", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.opaque_payload_ignores_field_type", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.date_datetime_timestamp", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("Compare.duration", Verdict::Ported, "test_binary_compare_and_opaque"),
    ("MergePatch.empty_input_panics", Verdict::Declined, "measured Go index-out-of-range panic; Rust returns None as the safe empty result"),
    ("MergePatch.nil_input_or_patch", Verdict::Unreachable, "Rust &[BinaryJSON] excludes Go nil pointers by type"),
    ("MergePatch.last_nonobject_discards_prefix", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("mergePatch.nonobject_replaces", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("mergePatch.object_target_object_or_empty", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("mergePatch.null_deletes_existing_or_missing", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("mergePatch.recursive_member_and_sorted_output", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Merge.empty_is_array", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Merge.single_is_identity", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Merge.adjacent_object_runs_anywhere", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Merge.scalar_autowrap_and_array_flatten", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Merge.object_new_or_duplicate_key_recursive", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Peek.empty_or_unknown_error", Verdict::Ported, "test_binary_json_peek_and_hash_source_contract"),
    ("Peek.object_array_header_and_declared_length", Verdict::Ported, "test_binary_json_peek_and_hash_source_contract"),
    ("Peek.string_prefix_complete_or_incomplete", Verdict::Ported, "test_binary_json_peek_and_hash_source_contract"),
    ("Peek.fixed_eight_literal_duration", Verdict::Ported, "test_binary_json_peek_and_hash_source_contract"),
    ("Peek.opaque_subtype_and_length", Verdict::Ported, "test_binary_json_peek_and_hash_source_contract"),
    ("Peek.opaque_missing_subtype_panics", Verdict::Declined, "measured Go slice-bounds panic; Rust reports InvalidBinary"),
    ("Peek.varint_overflow_negative_length", Verdict::Declined, "Go can return a negative required length; Rust usize API rejects the malformed prefix"),
    ("Contains.object_target_object_all_keys", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Contains.object_nonobject_false", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Contains.array_target_array_all_elements", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Contains.array_target_scalar_any_element", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Contains.scalar_compare_equal", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Overlaps.swap_nonarray_array", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Overlaps.object_shared_equal_member", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Overlaps.object_other_false", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Overlaps.array_array_any_equal", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Overlaps.array_scalar_any_equal", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Overlaps.scalar_equality", Verdict::Ported, "test_binary_json_merge_and_contains_source_rows"),
    ("Depth.empty_container_or_scalar_one", Verdict::Ported, "test_binary_json_type_unquote_keys_and_depth"),
    ("Depth.nonempty_max_child_plus_one", Verdict::Ported, "test_binary_json_type_unquote_keys_and_depth"),
    ("Search.invalid_mode", Verdict::Unreachable, "JSONSearchMode enum has exactly One and All"),
    ("Search.compile_percent_underscore_escape_unicode", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Search.string_only", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Search.one_stops_first", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Search.all_collects", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Search.restricted_or_root_walk", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Search.zero_one_many_result_shape", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("extractCallback.complete_calls_callback", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("extractCallback.array_asterisk_index_range", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("extractCallback.nonarray_does_not_autowrap", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("extractCallback.object_asterisk_or_key", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("extractCallback.recursive_array_object", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("extractCallback.stop_or_error_propagates", Verdict::Ported, "test_walk_callback_stop_and_error_propagation"),
    ("Walk.path_dedup", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Walk.callback_before_children", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Walk.array_and_object_preorder", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
    ("Walk.callback_stop_global", Verdict::Ported, "test_walk_callback_stop_and_error_propagation"),
    ("Walk.callback_error_exact", Verdict::Ported, "test_walk_callback_stop_and_error_propagation"),
    ("Walk.selected_paths_or_root", Verdict::Ported, "test_binary_json_walk_and_search_source_rows"),
];

fn go_function_symbols(source: &str) -> Vec<String> {
    let mut symbols = Vec::new();
    for line in source.lines() {
        let Some(declaration) = line.trim_start().strip_prefix("func ") else {
            continue;
        };
        if declaration.starts_with('(') {
            let receiver_end = declaration.find(") ").expect("Go receiver terminates");
            let receiver = &declaration[..=receiver_end];
            let name = declaration[receiver_end + 2..]
                .split_once('(')
                .expect("Go method has arguments")
                .0;
            symbols.push(format!("{receiver} {name}"));
        } else {
            symbols.push(
                declaration
                    .split_once('(')
                    .expect("Go function has arguments")
                    .0
                    .to_owned(),
            );
        }
    }
    symbols.sort();
    symbols
}

fn inventory_symbols(rows: &[Row]) -> Vec<String> {
    let mut symbols = rows
        .iter()
        .map(|(symbol, _, _)| (*symbol).to_owned())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols
}

fn go_test_symbols(file: &str, source: &str) -> Vec<String> {
    let mut symbols = source
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("func "))
        .filter(|declaration| !declaration.starts_with('('))
        .map(|declaration| {
            format!(
                "{file}::{}",
                declaration
                    .split_once('(')
                    .expect("Go test/support function has arguments")
                    .0
            )
        })
        .collect::<Vec<_>>();
    symbols.sort();
    symbols
}

#[test]
fn json_binary_functions_go_source_and_declarations_are_still_current() {
    let source = include_str!("../../../../pkg/types/json_binary_functions.go");
    let actual = Sha256::digest(source.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(actual, GO_SOURCE_SHA256);
    assert_eq!(source.len(), GO_SOURCE_BYTE_COUNT);
    assert_eq!(source.lines().count(), GO_SOURCE_LINE_COUNT);
    assert_eq!(FUNCTIONS.len(), GO_FUNCTION_COUNT);
    assert_eq!(BRANCHES.len(), GO_BRANCH_RULE_COUNT);
    assert_eq!(go_function_symbols(source), inventory_symbols(FUNCTIONS));
}

#[test]
fn json_binary_functions_go_test_support_inventory_is_still_current() {
    let mut actual = go_test_symbols(
        "json_binary_functions_test.go",
        include_str!("../../../../pkg/types/json_binary_functions_test.go"),
    );
    actual.extend(go_test_symbols(
        "json_binary_test.go",
        include_str!("../../../../pkg/types/json_binary_test.go"),
    ));
    actual.sort();
    assert_eq!(GO_TESTS.len(), GO_TEST_SUPPORT_COUNT);
    assert_eq!(actual, inventory_symbols(GO_TESTS));
}

#[test]
fn json_binary_functions_inventory_has_no_unclassified_or_empty_reason() {
    for (name, verdict, reason) in FUNCTIONS.iter().chain(GO_TESTS).chain(BRANCHES) {
        assert!(!name.is_empty());
        assert!(!reason.is_empty());
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
    }
}

#[test]
fn every_ported_json_binary_functions_symbol_still_compiles() {
    let _ = BinaryJSON::type_name;
    let _ = BinaryJSON::unquote;
    let _ = unquote_string;
    let _ = unquote_json_string;
    let _ = decode_escaped_unicode;
    let _ = quote_json_string;
    let _ = BinaryJSON::extract;
    let _ = BinaryJSON::object_get;
    let _ = BinaryJSON::from_node;
    let _ = BinaryJSON::modify;
    let _ = BinaryJSON::array_insert;
    let _ = BinaryJSON::remove;
    let _ = compare_binary_json;
    let _ = merge_patch_binary_json;
    let _ = merge_binary_json;
    let _ = peek_binary_json_len;
    let _ = contains_binary_json;
    let _ = overlaps_binary_json;
    let _ = BinaryJSON::element_depth;
    let _ = BinaryJSON::search;
    let _ = BinaryJSON::extract_matches;
    let _ = BinaryJSON::walk;
    let _ = BinaryJSON::walk_with::<
        fn(JSONPathExpression, BinaryJSON) -> Result<bool, BinaryJSONError>,
    >;
    let _: Option<JSONModifyType> = None;
    let _: Option<JSONSearchMode> = None;
}
