// Copyright 2018 PingCAP, Inc.
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

package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestUnfoldableFuncs(t *testing.T) {
	_, ok := unFoldableFunctions[ast.Sysdate]
	require.True(t, ok)
}

// CREATE TABLE (..., ... AS (func(...))) is using IllegalFunctions4GeneratedColumns
// as list of functions that are illegal. This is basically a blocklist.
//
// This functions has knownGood as list of functions that should not be on this blocklist.
// This ensures that for new functions a conscious decision is made to allow or not allow
// the use in generated columns.
//
// Functions should only be allowed if they are:
// - Deterministic (this excludes RAND(), UUID(), CURRENT_TIMESTAMP(), etc)
// - Not dependent on session or global state (this excludes CONNECTION_ID(), CURRENT_USER(), etc)
// - Functions that have system interactions (this excludes GET_LOCK(), RELEASE_LOCK(), SLEEP(), ec)
func TestIllegalFunctions4GeneratedColumns(t *testing.T) {
	builtin := GetBuiltinList()
	legal := make([]string, 0) // Not on illegal list
	knownGood := []string{
		"abs",
		"acos",
		"adddate",
		"addtime",
		"aes_decrypt",
		"aes_encrypt",
		"and", // operator
		"any_value",
		"ascii",
		"asin",
		"atan",
		"atan2",
		"bin",
		"bin_to_uuid",
		"bit_count",
		"bit_length",
		"bitand", // bit_and results in: ERROR 1111 (HY000): Invalid use of group function
		"bitneg", // Not allowed. Maybe in known as bit_neg and bitneg
		"bitor",  // Not allowed.
		"bitxor", // Not allowed.
		"case",   // OK: (case when 1=1 then 2 else 3 end)
		"ceil",
		"ceiling",
		"char_func",
		"char_length",
		"character_length",
		"charset",
		"coalesce",
		"coercibility",
		"collation",
		"compress",
		"concat",
		"concat_ws",
		"conv",
		"convert",
		"convert_tz", // Allowed by MySQL and TiDB: (convert_tz('00:00:00', 'SYSTEM', 'Europe/Amsterdam'))
		"cos",
		"cot",
		"crc32",
		"date",
		"date_add",
		"date_format",
		"date_sub",
		"datediff",
		"day",
		"dayname",
		"dayofmonth",
		"dayofweek",
		"dayofyear",
		"decode",
		"default_func",
		"degrees",
		"div",
		"elt",
		"encode",
		"eq",
		"exp",
		"export_set",
		"extract",
		"field",
		"find_in_set",
		"floor",
		"format",
		"format_bytes",
		"format_nano_time",
		"from_base64",
		"from_days",
		"from_unixtime",
		"fts_match_word",
		"ge",
		"get_format",
		"getparam",
		"greatest",
		"grouping",
		"gt",
		"hex",
		"hour",
		"if",
		"ifnull",
		"ilike",
		"in",
		"inet6_aton",
		"inet6_ntoa",
		"inet_aton",
		"inet_ntoa",
		"insert_func",
		"instr",
		"intdiv",
		"interval",
		"is_ipv4",
		"is_ipv4_compat",
		"is_ipv4_mapped",
		"is_ipv6",
		"is_uuid",
		"isfalse",
		"isnull",
		"istrue",
		"json_array",
		"json_array_append",
		"json_array_insert",
		"json_contains",
		"json_contains_path",
		"json_depth",
		"json_extract",
		"json_insert",
		"json_keys",
		"json_length",
		"json_memberof",
		"json_merge_patch",
		"json_merge_preserve",
		"json_object",
		"json_overlaps",
		"json_pretty",
		"json_quote",
		"json_remove",
		"json_replace",
		"json_schema_valid",
		"json_search",
		"json_set",
		"json_storage_free",
		"json_storage_size",
		"json_type",
		"json_unquote",
		"json_valid",
		"last_day",
		"lastval",
		"lcase",
		"le",
		"least",
		"left",
		"leftshift",
		"length",
		"like",
		"ln",
		"locate",
		"log",
		"log10",
		"log2",
		"lower",
		"lpad",
		"lt",
		"ltrim",
		"make_set",
		"makedate",
		"maketime",
		"md5",
		"microsecond",
		"mid",
		"minus",
		"minute",
		"mod",
		"month",
		"monthname",
		"mul",
		"ne",
		"nextval",
		"not",
		"nulleq",
		"oct",
		"octet_length",
		"or",
		"ord",
		"password",
		"period_add",
		"period_diff",
		"pi",
		"plus",
		"position",
		"pow",
		"power",
		"quarter",
		"quote",
		"radians",
		"regexp",
		"regexp_instr",
		"regexp_like",
		"regexp_replace",
		"regexp_substr",
		"repeat",
		"replace",
		"reverse",
		"right",
		"rightshift",
		"round",
		"rpad",
		"rtrim",
		"sec_to_time",
		"second",
		"setval",
		"sha",
		"sha1",
		"sha2",
		"sign",
		"sin",
		"sm3", // TiDB specific?
		"space",
		"sqrt",
		"str_to_date",
		"strcmp",
		"subdate",
		"substr",
		"substring",
		"substring_index",
		"subtime",
		"tan",
		"tidb_decode_binary_plan",
		"tidb_decode_key",
		"tidb_decode_plan",
		"tidb_decode_sql_digests",
		"tidb_encode_index_key",
		"tidb_encode_record_key",
		"tidb_encode_sql_digest",
		"tidb_mvcc_info",
		"tidb_parse_tso",
		"tidb_parse_tso_logical",
		"tidb_shard",
		"time",
		"time_format",
		"time_to_sec",
		"timediff",
		"timestamp",
		"timestampadd",
		"timestampdiff",
		"to_base64",
		"to_days",
		"to_seconds",
		"translate",
		"trim",
		"truncate",
		"ucase",
		"unaryminus",
		"uncompress",
		"uncompressed_length",
		"unhex",
		"upper",
		"uuid_to_bin",
		"validate_password_strength",
		"vec_as_text",
		"vec_cosine_distance",
		"vec_dims",
		"vec_from_text",
		"vec_l1_distance",
		"vec_l2_distance",
		"vec_l2_norm",
		"vec_negative_inner_product",
		"vitess_hash", // TiDB specific
		"week",
		"weekday",
		"weekofyear",
		"weight_string",
		"xor",
		"year",
		"yearweek",
	}

	for _, fname := range builtin {
		_, ok := IllegalFunctions4GeneratedColumns[fname]
		if !ok {
			legal = append(legal, fname)
		}
	}
	require.Equal(t, knownGood, legal)
}
