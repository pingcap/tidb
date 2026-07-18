// Copyright 2025 PingCAP, Inc.
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

package generator

import (
	"encoding/json"
	"fmt"

	"github.com/openai/openai-go"
	"github.com/pingcap/tidb/tests/llmtest/logger"
	"github.com/pingcap/tidb/tests/llmtest/testcase"
	"go.uber.org/zap"
)

type expressionPromptGenerator struct {
}

// Name implements PromptGenerator.Name
func (g *expressionPromptGenerator) Name() string {
	return "expression"
}

// Groups implements PromptGenerator.Groups
func (g *expressionPromptGenerator) Groups() []string {
	return []string{
		// scalar functions
		"and", "cast", "<<", ">>", "or", ">=", "<=", "=", "!=", "<", ">", "+", "-", "&", "|", "%", "^", "/", "*", "not", "~", "div", "xor", "<=>", "+", "-", "in", "like", "case", "regexp", "regexp_like", "regexp_substr", "regexp_instr", "regexp_replace", "is", "row", "bit_count",
		// "ilike"

		// common functions
		"coalesce", "greatest", "least", "interval",

		// math functions
		"abs", "acos", "asin", "atan", "atan2", "ceil", "ceiling", "conv", "cos", "cot", "crc32", "degrees", "exp", "floor", "ln", "log", "log2", "log10", "pi", "pow", "power", "radians", "round", "sign", "sin", "sqrt", "tan", "truncate",
		// "rand"

		// time functions
		"adddate", "addtime", "convert_tz", "curdate", "current_date", "date", "dateliteral", "date_add", "date_format", "date_sub", "datediff", "day", "dayname", "dayofmonth", "dayofweek", "dayofyear", "extract", "from_days", "from_unixtime", "get_format", "hour", "localtime", "localtimestamp", "makedate", "maketime", "microsecond", "minute", "month", "monthname", "period_add", "period_diff", "quarter", "sec_to_time", "second", "str_to_date", "subdate", "subtime", "sysdate", "time", "timeliteral", "time_format", "time_to_sec", "timediff", "timestamp", "timestampliteral", "timestampadd", "timestampdiff", "to_days", "to_seconds", "unix_timestamp", "utc_date", "utc_time", "utc_timestamp", "week", "weekday", "weekofyear", "year", "yearweek", "last_day",
		// "current_time", "current_timestamp", "curtime", "now", "tidb_bounded_staleness", "tidb_parse_tso", "tidb_parse_tso_logical", "tidb_current_tso",

		// string functions
		"ascii", "bin", "concat", "concat_ws", "convert", "elt", "export_set", "field", "format", "from_base64", "instr", "lcase", "left", "length", "locate", "lower", "lpad", "ltrim", "make_set", "mid", "oct", "octet_length", "ord", "position", "quote", "repeat", "replace", "reverse", "right", "rtrim", "space", "strcmp", "substring", "substr", "substring_index", "to_base64", "trim", "upper", "ucase", "hex", "unhex", "rpad", "bit_length", "char_length", "character_length", "find_in_set", "weight_string",
		// "soundex", "insert_func", "char_func", "load_file", "translate"

		// information functions
		"charset", "coercibility", "collation", "current_user", "database", "found_rows", "last_insert_id", "row_count", "schema", "session_user", "system_user", "user", "format_bytes",
		// "tidb_version", "tidb_is_ddl_owner", "tidb_decode_plan", "tidb_decode_binary_plan", "tidb_decode_sql_digests", "tidb_encode_sql_digest", "current_resource_group", "connection_id", "benchmark", "version", "current_role", "format_nano_time"

		// control functions
		"if", "ifnull", "nullif",

		// miscellaneous functions
		"inet_aton", "inet_ntoa", "inet6_aton", "inet6_ntoa", "is_ipv4", "is_ipv4_compat", "is_ipv4_mapped", "is_ipv6", "is_uuid", "name_const", "uuid_to_bin", "bin_to_uuid", "grouping",
		// "master_pos_wait", "vitess_hash", "get_lock", "release_lock", "release_all_locks", "is_free_lock", "is_used_lock", "tidb_shard", "tidb_row_checksum", "sleep", "uuid", "uuid_short",

		// encryption and compression functions
		"aes_decrypt", "aes_encrypt", "md5", "sha1", "sha", "sha2", "uncompress", "uncompressed_length", "validate_password_strength",
		// "password", "sm3", "random_bytes", "encode"

		// json functions
		"json_type", "json_extract", "json_unquote", "json_array", "json_object", "json_merge", "json_set", "json_insert", "json_replace", "json_remove", "json_overlaps", "json_contains", "json_contains_path", "json_valid", "json_array_append", "json_array_insert", "json_merge_patch", "json_merge_preserve", "json_quote", "json_schema_valid", "json_search", "json_depth", "json_keys", "json_length",
		// "json_pretty", "json_storage_free", "json_storage_size", "json_memberof"

		// vector functions (tidb extension)
		// "vec_dims", "vec_l1_distance", "vec_l2_distance", "vec_negative_inner_product", "vec_cosine_distance", "vec_l2_norm", "vec_from_text", "vec_as_text",

		// TiDB internal function
		// "tidb_decode_key", "tidb_mvcc_info", "tidb_encode_record_key", "tidb_encode_index_key", "tidb_decode_base64_key",

		// Sequence function
		// "nextval", "lastval", "setval",
	}
}

type simplePromptResponse struct {
	Queries []string `json:"queries"`
}

// GeneratePrompt implements PromptGenerator.GeneratePrompt
func (g *expressionPromptGenerator) GeneratePrompt(group string, count int, existCases []*testcase.Case) []openai.ChatCompletionMessageParamUnion {
	messages := make([]openai.ChatCompletionMessageParamUnion, 0, 2)

	systemPrompt := `You are a professional QA engineer testing a new SQL database compatible with MySQL. You are tasked with testing the compatibility of the database with MySQL for a specific function. You shouldn't use any Database and Tables in your queries. You should write the queries to cover the corner cases of the function. The common cases are not needed. You should try to use this function with different valid argument types to test the implicit type conversion. You should try to use this function with NULL to test the behavior of NULL. Please return a valid JSON object with the key "queries" and an array of strings as the value. Be careful with the escape characters. You should avoid using NOW(), RAND() or any other functions that return different results on each call.

	IMPORTANT: Don't put anything else in the response.

	EXAMPLE INPUT:
	Return 3 random SQL queries using this function: CONCAT.
	
	EXAMPLE JSON OUTPUT:
	{"queries": ["SELECT CONCAT('a', 'b')", "SELECT CONCAT(1, 'd')", "SELECT CONCAT(1, '\\n')"]}`
	messages = append(messages, openai.SystemMessage(systemPrompt))

	userPromptTemplate := `Return %d random SQL queries using this function: %s.`

	if len(existCases) > 0 {
		messages = append(messages, openai.UserMessage(fmt.Sprintf(userPromptTemplate, len(existCases), group)))

		existResponse := make([]string, 0, len(existCases))
		for _, c := range existCases {
			existResponse = append(existResponse, c.SQL)
		}
		assistantMessage, err := json.Marshal(simplePromptResponse{
			Queries: existResponse,
		})
		// should never happen
		if err != nil {
			logger.Global.Info("failed to marshal exist response", zap.Error(err))
			return nil
		}
		messages = append(messages, openai.AssistantMessage(string(assistantMessage)))
	}
	messages = append(messages, openai.UserMessage(fmt.Sprintf(userPromptTemplate, count, group)))

	return messages
}

// Unmarshal implements PromptGenerator.Unmarshal
func (g *expressionPromptGenerator) Unmarshal(response string) []testcase.Case {
	var resp simplePromptResponse
	err := json.Unmarshal([]byte(response), &resp)
	if err != nil {
		logger.Global.Error("failed to unmarshal expression prompt response", zap.Error(err), zap.String("response", response))
		return nil
	}

	cases := make([]testcase.Case, 0, len(resp.Queries))
	for _, q := range resp.Queries {
		cases = append(cases, testcase.Case{
			SQL: q,
		})
	}

	return cases
}

func init() {
	registerPromptGenerator(&expressionPromptGenerator{})
}
