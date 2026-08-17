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

//! Go `pkg/expression/builtin.go`: the `funcs` function-class registry, its
//! `baseFunctionClass` arity contract, and `pkg/expression/extension.go`'s
//! runtime-registered extension functions.
//!
//! # What is and is not here
//!
//! Go's `funcs` maps each name to a `functionClass` whose `getFunction`
//! performs THREE jobs: arity verification, argument/return type inference,
//! and collation derivation. This module ports the FIRST of those three
//! completely and exactly -- [`FUNCTION_CLASSES`] carries all 309 entries of
//! the Go map literal with each class's `baseFunctionClass{name, minArgs,
//! maxArgs}` arity pair, so [`verify_args_by_count`] is a faithful
//! `VerifyArgsWrapper`.
//!
//! Type inference and collation derivation are NOT here: they live in the
//! hundreds of per-class `getFunction` bodies, which this crate does not
//! model (see [`crate::scalar_function`]'s BRIDGE DECISION -- the Rust
//! `ScalarFunction` is name-keyed and holds its own args instead of a
//! `builtinFunc`). [`crate::new_function`] names the consequence for return
//! types as an explicit narrowing.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

/// The arity contract Go stores in `baseFunctionClass` (`builtin.go:583`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FunctionArity {
    /// Go `baseFunctionClass.minArgs`.
    pub min_args: usize,
    /// Go `baseFunctionClass.maxArgs`, where Go's `-1` sentinel (variadic) is
    /// spelled `None` here.
    pub max_args: Option<usize>,
}

impl FunctionArity {
    /// Whether `count` arguments satisfy this contract. Go
    /// `baseFunctionClass.verifyArgsByCount` (`builtin.go:597`).
    #[must_use]
    pub fn accepts(self, count: usize) -> bool {
        count >= self.min_args && self.max_args.is_none_or(|max| count <= max)
    }
}

/// Every entry of Go's `funcs` map (`builtin.go:659`), as
/// `(name, minArgs, maxArgs)` taken from each class's embedded
/// `baseFunctionClass` literal. Sorted by name so lookup can binary-search.
///
/// This table is the single source of registered-builtin names for the crate:
/// [`is_registered_builtin`] is a lookup into it, so a name and its arity can
/// never drift apart.
static FUNCTION_CLASSES: &[(&str, usize, Option<usize>)] = &[
    ("'tidb`.(dateliteral", 1, Some(1)),
    ("'tidb`.(timeliteral", 1, Some(1)),
    ("'tidb`.(timestampliteral", 1, Some(2)),
    ("abs", 1, Some(1)),
    ("acos", 1, Some(1)),
    ("adddate", 3, Some(3)),
    ("addtime", 2, Some(2)),
    ("aes_decrypt", 2, Some(3)),
    ("aes_encrypt", 2, Some(3)),
    ("and", 2, Some(2)),
    ("any_value", 1, Some(1)),
    ("ascii", 1, Some(1)),
    ("asin", 1, Some(1)),
    ("atan", 1, Some(2)),
    ("atan2", 2, Some(2)),
    ("benchmark", 2, Some(2)),
    ("bin", 1, Some(1)),
    ("bin_to_uuid", 1, Some(2)),
    ("bit_count", 1, Some(1)),
    ("bit_length", 1, Some(1)),
    ("bitand", 2, Some(2)),
    ("bitneg", 1, Some(1)),
    ("bitor", 2, Some(2)),
    ("bitxor", 2, Some(2)),
    ("case", 1, None),
    ("ceil", 1, Some(1)),
    ("ceiling", 1, Some(1)),
    ("char_func", 2, None),
    ("char_length", 1, Some(1)),
    ("character_length", 1, Some(1)),
    ("charset", 1, Some(1)),
    ("coalesce", 1, None),
    ("coercibility", 1, Some(1)),
    ("collation", 1, Some(1)),
    ("compress", 1, Some(1)),
    ("concat", 1, None),
    ("concat_ws", 2, None),
    ("connection_id", 0, Some(0)),
    ("conv", 3, Some(3)),
    ("convert", 2, Some(2)),
    ("convert_tz", 3, Some(3)),
    ("cos", 1, Some(1)),
    ("cot", 1, Some(1)),
    ("crc32", 1, Some(1)),
    ("curdate", 0, Some(0)),
    ("current_date", 0, Some(0)),
    ("current_resource_group", 0, Some(0)),
    ("current_role", 0, Some(0)),
    ("current_time", 0, Some(1)),
    ("current_timestamp", 0, Some(1)),
    ("current_user", 0, Some(0)),
    ("curtime", 0, Some(1)),
    ("database", 0, Some(0)),
    ("date", 1, Some(1)),
    ("date_add", 3, Some(3)),
    ("date_format", 2, Some(2)),
    ("date_sub", 3, Some(3)),
    ("datediff", 2, Some(2)),
    ("day", 1, Some(1)),
    ("dayname", 1, Some(1)),
    ("dayofmonth", 1, Some(1)),
    ("dayofweek", 1, Some(1)),
    ("dayofyear", 1, Some(1)),
    ("decode", 2, Some(2)),
    ("default_func", 1, Some(1)),
    ("degrees", 1, Some(1)),
    ("div", 2, Some(2)),
    ("elt", 2, None),
    ("encode", 2, Some(2)),
    ("eq", 2, Some(2)),
    ("exp", 1, Some(1)),
    ("export_set", 3, Some(5)),
    ("extract", 2, Some(2)),
    ("field", 2, None),
    ("find_in_set", 2, Some(2)),
    ("floor", 1, Some(1)),
    ("format", 2, Some(3)),
    ("format_bytes", 1, Some(1)),
    ("format_nano_time", 1, Some(1)),
    ("found_rows", 0, Some(0)),
    ("from_base64", 1, Some(1)),
    ("from_days", 1, Some(1)),
    ("from_unixtime", 1, Some(2)),
    ("fts_match_word", 2, Some(2)),
    ("ge", 2, Some(2)),
    ("get_format", 2, Some(2)),
    ("get_lock", 2, Some(2)),
    ("getparam", 1, Some(1)),
    ("greatest", 2, None),
    ("grouping", 1, Some(1)),
    ("gt", 2, Some(2)),
    ("hex", 1, Some(1)),
    ("hour", 1, Some(1)),
    ("if", 3, Some(3)),
    ("ifnull", 2, Some(2)),
    ("ilike", 3, Some(3)),
    ("in", 2, None),
    ("inet6_aton", 1, Some(1)),
    ("inet6_ntoa", 1, Some(1)),
    ("inet_aton", 1, Some(1)),
    ("inet_ntoa", 1, Some(1)),
    ("insert_func", 4, Some(4)),
    ("instr", 2, Some(2)),
    ("intdiv", 2, Some(2)),
    ("interval", 2, None),
    ("is_free_lock", 1, Some(1)),
    ("is_ipv4", 1, Some(1)),
    ("is_ipv4_compat", 1, Some(1)),
    ("is_ipv4_mapped", 1, Some(1)),
    ("is_ipv6", 1, Some(1)),
    ("is_used_lock", 1, Some(1)),
    ("is_uuid", 1, Some(1)),
    ("isfalse", 1, Some(1)),
    ("isnull", 1, Some(1)),
    ("istrue", 1, Some(1)),
    ("istrue_with_null", 1, Some(1)),
    ("json_array", 0, None),
    ("json_array_append", 3, None),
    ("json_array_insert", 3, None),
    ("json_contains", 2, Some(3)),
    ("json_contains_path", 3, None),
    ("json_depth", 1, Some(1)),
    ("json_extract", 2, None),
    ("json_insert", 3, None),
    ("json_keys", 1, Some(2)),
    ("json_length", 1, Some(2)),
    ("json_memberof", 2, Some(2)),
    ("json_merge", 2, None),
    ("json_merge_patch", 2, None),
    ("json_merge_preserve", 2, None),
    ("json_object", 0, None),
    ("json_overlaps", 2, Some(2)),
    ("json_pretty", 1, Some(1)),
    ("json_quote", 1, Some(1)),
    ("json_remove", 2, None),
    ("json_replace", 3, None),
    ("json_schema_valid", 2, Some(2)),
    ("json_search", 3, None),
    ("json_set", 3, None),
    ("json_storage_free", 1, Some(1)),
    ("json_storage_size", 1, Some(1)),
    ("json_type", 1, Some(1)),
    ("json_unquote", 1, Some(1)),
    ("json_valid", 1, Some(1)),
    ("last_day", 1, Some(1)),
    ("last_insert_id", 0, Some(1)),
    ("lastval", 1, Some(1)),
    ("lcase", 1, Some(1)),
    ("le", 2, Some(2)),
    ("least", 2, None),
    ("left", 2, Some(2)),
    ("leftshift", 2, Some(2)),
    ("length", 1, Some(1)),
    ("like", 3, Some(3)),
    ("ln", 1, Some(1)),
    ("load_file", 1, Some(1)),
    ("localtime", 0, Some(1)),
    ("localtimestamp", 0, Some(1)),
    ("locate", 2, Some(3)),
    ("log", 1, Some(2)),
    ("log10", 1, Some(1)),
    ("log2", 1, Some(1)),
    ("lower", 1, Some(1)),
    ("lpad", 3, Some(3)),
    ("lt", 2, Some(2)),
    ("ltrim", 1, Some(1)),
    ("make_set", 2, None),
    ("makedate", 2, Some(2)),
    ("maketime", 3, Some(3)),
    ("match_against", 2, None),
    ("md5", 1, Some(1)),
    ("microsecond", 1, Some(1)),
    ("mid", 2, Some(3)),
    ("minus", 2, Some(2)),
    ("minute", 1, Some(1)),
    ("mod", 2, Some(2)),
    ("month", 1, Some(1)),
    ("monthname", 1, Some(1)),
    ("mul", 2, Some(2)),
    ("name_const", 2, Some(2)),
    ("ne", 2, Some(2)),
    ("nextval", 1, Some(1)),
    ("not", 1, Some(1)),
    ("now", 0, Some(1)),
    ("nulleq", 2, Some(2)),
    ("oct", 1, Some(1)),
    ("octet_length", 1, Some(1)),
    ("or", 2, Some(2)),
    ("ord", 1, Some(1)),
    ("password", 1, Some(1)),
    ("period_add", 2, Some(2)),
    ("period_diff", 2, Some(2)),
    ("pi", 0, Some(0)),
    ("plus", 2, Some(2)),
    ("position", 2, Some(2)),
    ("pow", 2, Some(2)),
    ("power", 2, Some(2)),
    ("quarter", 1, Some(1)),
    ("quote", 1, Some(1)),
    ("radians", 1, Some(1)),
    ("rand", 0, Some(1)),
    ("random_bytes", 1, Some(1)),
    ("regexp", 2, Some(2)),
    ("regexp_instr", 2, Some(6)),
    ("regexp_like", 2, Some(3)),
    ("regexp_replace", 3, Some(6)),
    ("regexp_substr", 2, Some(5)),
    ("release_all_locks", 0, Some(0)),
    ("release_lock", 1, Some(1)),
    ("repeat", 2, Some(2)),
    ("replace", 3, Some(3)),
    ("reverse", 1, Some(1)),
    ("right", 2, Some(2)),
    ("rightshift", 2, Some(2)),
    ("round", 1, Some(2)),
    ("row", 2, None),
    ("row_count", 0, Some(0)),
    ("rpad", 3, Some(3)),
    ("rtrim", 1, Some(1)),
    ("schema", 0, Some(0)),
    ("sec_to_time", 1, Some(1)),
    ("second", 1, Some(1)),
    ("session_user", 0, Some(0)),
    ("setval", 2, Some(2)),
    ("setvar", 2, Some(2)),
    ("sha", 1, Some(1)),
    ("sha1", 1, Some(1)),
    ("sha2", 2, Some(2)),
    ("sign", 1, Some(1)),
    ("sin", 1, Some(1)),
    ("sleep", 1, Some(1)),
    ("sm3", 1, Some(1)),
    ("space", 1, Some(1)),
    ("sqrt", 1, Some(1)),
    ("str_to_date", 2, Some(2)),
    ("strcmp", 2, Some(2)),
    ("subdate", 3, Some(3)),
    ("substr", 2, Some(3)),
    ("substring", 2, Some(3)),
    ("substring_index", 3, Some(3)),
    ("subtime", 2, Some(2)),
    ("sysdate", 0, Some(1)),
    ("system_user", 0, Some(0)),
    ("tan", 1, Some(1)),
    ("tidb_bounded_staleness", 2, Some(2)),
    ("tidb_current_tso", 0, Some(0)),
    ("tidb_decode_binary_plan", 1, Some(1)),
    ("tidb_decode_key", 1, Some(1)),
    ("tidb_decode_plan", 1, Some(1)),
    ("tidb_decode_sql_digests", 1, Some(2)),
    ("tidb_encode_index_key", 4, None),
    ("tidb_encode_record_key", 3, None),
    ("tidb_encode_sql_digest", 1, Some(1)),
    ("tidb_is_ddl_owner", 0, Some(0)),
    ("tidb_mvcc_info", 1, Some(1)),
    ("tidb_parse_tso", 1, Some(1)),
    ("tidb_parse_tso_logical", 1, Some(1)),
    ("tidb_row_checksum", 0, Some(0)),
    ("tidb_shard", 1, Some(1)),
    ("tidb_version", 0, Some(0)),
    ("time", 1, Some(1)),
    ("time_format", 2, Some(2)),
    ("time_to_sec", 1, Some(1)),
    ("timediff", 2, Some(2)),
    ("timestamp", 1, Some(2)),
    ("timestampadd", 3, Some(3)),
    ("timestampdiff", 3, Some(3)),
    ("to_base64", 1, Some(1)),
    ("to_days", 1, Some(1)),
    ("to_seconds", 1, Some(1)),
    ("translate", 3, Some(3)),
    ("trim", 1, Some(3)),
    ("truncate", 2, Some(2)),
    ("ucase", 1, Some(1)),
    ("unaryminus", 1, Some(1)),
    ("uncompress", 1, Some(1)),
    ("uncompressed_length", 1, Some(1)),
    ("unhex", 1, Some(1)),
    ("unix_timestamp", 0, Some(1)),
    ("upper", 1, Some(1)),
    ("user", 0, Some(0)),
    ("utc_date", 0, Some(0)),
    ("utc_time", 0, Some(1)),
    ("utc_timestamp", 0, Some(1)),
    ("uuid", 0, Some(0)),
    ("uuid_short", 0, Some(0)),
    ("uuid_timestamp", 1, Some(1)),
    ("uuid_to_bin", 1, Some(2)),
    ("uuid_v4", 0, Some(0)),
    ("uuid_v7", 0, Some(0)),
    ("uuid_version", 1, Some(1)),
    ("validate_password_strength", 1, Some(1)),
    ("vec_as_text", 1, Some(1)),
    ("vec_cosine_distance", 2, Some(2)),
    ("vec_dims", 1, Some(1)),
    ("vec_from_text", 1, Some(1)),
    ("vec_l1_distance", 2, Some(2)),
    ("vec_l2_distance", 2, Some(2)),
    ("vec_l2_norm", 1, Some(1)),
    ("vec_negative_inner_product", 2, Some(2)),
    ("version", 0, Some(0)),
    ("vitess_hash", 1, Some(1)),
    ("week", 1, Some(2)),
    ("weekday", 1, Some(1)),
    ("weekofyear", 1, Some(1)),
    ("weight_string", 1, Some(3)),
    ("xor", 2, Some(2)),
    ("year", 1, Some(1)),
    ("yearweek", 1, Some(2)),
];

/// Go `extensionFuncs` (`pkg/expression/extension.go:33`), the `sync.Map` an
/// extension registers additional function classes into at startup.
///
/// NARROWING: Go stores a whole `functionClass`; this stores only the arity
/// contract, which is the only part [`crate::new_function`] consults. Go keys
/// by `string`; this keys by `&'static str` because the only registrations
/// are startup-time ones with literal names, and that keeps arity errors on
/// the existing `&'static str`-carrying error variant.
static EXTENSION_FUNCS: LazyLock<RwLock<HashMap<&'static str, FunctionArity>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Looks a name up in [`FUNCTION_CLASSES`], returning its `&'static str` key
/// alongside its arity so callers can report errors without allocating.
fn builtin_class(name: &str) -> Option<(&'static str, FunctionArity)> {
    FUNCTION_CLASSES
        .binary_search_by(|(candidate, _, _)| (*candidate).cmp(name))
        .ok()
        .map(|index| {
            let (key, min_args, max_args) = FUNCTION_CLASSES[index];
            (key, FunctionArity { min_args, max_args })
        })
}

/// The arity contract for `name`, consulting the builtin table first and then
/// the extension registry -- the same order as `newFunctionImpl`
/// (`scalar_function.go:220-225`).
#[must_use]
pub fn function_class(name: &str) -> Option<(&'static str, FunctionArity)> {
    if let Some(found) = builtin_class(name) {
        return Some(found);
    }
    let extensions = EXTENSION_FUNCS
        .read()
        .expect("extension function registry poisoned");
    extensions
        .get_key_value(name)
        .map(|(key, arity)| (*key, *arity))
}

/// Go `VerifyArgsWrapper` (`builtin.go:606`): verifies an argument count by
/// function name, and returns `Ok(())` for an unregistered name exactly as Go
/// does ("this function assumes that the function is supported").
///
/// # Errors
///
/// Returns [`crate::EvalError::WrongParameterCount`], Go's
/// `ErrIncorrectParameterCount` (`ErrWrongParamcountToNativeFct`, 1582), when
/// the count falls outside the class's `[minArgs, maxArgs]` range.
pub fn verify_args_by_count(name: &str, count: usize) -> Result<(), crate::EvalError> {
    let Some((key, arity)) = function_class(name) else {
        return Ok(());
    };
    if arity.accepts(count) {
        return Ok(());
    }
    Err(crate::EvalError::WrongParameterCount(key))
}

/// Go `RegisterExtensionFunc` (`pkg/expression/extension.go:54`), which uses
/// `LoadOrStore` and fails when the name is already taken.
///
/// # Errors
///
/// Returns [`crate::EvalError::FunctionNotExists`]-free duplicate reporting via
/// [`crate::EvalError::IncorrectArguments`] carrying Go's duplicate-name text.
pub fn register_extension_func(
    name: &'static str,
    arity: FunctionArity,
) -> Result<(), crate::EvalError> {
    if builtin_class(name).is_some() {
        return Err(crate::EvalError::IncorrectArguments(format!(
            "extension function name '{name}' conflicts with a builtin"
        )));
    }
    let mut extensions = EXTENSION_FUNCS
        .write()
        .expect("extension function registry poisoned");
    if extensions.contains_key(name) {
        return Err(crate::EvalError::IncorrectArguments(format!(
            "extension function '{name}' already registered"
        )));
    }
    extensions.insert(name, arity);
    Ok(())
}

/// Go `RemoveExtensionFunc` (`pkg/expression/extension.go:63`).
pub fn remove_extension_func(name: &str) {
    let mut extensions = EXTENSION_FUNCS
        .write()
        .expect("extension function registry poisoned");
    extensions.remove(name);
}

/// Names registered in Go's `pkg/expression/builtin.go::funcs`.
///
/// This is deliberately wider than the Rust evaluator. A registered but not
/// yet evaluated function remains an implementation-boundary 1105; only a
/// name absent from this set is TiDB's 1305 "FUNCTION ... does not exist".
pub(crate) fn is_registered_builtin(name: &str) -> bool {
    function_class(name).is_some()
}

pub(crate) fn unresolved_error(name: &str, current_database: Option<String>) -> crate::EvalError {
    // These names are in Go's registry so the parser can recognize them, but
    // their function classes deliberately return ErrFunctionNotExists.
    let unavailable = match name {
        "default_func" => Some("DEFAULT"),
        "uuid_short" => Some("UUID_SHORT"),
        _ => None,
    };
    if let Some(display_name) = unavailable {
        return crate::EvalError::FunctionNotExists(display_name.to_owned());
    }
    if is_registered_builtin(name) {
        return crate::EvalError::Unsupported("this builtin is not yet built for chunk evaluation");
    }
    match current_database {
        Some(database) if !database.is_empty() => {
            crate::EvalError::FunctionNotExists(format!("{database}.{name}"))
        }
        _ => crate::EvalError::NoDatabaseSelected,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        function_class, is_registered_builtin, register_extension_func, remove_extension_func,
        unresolved_error, FunctionArity, FUNCTION_CLASSES,
    };
    use crate::EvalError;

    #[test]
    fn source_registry_distinguishes_unported_from_unknown() {
        assert!(is_registered_builtin("get_lock"));
        assert!(is_registered_builtin("last_insert_id"));
        assert!(!is_registered_builtin("no_such_fn"));
    }

    #[test]
    fn registered_but_unavailable_functions_keep_source_names() {
        assert_eq!(
            unresolved_error("uuid_short", Some("test".to_owned())),
            EvalError::FunctionNotExists("UUID_SHORT".to_owned())
        );
        assert_eq!(
            unresolved_error("default_func", None),
            EvalError::FunctionNotExists("DEFAULT".to_owned())
        );
    }

    /// The table is binary-searched, so an unsorted or duplicated entry would
    /// silently make a builtin unreachable.
    #[test]
    fn function_class_table_is_sorted_and_unique() {
        assert!(FUNCTION_CLASSES
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(FUNCTION_CLASSES.len(), 309, "Go funcs map has 309 entries");
    }

    /// Spot-checks arities transcribed from Go's `funcs` map literal,
    /// including both variadic sentinels and the two the FTS fallback calls.
    #[test]
    fn arities_match_the_go_function_class_literals() {
        let expect = |name: &str, min_args: usize, max_args: Option<usize>| {
            assert_eq!(
                function_class(name).map(|(_, arity)| arity),
                Some(FunctionArity { min_args, max_args }),
                "{name}"
            );
        };
        // ast.Ilike / ast.Ifnull: the two the LIKE fallback builds.
        expect("ilike", 3, Some(3));
        expect("ifnull", 2, Some(2));
        expect("not", 1, Some(1));
        // Variadic classes carry Go's -1 maxArgs sentinel as None.
        expect("coalesce", 1, None);
        expect("concat", 1, None);
        expect("greatest", 2, None);
        // A class whose arity is a genuine range, not a fixed count.
        expect("atan", 1, Some(2));
        expect("now", 0, Some(1));
    }

    #[test]
    fn extension_functions_register_and_unregister() {
        let arity = FunctionArity {
            min_args: 1,
            max_args: Some(2),
        };
        assert!(!is_registered_builtin("agent_test_ext_fn"));
        register_extension_func("agent_test_ext_fn", arity).unwrap();
        assert_eq!(
            function_class("agent_test_ext_fn"),
            Some(("agent_test_ext_fn", arity))
        );
        // Go's LoadOrStore rejects a second registration of the same name.
        assert!(register_extension_func("agent_test_ext_fn", arity).is_err());
        // An extension may not shadow a builtin.
        assert!(register_extension_func("abs", arity).is_err());
        remove_extension_func("agent_test_ext_fn");
        assert!(!is_registered_builtin("agent_test_ext_fn"));
    }
}
