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

//! Source-owned native policy from Go `pkg/expression/infer_pushdown.go`.

use crate::expression::Expression;
use tidb_kv::store_type::StoreType;
use tidb_datatype::{EvalType, FieldType, FieldTypeCode};
use tidb_hack::go_to_lower;
use tidb_proto::tipb::ScalarFuncSig;

/// The facts `infer_pushdown.go` reads from a resolved scalar function.
///
/// Optional facts are required only by the named special cases. Missing a
/// required fact refuses pushdown instead of guessing a remotely executable
/// signature.
#[derive(Clone, Copy, Debug)]
pub struct PushDownPolicy<'a> {
    /// Lowercase scalar function name.
    pub name: &'a str,
    /// Locally generated TiPB signature, when this reduced protocol has it.
    pub signature: ScalarFuncSig,
    /// Full Go TiPB signature name; overrides the reduced enum when nonempty.
    pub signature_name: &'a str,
    /// CAST source field type.
    pub source_type: Option<&'a FieldType>,
    /// CAST result field type.
    pub return_type: Option<&'a FieldType>,
    /// Function charset.
    pub charset: &'a str,
    /// Function collation.
    pub collation: &'a str,
    /// Whether CONV receives a CAST from a hybrid or binary literal.
    pub conv_casts_hybrid_or_binary_literal: bool,
    /// Whether MATCH AGAINST uses Boolean mode.
    pub fts_boolean_mode: bool,
    /// Whether MATCH AGAINST requests query expansion.
    pub fts_with_query_expansion: bool,
}

impl<'a> PushDownPolicy<'a> {
    /// Constructs the ordinary policy facts for a resolved local signature.
    #[must_use]
    pub const fn new(name: &'a str, signature: ScalarFuncSig) -> Self {
        Self {
            name,
            signature,
            signature_name: "",
            source_type: None,
            return_type: None,
            charset: "",
            collation: "",
            conv_casts_hybrid_or_binary_literal: false,
            fts_boolean_mode: false,
            fts_with_query_expansion: false,
        }
    }
}

/// Go `storeTypeMask`.
#[must_use]
pub const fn store_type_mask(store: StoreType) -> u32 {
    match store {
        StoreType::Unspecified => {
            (1 << StoreType::TiKv as u8)
                | (1 << StoreType::TiFlash as u8)
                | (1 << StoreType::TiDb as u8)
        }
        other => 1 << other as u8,
    }
}

/// Go `LoadExprPushdownBlacklist`'s published map: function name to the OR of
/// the store bits it is blacklisted for. Go holds it in the process-wide
/// atomic `DefaultExprPushDownBlacklist`; this tier passes a handle to it.
pub type ExprPushDownBlacklist = std::collections::HashMap<String, u32>;

/// Go `funcName2Alias`, minus its 241 identity entries.
///
/// The map exists so an operator may blacklist a function by the spelling
/// they know -- `<` rather than `lt` -- and most of its entries map a name to
/// itself, which the lookup's `if alias, ok := ...; ok` makes
/// indistinguishable from being absent. Only these 27 rewrites change
/// anything.
const FUNC_NAME_TO_ALIAS: &[(&str, &str)] = &[
    ("<<", "leftshift"),
    (">>", "rightshift"),
    (">=", "ge"),
    ("<=", "le"),
    ("=", "eq"),
    ("!=", "ne"),
    ("<>", "ne"),
    ("<", "lt"),
    (">", "gt"),
    ("+", "plus"),
    ("-", "minus"),
    ("&&", "bitand"),
    ("||", "bitor"),
    ("%", "mod"),
    ("xor_bit", "bitxor"),
    ("/", "div"),
    ("*", "mul"),
    ("!", "not"),
    ("~", "bitneg"),
    ("div", "intdiv"),
    ("xor_logic", "xor"),
    ("<=>", "nulleq"),
    ("+_unary", "unaryplus"),
    ("-_unary", "unaryminus"),
    ("is null", "isnull"),
    ("is true", "istrue"),
    ("is false", "isfalse"),
];

/// Go `LoadExprPushdownBlacklist`'s per-row name: lowercased, then rewritten
/// through `funcName2Alias` when it has an entry.
#[must_use]
pub fn blacklist_name(name: &str) -> String {
    let lowered = go_to_lower(name);
    FUNC_NAME_TO_ALIAS
        .iter()
        .find(|(from, _)| *from == lowered)
        .map_or(lowered, |(_, to)| (*to).to_owned())
}

/// Go `LoadExprPushdownBlacklist`'s per-row `store_type` word list, as the OR
/// of its bits. An unrecognized word contributes nothing, which is what Go's
/// `if`/`else if` chain does with it.
///
/// The words are NOT trimmed, because Go's are not: `strings.Split` on `,`
/// and then `typeString == kv.TiKV.Name()`, so a row written
/// `'tikv, tiflash'` sets the TiKV bit and NOT the TiFlash one. Trimming
/// would blacklist more than the operator wrote.
#[must_use]
pub fn blacklist_store_mask(store_types: &str) -> u32 {
    let mut mask = 0;
    for word in go_to_lower(store_types).split(',') {
        mask |= match word {
            "tikv" => 1 << StoreType::TiKv as u8,
            "tiflash" => 1 << StoreType::TiFlash as u8,
            "tidb" => 1 << StoreType::TiDb as u8,
            _ => 0,
        };
    }
    mask
}

/// Go `IsPushDownEnabled`, parameterized by the atomically published map.
#[must_use]
pub fn is_push_down_enabled(
    blacklist: &std::collections::HashMap<String, u32>,
    name: &str,
    store: StoreType,
) -> bool {
    blacklist
        .get(name)
        .is_none_or(|value| value & store_type_mask(store) != store_type_mask(store))
}

/// The pure part of Go `canFuncBePushed`, including the two-stage blacklist
/// lookup by function name and then `function.signature`.
#[must_use]
pub fn can_function_be_pushed(
    policy: &PushDownPolicy<'_>,
    store: StoreType,
    blacklist: &std::collections::HashMap<String, u32>,
) -> bool {
    let supported = match store {
        StoreType::TiKv => scalar_expr_supported_by_tikv(policy),
        StoreType::TiFlash => scalar_expr_supported_by_flash(policy),
        StoreType::TiDb => scalar_expr_supported_by_tidb(policy),
        StoreType::Unspecified => {
            scalar_expr_supported_by_tidb(policy)
                || scalar_expr_supported_by_tikv(policy)
                || scalar_expr_supported_by_flash(policy)
        }
    };
    if !supported || blacklist.is_empty() {
        return supported;
    }
    if !is_push_down_enabled(blacklist, policy.name, store) {
        return false;
    }
    let full_name = format!(
        "{}.{}",
        policy.name,
        signature_name(policy).to_ascii_lowercase()
    );
    is_push_down_enabled(blacklist, &full_name, store)
}

/// Go `scalarExprSupportedByTiDB`: TiDB admits a signature when either remote
/// engine's pure signature policy admits it.
#[must_use]
pub fn scalar_expr_supported_by_tidb(policy: &PushDownPolicy<'_>) -> bool {
    scalar_expr_supported_by_tikv(policy) || scalar_expr_supported_by_flash(policy)
}

fn signature_name<'a>(policy: &'a PushDownPolicy<'a>) -> &'a str {
    if policy.signature_name.is_empty() {
        policy.signature.as_str_name()
    } else {
        policy.signature_name
    }
}

/// Go `scalarExprSupportedByTiKV`, after signature resolution.
#[must_use]
pub fn scalar_expr_supported_by_tikv(policy: &PushDownPolicy<'_>) -> bool {
    if matches!(
        policy.name,
        "and"
            | "or"
            | "xor"
            | "not"
            | "bitand"
            | "bitor"
            | "bitxor"
            | "bitneg"
            | "leftshift"
            | "rightshift"
            | "unaryminus"
            | "lt"
            | "le"
            | "eq"
            | "ne"
            | "ge"
            | "gt"
            | "nulleq"
            | "in"
            | "isnull"
            | "like"
            | "istrue"
            | "istrue_with_null"
            | "isfalse"
            | "pi"
            | "plus"
            | "minus"
            | "mul"
            | "div"
            | "abs"
            | "mod"
            | "intdiv"
            | "ceil"
            | "ceiling"
            | "floor"
            | "sqrt"
            | "sign"
            | "ln"
            | "log"
            | "log2"
            | "log10"
            | "exp"
            | "pow"
            | "power"
            | "sin"
            | "asin"
            | "cos"
            | "acos"
            | "atan"
            | "atan2"
            | "cot"
            | "radians"
            | "degrees"
            | "crc32"
            | "case"
            | "if"
            | "ifnull"
            | "coalesce"
            | "upper"
            | "lower"
            | "length"
            | "bit_length"
            | "concat"
            | "concat_ws"
            | "replace"
            | "ascii"
            | "hex"
            | "reverse"
            | "ltrim"
            | "rtrim"
            | "strcmp"
            | "space"
            | "elt"
            | "field"
            | "from_binary"
            | "to_binary"
            | "mid"
            | "substring"
            | "substr"
            | "char_length"
            | "right"
            | "json_type"
            | "json_extract"
            | "json_object"
            | "json_array"
            | "json_merge"
            | "json_set"
            | "json_insert"
            | "json_replace"
            | "json_remove"
            | "json_length"
            | "json_merge_patch"
            | "json_unquote"
            | "json_contains"
            | "json_valid"
            | "json_memberof"
            | "json_array_append"
            | "vec_dims"
            | "vec_l1_distance"
            | "vec_l2_distance"
            | "vec_negative_inner_product"
            | "vec_cosine_distance"
            | "vec_l2_norm"
            | "vec_as_text"
            | "date"
            | "week"
            | "datediff"
            | "monthname"
            | "makedate"
            | "time_to_sec"
            | "maketime"
            | "date_format"
            | "date_add"
            | "adddate"
            | "date_sub"
            | "subdate"
            | "hour"
            | "minute"
            | "second"
            | "microsecond"
            | "month"
            | "dayofmonth"
            | "dayofweek"
            | "dayofyear"
            | "weekofyear"
            | "year"
            | "from_days"
            | "period_add"
            | "period_diff"
            | "timestampdiff"
            | "from_unixtime"
            | "sysdate"
            | "md5"
            | "sha1"
            | "sha2"
            | "uncompressed_length"
            | "cast"
            | "uuid"
            | "uuid_version"
            | "uuid_timestamp"
    ) {
        return true;
    }
    match policy.name {
        "unix_timestamp" => signature_name(policy) != "UnixTimestampCurrent",
        "conv" => !policy.conv_casts_hybrid_or_binary_literal,
        "round" => matches!(
            signature_name(policy),
            "RoundReal" | "RoundInt" | "RoundDec"
        ),
        "rand" => signature_name(policy) == "RandWithSeedFirstGen",
        "regexp" | "regexp_like" | "regexp_substr" | "regexp_instr" | "regexp_replace" => {
            policy.charset != "binary" || policy.collation != "binary"
        }
        _ => false,
    }
}

/// Go `scalarExprSupportedByFlash`, after signature resolution.
#[must_use]
pub fn scalar_expr_supported_by_flash(policy: &PushDownPolicy<'_>) -> bool {
    if matches!(policy.name, "floor" | "ceil" | "ceiling") {
        return !matches!(signature_name(policy), "FloorIntToDec" | "CeilIntToDec");
    }
    if matches!(
        policy.name,
        "or" | "and"
            | "not"
            | "bitneg"
            | "bitxor"
            | "bitand"
            | "bitor"
            | "rightshift"
            | "leftshift"
            | "ge"
            | "le"
            | "eq"
            | "ne"
            | "lt"
            | "gt"
            | "in"
            | "isnull"
            | "like"
            | "ilike"
            | "strcmp"
            | "plus"
            | "minus"
            | "div"
            | "mul"
            | "abs"
            | "mod"
            | "if"
            | "ifnull"
            | "case"
            | "concat"
            | "concat_ws"
            | "date"
            | "year"
            | "month"
            | "day"
            | "quarter"
            | "dayname"
            | "monthname"
            | "datediff"
            | "timestampdiff"
            | "date_format"
            | "from_unixtime"
            | "dayofweek"
            | "dayofmonth"
            | "dayofyear"
            | "last_day"
            | "weekofyear"
            | "to_seconds"
            | "from_days"
            | "to_days"
            | "sqrt"
            | "log"
            | "log2"
            | "log10"
            | "ln"
            | "exp"
            | "pow"
            | "power"
            | "sign"
            | "radians"
            | "degrees"
            | "conv"
            | "crc32"
            | "json_length"
            | "json_depth"
            | "json_extract"
            | "json_unquote"
            | "json_object"
            | "json_array"
            | "json_contains_path"
            | "json_valid"
            | "json_keys"
            | "repeat"
            | "inet_ntoa"
            | "inet_aton"
            | "inet6_ntoa"
            | "inet6_aton"
            | "coalesce"
            | "ascii"
            | "length"
            | "trim"
            | "position"
            | "format"
            | "elt"
            | "ltrim"
            | "rtrim"
            | "lpad"
            | "rpad"
            | "hour"
            | "minute"
            | "second"
            | "microsecond"
            | "time_to_sec"
    ) {
        return !matches!(
            signature_name(policy),
            "InDuration"
                | "CoalesceDuration"
                | "IfNullDuration"
                | "IfDuration"
                | "CaseWhenDuration"
                | "LTJson"
                | "LEJson"
                | "GTJson"
                | "GEJson"
                | "EQJson"
                | "NEJson"
                | "JsonIsNull"
                | "InJson"
        );
    }
    match policy.name {
        "regexp" | "regexp_like" | "regexp_instr" | "regexp_substr" | "regexp_replace" => {
            policy.charset != "binary" || policy.collation != "binary"
        }
        "substr" | "substring" | "left" | "right" | "char_length" | "substring_index"
        | "reverse" => matches!(
            signature_name(policy),
            "LeftUTF8"
                | "RightUTF8"
                | "CharLengthUTF8"
                | "Substring2ArgsUTF8"
                | "Substring3ArgsUTF8"
                | "SubstringIndex"
                | "ReverseUTF8"
                | "Reverse"
        ),
        "cast" => flash_cast_supported(policy),
        "date_add" | "adddate" => matches!(
            signature_name(policy),
            "AddDateDatetimeInt" | "AddDateStringInt" | "AddDateStringReal"
        ),
        "date_sub" | "subdate" => matches!(
            signature_name(policy),
            "SubDateDatetimeInt" | "SubDateStringInt" | "SubDateStringReal"
        ),
        "unix_timestamp" => matches!(
            signature_name(policy),
            "UnixTimestampInt" | "UnixTimestampDec"
        ),
        "round" => matches!(
            signature_name(policy),
            "RoundInt"
                | "RoundReal"
                | "RoundDec"
                | "RoundWithFracInt"
                | "RoundWithFracReal"
                | "RoundWithFracDec"
        ),
        "truncate" => matches!(
            signature_name(policy),
            "TruncateUint" | "TruncateInt" | "TruncateReal" | "TruncateDecimal"
        ),
        "extract" => matches!(
            signature_name(policy),
            "ExtractDatetime" | "ExtractDuration"
        ),
        "replace" => signature_name(policy) == "Replace",
        "str_to_date" => matches!(
            signature_name(policy),
            "StrToDateDate" | "StrToDateDatetime"
        ),
        "upper"
        | "ucase"
        | "lower"
        | "lcase"
        | "space"
        | "sysdate"
        | "istrue_with_null"
        | "istrue"
        | "isfalse"
        | "hex"
        | "unhex"
        | "bin"
        | "get_format"
        | "is_ipv4"
        | "is_ipv6"
        | "vec_dims"
        | "vec_l1_distance"
        | "vec_l2_distance"
        | "vec_negative_inner_product"
        | "vec_cosine_distance"
        | "vec_l2_norm"
        | "vec_as_text"
        | "fts_match_word"
        | "grouping" => true,
        "least" | "greatest" => matches!(
            signature_name(policy),
            "GreatestInt"
                | "GreatestReal"
                | "LeastInt"
                | "LeastReal"
                | "LeastString"
                | "GreatestString"
        ),
        "match_against" => !policy.fts_boolean_mode && !policy.fts_with_query_expansion,
        _ => false,
    }
}

fn flash_cast_supported(policy: &PushDownPolicy<'_>) -> bool {
    let (Some(source), Some(ret)) = (policy.source_type, policy.return_type) else {
        return false;
    };
    match signature_name(policy) {
        "CastDecimalAsInt" | "CastIntAsInt" | "CastRealAsInt" | "CastTimeAsInt"
        | "CastStringAsInt" => {
            (source.code() == ret.code() && source.is_unsigned() == ret.is_unsigned())
                || ret.code() == FieldTypeCode::LongLong
        }
        "CastIntAsReal" | "CastRealAsReal" | "CastStringAsReal" | "CastTimeAsReal"
        | "CastDecimalAsReal" => source.code() == ret.code() || ret.code() == FieldTypeCode::Double,
        "CastDecimalAsDecimal"
        | "CastIntAsDecimal"
        | "CastRealAsDecimal"
        | "CastTimeAsDecimal"
        | "CastStringAsDecimal" => ret.is_decimal_valid(),
        "CastDecimalAsString"
        | "CastIntAsString"
        | "CastRealAsString"
        | "CastTimeAsString"
        | "CastStringAsString"
        | "CastJsonAsString" => true,
        "CastDecimalAsTime" | "CastIntAsTime" | "CastRealAsTime" | "CastTimeAsTime"
        | "CastStringAsTime" => source.code() != FieldTypeCode::Year,
        "CastTimeAsDuration" => ret.code() == FieldTypeCode::Duration,
        "CastVectorFloat32AsString"
        | "CastVectorFloat32AsVectorFloat32"
        | "CastIntAsJson"
        | "CastRealAsJson"
        | "CastDecimalAsJson"
        | "CastStringAsJson"
        | "CastTimeAsJson"
        | "CastDurationAsJson"
        | "CastJsonAsJson" => true,
        _ => false,
    }
}

/// Go `canEnumPushdownPreliminarily`.
#[must_use]
pub fn can_enum_pushdown_preliminarily(name: &str, return_type: &FieldType) -> bool {
    name == "cast"
        && matches!(
            return_type.eval_type(),
            EvalType::Int | EvalType::Real | EvalType::Decimal
        )
}

// ***** Go `CanExprsPushDown` / `PushDownExprs` and the walk under them *****
//
// Go `canExprPushDown` first proves that an expression has a protobuf
// representation, and `canScalarFuncPushDown` then applies the store policy
// above recursively; concrete signatures come from `pushdown_catalog`.

/// Go `CanExprsPushDown(ctx, exprs, kv.TiKV)` over the expression families
/// represented by the shared TiPB scalar catalog.
#[must_use]
pub fn can_exprs_push_down_tikv(exprs: &[Expression]) -> bool {
    exprs.iter().all(can_expr_push_down_tikv)
}

/// Go `CanExprsPushDown(ctx, exprs, kv.TiFlash)` over the expression families
/// represented by the shared TiPB scalar catalog.
#[must_use]
pub fn can_exprs_push_down_tiflash(exprs: &[Expression]) -> bool {
    exprs.iter().all(can_expr_push_down_tiflash)
}

/// Shared admission for the logical datasource and store-specific consumers.
/// Go uses Unspecified before deriving any ordinary or index-merge ranges.
#[must_use]
pub fn can_exprs_push_down(
    exprs: &[Expression],
    store: crate::infer_pushdown::StoreType,
    blacklist: &crate::infer_pushdown::ExprPushDownBlacklist,
) -> bool {
    exprs
        .iter()
        .all(|expr| can_expr_push_down(expr, store, blacklist))
}

/// Go `expression.PushDownExprs`: the filters `store` can evaluate, then the
/// ones it cannot. Unlike [`split_scan_filters`] it keeps a virtual-column
/// filter: an index that covers the hidden column evaluates it.
pub fn push_down_exprs(
    filters: Vec<Expression>,
    store: crate::infer_pushdown::StoreType,
    blacklist: &crate::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    filters.into_iter().partition(|condition| {
        can_exprs_push_down(std::slice::from_ref(condition), store, blacklist)
    })
}

fn can_expr_push_down_tikv(expr: &Expression) -> bool {
    can_expr_push_down(
        expr,
        crate::infer_pushdown::StoreType::TiKv,
        &Default::default(),
    )
}

fn can_expr_push_down_tiflash(expr: &Expression) -> bool {
    can_expr_push_down(
        expr,
        crate::infer_pushdown::StoreType::TiFlash,
        &Default::default(),
    )
}

/// [`can_exprs_push_down`] that also reports, as Go's `canScalarFuncPushDown`
/// does through its `PushDownContext`, the outermost scalar function that
/// cannot be pushed: `report` receives Go's warning text. Go checks a
/// function before its arguments and stops at the first refusal.
pub fn can_exprs_push_down_reporting(
    exprs: &[Expression],
    store: crate::infer_pushdown::StoreType,
    blacklist: &crate::infer_pushdown::ExprPushDownBlacklist,
    report: &dyn Fn(String),
) -> bool {
    exprs
        .iter()
        .all(|expr| can_expr_push_down_reported(expr, store, blacklist, Some(report)))
}

fn can_expr_push_down(
    expr: &Expression,
    store: crate::infer_pushdown::StoreType,
    blacklist: &crate::infer_pushdown::ExprPushDownBlacklist,
) -> bool {
    can_expr_push_down_reported(expr, store, blacklist, None)
}

fn can_expr_push_down_reported(
    expr: &Expression,
    store: crate::infer_pushdown::StoreType,
    blacklist: &crate::infer_pushdown::ExprPushDownBlacklist,
    report: Option<&dyn Fn(String)>,
) -> bool {
    use crate::infer_pushdown::{is_push_down_enabled, StoreType};

    let Expression::ScalarFunction(function) = expr else {
        // Go PbConverter.columnToPBExpr checks column types independently of
        // the requested engine, including correlated columns.
        if matches!(
            expr,
            Expression::Column(_) | Expression::CorrelatedColumn(_)
        ) {
            if let Some(field_type) = expr.static_type() {
                use tidb_datatype::FieldTypeCode;
                return match field_type.code() {
                    FieldTypeCode::Enum => {
                        is_push_down_enabled(blacklist, "enum", StoreType::Unspecified)
                    }
                    FieldTypeCode::Bit => {
                        is_push_down_enabled(blacklist, "bit", StoreType::TiKv)
                    }
                    FieldTypeCode::Set | FieldTypeCode::Geometry | FieldTypeCode::Unspecified => {
                        false
                    }
                    _ => true,
                };
            }
        }
        return true;
    };
    // Go `canScalarFuncPushDown`: the function itself first, with the
    // refusal warning, then each argument.
    let signature = match crate::pushdown_catalog::from_expression(expr) {
        Some(crate::pushdown_catalog::PbScalar::Call { signature, .. }) => signature.sig,
        Some(crate::pushdown_catalog::PbScalar::Cast { signature, .. }) => signature,
        _ => tidb_proto::tipb::ScalarFuncSig::Unspecified,
    };
    if !scalar_function_can_be_pushed(expr, function, signature, store, blacklist) {
        if let Some(report) = report {
            let storage = match store {
                StoreType::TiKv => "tikv",
                StoreType::TiFlash => "tiflash",
                StoreType::TiDb => "tidb",
                _ => "storage layer",
            };
            report(format!(
                "Scalar function '{}'(signature: {}, return type: {}) is not supported to push down to {storage} now.",
                function.func_name.lowercase(),
                signature.as_str_name(),
                expr.static_type().map_or_else(String::new, |field_type| {
                    field_type.compact_str(tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH)
                }),
            ));
        }
        return false;
    }
    function
        .args
        .iter()
        .all(|arg| can_expr_push_down_reported(arg, store, blacklist, report))
}

/// Go `canFuncBePushed` and the PbCode check of `canScalarFuncPushDown`, for
/// the function alone.
fn scalar_function_can_be_pushed(
    expr: &Expression,
    function: &crate::scalar_function::ScalarFunction,
    signature: tidb_proto::tipb::ScalarFuncSig,
    store: crate::infer_pushdown::StoreType,
    blacklist: &crate::infer_pushdown::ExprPushDownBlacklist,
) -> bool {
    use crate::infer_pushdown::{can_function_be_pushed, PushDownPolicy, StoreType};
    use tidb_proto::tipb::ScalarFuncSig;

    let name = function.func_name.lowercase();
    // Dedicated Rust casts represent Go's single ast.Cast function family.
    let policy_name = if name.starts_with("cast_") {
        "cast"
    } else {
        name.as_ref()
    };
    if store == StoreType::TiKv
        && signature == ScalarFuncSig::Unspecified
        && matches!(
            policy_name,
            "if" | "ifnull"
                | "case"
                | "unix_timestamp"
                | "conv"
                | "round"
                | "rand"
                | "regexp"
                | "regexp_like"
                | "regexp_substr"
                | "regexp_instr"
                | "regexp_replace"
                // Go's `builtinPeriodAddSig`/`builtinPeriodDiffSig` never call
                // `setPbCode`, so `canScalarFuncPushDown` refuses their
                // unspecified signature although the name is listed.
                | "period_add"
                | "period_diff"
        )
    {
        return false;
    }
    // A cast between eval types with no `CastXAsY` signature (to or from a
    // vector, say) has no pb code in Go and refuses itself and its parent.
    if store == StoreType::TiKv && policy_name == "cast" {
        if let (Some(from), Some(to)) = (
            function.args.first().and_then(Expression::static_type),
            expr.static_type(),
        ) {
            if !crate::pushdown_catalog::cast_pair_has_signature(
                from.eval_type(),
                to.eval_type(),
            ) {
                return false;
            }
        }
    }
    let mut policy = PushDownPolicy::new(policy_name, signature);
    if policy_name == "conv" {
        policy.conv_casts_hybrid_or_binary_literal = function
            .args
            .first()
            .is_some_and(crate::scalar_function::conv_reads_bits);
    }
    if policy_name == "cast" {
        policy.source_type = function.args.first().and_then(Expression::static_type);
        policy.return_type = expr.static_type();
    }
    if matches!(
        policy_name,
        "regexp" | "regexp_like" | "regexp_substr" | "regexp_instr" | "regexp_replace"
    ) {
        (policy.charset, policy.collation) = function.collation.charset_and_collation();
    }
    can_function_be_pushed(&policy, store, blacklist)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy<'a>(name: &'a str, signature_name: &'a str) -> PushDownPolicy<'a> {
        PushDownPolicy {
            signature_name,
            ..PushDownPolicy::new(name, ScalarFuncSig::Unspecified)
        }
    }

    #[test]
    fn infer_pushdown_store_masks_match_go_bit_positions() {
        assert_eq!(store_type_mask(StoreType::TiKv), 1);
        assert_eq!(store_type_mask(StoreType::TiFlash), 2);
        assert_eq!(store_type_mask(StoreType::TiDb), 4);
        assert_eq!(store_type_mask(StoreType::Unspecified), 7);
    }

    #[test]
    fn infer_pushdown_blacklist_checks_name_then_exact_signature() {
        let mut blacklist = std::collections::HashMap::new();
        blacklist.insert("round.roundreal".to_owned(), 1);
        let round = policy("round", "RoundReal");
        assert!(!can_function_be_pushed(
            &round,
            StoreType::TiKv,
            &blacklist
        ));
        assert!(can_function_be_pushed(
            &round,
            StoreType::TiFlash,
            &blacklist
        ));
        blacklist.insert("round".to_owned(), 7);
        assert!(!can_function_be_pushed(
            &round,
            StoreType::Unspecified,
            &blacklist
        ));
    }

    #[test]
    fn infer_pushdown_tikv_special_boundaries_match_go() {
        assert!(scalar_expr_supported_by_tikv(&policy("plus", "PlusReal")));
        assert!(!scalar_expr_supported_by_tikv(&policy("tan", "Tan")));
        assert!(!scalar_expr_supported_by_tikv(&policy(
            "unix_timestamp",
            "UnixTimestampCurrent"
        )));
        assert!(scalar_expr_supported_by_tikv(&policy(
            "unix_timestamp",
            "UnixTimestampInt"
        )));
        let mut conv = policy("conv", "Conv");
        assert!(scalar_expr_supported_by_tikv(&conv));
        conv.conv_casts_hybrid_or_binary_literal = true;
        assert!(!scalar_expr_supported_by_tikv(&conv));
        assert!(scalar_expr_supported_by_tikv(&policy("round", "RoundDec")));
        assert!(!scalar_expr_supported_by_tikv(&policy(
            "round",
            "RoundWithFracDec"
        )));
        assert!(scalar_expr_supported_by_tikv(&policy(
            "rand",
            "RandWithSeedFirstGen"
        )));
        assert!(!scalar_expr_supported_by_tikv(&policy("rand", "Rand")));
        let mut regexp = policy("regexp_like", "RegexpLike");
        regexp.charset = "binary";
        regexp.collation = "binary";
        assert!(!scalar_expr_supported_by_tikv(&regexp));
        regexp.collation = "utf8mb4_bin";
        assert!(scalar_expr_supported_by_tikv(&regexp));
    }

    #[test]
    fn infer_pushdown_flash_signature_boundaries_match_go() {
        assert!(!scalar_expr_supported_by_flash(&policy(
            "floor",
            "FloorIntToDec"
        )));
        assert!(scalar_expr_supported_by_flash(&policy(
            "floor",
            "FloorReal"
        )));
        assert!(!scalar_expr_supported_by_flash(&policy("in", "InDuration")));
        assert!(scalar_expr_supported_by_flash(&policy("in", "InInt")));
        assert!(scalar_expr_supported_by_flash(&policy(
            "substring",
            "Substring3ArgsUTF8"
        )));
        assert!(!scalar_expr_supported_by_flash(&policy(
            "substring",
            "Substring3Args"
        )));
        assert!(scalar_expr_supported_by_flash(&policy(
            "date_add",
            "AddDateStringReal"
        )));
        assert!(!scalar_expr_supported_by_flash(&policy(
            "date_add",
            "AddDateDurationReal"
        )));
        assert!(scalar_expr_supported_by_flash(&policy(
            "str_to_date",
            "StrToDateDate"
        )));
        assert!(!scalar_expr_supported_by_flash(&policy(
            "str_to_date",
            "StrToDateDuration"
        )));
        let mut regexp = policy("regexp_replace", "RegexpReplace");
        regexp.charset = "binary";
        regexp.collation = "binary";
        assert!(!scalar_expr_supported_by_flash(&regexp));
        regexp.charset = "utf8mb4";
        assert!(scalar_expr_supported_by_flash(&regexp));
        for (name, accepted, rejected) in [
            ("date_sub", "SubDateStringInt", "SubDateDurationInt"),
            ("unix_timestamp", "UnixTimestampDec", "UnixTimestampCurrent"),
            ("round", "RoundWithFracReal", "RoundDuration"),
            ("truncate", "TruncateDecimal", "TruncateString"),
            ("extract", "ExtractDuration", "ExtractDate"),
            ("replace", "Replace", "ReplaceUtf8"),
            ("least", "LeastString", "LeastTime"),
        ] {
            assert!(scalar_expr_supported_by_flash(&policy(name, accepted)));
            assert!(!scalar_expr_supported_by_flash(&policy(name, rejected)));
        }
        assert!(scalar_expr_supported_by_flash(&policy(
            "grouping", "Grouping"
        )));
        assert!(!scalar_expr_supported_by_flash(&policy(
            "unknown", "Unknown"
        )));
    }

    #[test]
    fn infer_pushdown_flash_cast_and_fts_boundaries_match_go() {
        let source = FieldType::new(FieldTypeCode::Year);
        let datetime = FieldType::new(FieldTypeCode::Datetime);
        let mut cast = policy("cast", "CastIntAsTime");
        cast.source_type = Some(&source);
        cast.return_type = Some(&datetime);
        assert!(!scalar_expr_supported_by_flash(&cast));
        let source = FieldType::new(FieldTypeCode::LongLong);
        cast.source_type = Some(&source);
        assert!(scalar_expr_supported_by_flash(&cast));

        let unsigned_int = FieldType::new(FieldTypeCode::Long).with_unsigned(true);
        let signed_int = FieldType::new(FieldTypeCode::Long);
        let mut int_cast = policy("cast", "CastIntAsInt");
        int_cast.source_type = Some(&unsigned_int);
        int_cast.return_type = Some(&signed_int);
        assert!(!scalar_expr_supported_by_flash(&int_cast));
        let int_ret = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        int_cast.return_type = Some(&int_ret);
        assert!(scalar_expr_supported_by_flash(&int_cast));

        let real = FieldType::new(FieldTypeCode::Double);
        let mut real_cast = policy("cast", "CastIntAsReal");
        real_cast.source_type = Some(&source);
        real_cast.return_type = Some(&real);
        assert!(scalar_expr_supported_by_flash(&real_cast));
        real_cast.return_type = Some(&datetime);
        assert!(!scalar_expr_supported_by_flash(&real_cast));

        let valid_decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(12)
            .with_decimal(3);
        let invalid_decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(3)
            .with_decimal(4);
        let mut decimal_cast = policy("cast", "CastIntAsDecimal");
        decimal_cast.source_type = Some(&source);
        decimal_cast.return_type = Some(&valid_decimal);
        assert!(scalar_expr_supported_by_flash(&decimal_cast));
        decimal_cast.return_type = Some(&invalid_decimal);
        assert!(!scalar_expr_supported_by_flash(&decimal_cast));

        for signature in [
            "CastIntAsString",
            "CastVectorFloat32AsString",
            "CastVectorFloat32AsVectorFloat32",
            "CastDurationAsJson",
        ] {
            let mut supported = policy("cast", signature);
            supported.source_type = Some(&source);
            supported.return_type = Some(&datetime);
            assert!(scalar_expr_supported_by_flash(&supported));
        }
        let duration = FieldType::new(FieldTypeCode::Duration);
        let mut duration_cast = policy("cast", "CastTimeAsDuration");
        duration_cast.source_type = Some(&datetime);
        duration_cast.return_type = Some(&duration);
        assert!(scalar_expr_supported_by_flash(&duration_cast));
        duration_cast.return_type = Some(&datetime);
        assert!(!scalar_expr_supported_by_flash(&duration_cast));
        assert!(!scalar_expr_supported_by_flash(&policy(
            "cast",
            "CastDurationAsDuration"
        )));

        let mut fts = policy("match_against", "FTSMysqlMatchAgainst");
        assert!(scalar_expr_supported_by_flash(&fts));
        fts.fts_boolean_mode = true;
        assert!(!scalar_expr_supported_by_flash(&fts));
        fts.fts_boolean_mode = false;
        fts.fts_with_query_expansion = true;
        assert!(!scalar_expr_supported_by_flash(&fts));
    }

    #[test]
    fn infer_pushdown_tidb_is_union_and_enum_is_numeric_cast_only() {
        assert!(scalar_expr_supported_by_tidb(&policy("plus", "PlusReal")));
        assert!(scalar_expr_supported_by_tidb(&policy("ilike", "IlikeSig")));
        assert!(!scalar_expr_supported_by_tidb(&policy(
            "unknown", "Unknown"
        )));
        for code in [
            FieldTypeCode::LongLong,
            FieldTypeCode::Double,
            FieldTypeCode::NewDecimal,
        ] {
            assert!(can_enum_pushdown_preliminarily(
                "cast",
                &FieldType::new(code)
            ));
        }
        assert!(!can_enum_pushdown_preliminarily(
            "cast",
            &FieldType::new(FieldTypeCode::VarString)
        ));
        assert!(!can_enum_pushdown_preliminarily(
            "plus",
            &FieldType::new(FieldTypeCode::LongLong)
        ));
    }

    /// Go `LoadExprPushdownBlacklist`'s two per-row transforms.
    #[test]
    fn a_blacklist_row_is_lowercased_aliased_and_masked_as_go_does() {
        // `funcName2Alias`: the operator spelling and the function name land
        // on the same key, and a name with no entry is left alone.
        assert_eq!(blacklist_name("<"), "lt");
        assert_eq!(blacklist_name("LT"), "lt");
        assert_eq!(blacklist_name("<>"), "ne");
        assert_eq!(blacklist_name("!="), "ne");
        assert_eq!(blacklist_name("DIV"), "intdiv");
        assert_eq!(blacklist_name("Is Null"), "isnull");
        assert_eq!(blacklist_name("enum"), "enum");
        assert_eq!(blacklist_name("MOD"), "mod");

        let tikv = 1 << StoreType::TiKv as u8;
        let tiflash = 1 << StoreType::TiFlash as u8;
        let tidb = 1 << StoreType::TiDb as u8;
        assert_eq!(
            blacklist_store_mask("tikv,tiflash,tidb"),
            tikv | tiflash | tidb
        );
        assert_eq!(blacklist_store_mask("TiKV"), tikv);
        assert_eq!(blacklist_store_mask("mock"), 0);
        // NOT trimmed, because Go's `strings.Split` result is compared
        // verbatim: the space keeps `tiflash` out.
        assert_eq!(blacklist_store_mask("tikv, tiflash"), tikv);
    }

    /// Go `IsPushDownEnabled`: a store set refuses only when EVERY bit the
    /// question asks about is blacklisted.
    #[test]
    fn a_store_set_refuses_only_when_it_covers_the_question() {
        let mut blacklist = ExprPushDownBlacklist::new();
        blacklist.insert("lt".to_owned(), blacklist_store_mask("tikv"));
        // The TiKV question is covered.
        assert!(!is_push_down_enabled(&blacklist, "lt", StoreType::TiKv));
        // The `kv.UnSpecified` one asks for all three, and one bit is not all.
        assert!(is_push_down_enabled(
            &blacklist,
            "lt",
            StoreType::Unspecified
        ));
        blacklist.insert("lt".to_owned(), blacklist_store_mask("tikv,tiflash,tidb"));
        assert!(!is_push_down_enabled(
            &blacklist,
            "lt",
            StoreType::Unspecified
        ));
        // A name that is not in the map at all is always enabled.
        assert!(is_push_down_enabled(&blacklist, "gt", StoreType::TiKv));
    }
}

#[cfg(test)]
mod push_down_admission_tests {
    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use crate::column::Column;
    use crate::constant::Constant;
    use crate::scalar_function::ScalarFunction;

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        let mut sf = ScalarFunction::default();
        sf.func_name = tidb_ast::CiString::new(name.to_owned());
        sf.args = args;
        Expression::ScalarFunction(sf)
    }

    /// Go `canScalarFuncPushDown` checks a function before its arguments:
    /// a refused outer function is the one reported, and the walk stops
    /// there. A pushable outer function reports its refused argument.
    #[test]
    fn a_refused_outer_function_is_reported_before_its_arguments() {
        use crate::infer_pushdown::StoreType;
        let column = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let reported = std::cell::RefCell::new(Vec::new());
        let report = |message: String| reported.borrow_mut().push(message);
        let inner = func("json_overlaps", vec![column.clone(), column.clone()]);
        let outer = func("json_overlaps", vec![inner.clone(), column.clone()]);
        assert!(!can_exprs_push_down_reporting(
            &[outer],
            StoreType::Unspecified,
            &Default::default(),
            &report,
        ));
        assert_eq!(reported.borrow().len(), 1);
        reported.borrow_mut().clear();
        let pushable = func("isnull", vec![inner]);
        assert!(!can_exprs_push_down_reporting(
            &[pushable],
            StoreType::Unspecified,
            &Default::default(),
            &report,
        ));
        let messages = reported.borrow();
        assert_eq!(messages.len(), 1);
        assert!(
            messages[0].starts_with("Scalar function 'json_overlaps'(signature: Unspecified"),
            "{messages:?}"
        );
        assert!(messages[0].ends_with("is not supported to push down to storage layer now."));
    }

    #[test]
    fn datasource_admission_preserves_unspecified_store_blacklist_semantics() {
        use crate::infer_pushdown::{blacklist_store_mask, StoreType};
        let column = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let one = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let predicate = func("gt", vec![column, one]);
        let mut blacklist = Default::default();
        for name in ["gt", "gt.gtint"] {
            blacklist =
                std::collections::HashMap::from([(name.into(), blacklist_store_mask("tikv"))]);
            assert!(can_exprs_push_down(
                std::slice::from_ref(&predicate),
                StoreType::Unspecified,
                &blacklist
            ));
            assert!(!can_exprs_push_down(
                std::slice::from_ref(&predicate),
                StoreType::TiKv,
                &blacklist
            ));
            blacklist.insert(name.into(), blacklist_store_mask("tikv,tiflash,tidb"));
            assert!(!can_exprs_push_down(
                std::slice::from_ref(&predicate),
                StoreType::Unspecified,
                &blacklist
            ));
        }
        let enum_column = Expression::Column(Column::new(2, FieldType::new(FieldTypeCode::Enum)));
        blacklist.clear();
        blacklist.insert("enum".into(), blacklist_store_mask("tikv"));
        assert!(can_exprs_push_down(
            std::slice::from_ref(&enum_column),
            StoreType::TiKv,
            &blacklist
        ));
        blacklist.insert("enum".into(), blacklist_store_mask("tikv,tiflash,tidb"));
        assert!(!can_exprs_push_down(
            &[enum_column],
            StoreType::Unspecified,
            &blacklist
        ));
    }

    #[test]
    fn admission_is_the_exact_tikv_scalar_catalog_receipt() {
        let col = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let one = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));

        assert!(can_exprs_push_down_tikv(&[func(
            "eq",
            vec![col.clone(), one.clone()]
        )]));
        // Go admits only the one-argument RoundInt/RoundReal/RoundDec PbCodes.
        assert!(can_exprs_push_down_tikv(&[func(
            "round",
            vec![col.clone()]
        )]));
        assert!(!can_exprs_push_down_tikv(&[func(
            "round",
            vec![col.clone(), one]
        )]));
        // Go rejects a scalar name unless it resolves to a concrete TiKV
        // protobuf signature; conditional functions are no exception.
        assert!(!can_exprs_push_down_tikv(&[func("if", vec![col.clone()])]));
        assert!(!can_exprs_push_down_tikv(&[func("tan", vec![col])]));
    }

    /// Go builds every cast under the single name `ast.Cast`, which
    /// `scalarExprSupportedByTiKV` admits unconditionally
    /// (`pkg/expression/infer_pushdown.go:246`). Rust's dedicated-cast
    /// transcreation names each target type, so `cast_decimal` must answer
    /// like `cast`; otherwise a derived `not(isnull(cast(col)))` filter is
    /// left above the projection that defines the cast instead of inside the
    /// cop reader.
    #[test]
    fn a_dedicated_cast_name_answers_like_go_cast() {
        let col = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::NewDecimal)));
        let cast = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("cast_decimal"),
            FieldType::new(FieldTypeCode::NewDecimal),
            vec![col.clone()],
        ));
        assert!(can_exprs_push_down_tikv(std::slice::from_ref(&cast)));
        assert!(can_exprs_push_down_tikv(&[func(
            "not",
            vec![func("isnull", vec![cast])]
        )]));
        // The mapping is name-prefix only; a name outside the cast family is
        // still decided by the shared policy.
        assert!(!can_exprs_push_down_tikv(&[func("castaway", vec![col])]));
    }

    #[test]
    fn tiflash_admission_uses_the_shared_signature_policy() {
        let col = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let one = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        assert!(can_exprs_push_down_tiflash(&[func(
            "eq",
            vec![col.clone(), one.clone()],
        )]));
        assert!(can_exprs_push_down_tiflash(&[func(
            "if",
            vec![col.clone(), one.clone(), one.clone()],
        )]));
        // TiFlash does not admit TiKV-only signatures such as RAND, and an
        // unresolved function is rejected without a catalog signature.
        assert!(!can_exprs_push_down_tiflash(&[func(
            "rand",
            vec![col.clone()]
        )]));
        assert!(!can_exprs_push_down_tiflash(&[func("tan", vec![col])]));
    }
}
