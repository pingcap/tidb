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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Expression restore and `Format`: the SQL text an [`Expr`] tree prints back
//! to, mirroring the `Restore`/`Format` methods Go hangs off its expression
//! nodes in `pkg/parser/ast/expressions.go`.

use super::cast::{restore_cast_type, restore_typed_literal};
use super::*;
use tidb_mysql::to_lowercase as identifier_to_lower;

impl Expr {
    /// Formats this expression using Go AST's `ExprNode.Format` contract.
    ///
    /// This is intentionally separate from [`Self::restore`]: `Format`
    /// uses double-quoted strings, lowercase function names, and spaces
    /// around every binary operator. Go leaves several expression kinds
    /// unimplemented; the corresponding Rust variants panic as well.
    pub fn format(&self) -> String {
        let mut out = String::new();
        self.format_into(&mut out);
        out
    }

    pub(crate) fn format_into(&self, out: &mut String) {
        match self {
            Self::Column(path) => restore_path(path, out),
            Self::ParamMarker { .. } => panic!("Format is not implemented for parameter markers"),
            Self::Int(value) => out.push_str(&normalize_int(value)),
            Self::Decimal(value) => out.push_str(&normalize_decimal(value)),
            Self::Float(value) => out.push_str(&format_go_float(*value)),
            Self::Hex(value) => {
                out.push_str("x'");
                out.push_str(value);
                out.push('\'');
            }
            Self::Bit(value) => {
                out.push_str("b'");
                out.push_str(value);
                out.push('\'');
            }
            Self::String(value) | Self::RawString(value) | Self::CharsetString { value, .. } => {
                format_double_quoted_string(value, out);
            }
            Self::CharsetBinary { .. } => {
                panic!("Format is not implemented for charset binary literals")
            }
            Self::Null => out.push_str("NULL"),
            Self::Bool(true) => out.push_str("TRUE"),
            Self::Bool(false) => out.push_str("FALSE"),
            Self::Default(None) => out.push_str("DEFAULT"),
            Self::Default(Some(_)) => panic!("Format is not implemented for DEFAULT(column)"),
            Self::UserVar(_) | Self::SysVar { .. } | Self::Assign { .. } => {
                panic!("Format is not implemented for variable expressions")
            }
            Self::Unary(op, expr) => {
                out.push_str(op.restore());
                expr.format_into(out);
            }
            Self::Binary(op, left, right) => {
                left.format_into(out);
                out.push(' ');
                out.push_str(op.opcode().literal());
                out.push(' ');
                right.format_into(out);
            }
            Self::Paren(expr) => {
                out.push('(');
                expr.format_into(out);
                out.push(')');
            }
            Self::Row(_) => panic!("Format is not implemented for row expressions"),
            Self::Func { name, args, .. } => {
                out.push_str(&identifier_to_lower(name));
                out.push('(');
                format_expr_list(args, out, ", ");
                out.push(')');
            }
            Self::GenericFuncCall { name, args, .. } => {
                out.push_str(&identifier_to_lower(name));
                out.push('(');
                format_expr_list(args, out, ", ");
                out.push(')');
            }
            Self::Aggregate { .. } | Self::GroupConcat { .. } | Self::Window { .. } => {
                panic!("Format is not implemented for aggregate or window expressions")
            }
            Self::Interval { value, unit } => {
                out.push_str("INTERVAL ");
                value.format_into(out);
                out.push(' ');
                out.push_str(unit);
            }
            Self::Extract { unit, value } => {
                out.push_str("extract(");
                out.push_str(unit);
                out.push_str(" FROM ");
                value.format_into(out);
                out.push(')');
            }
            Self::Position { .. } | Self::WeightString { .. } | Self::Trim { .. } => {
                panic!("Format is not implemented for this special function")
            }
            Self::TimestampAdd {
                unit,
                interval,
                expr,
            } => {
                out.push_str("timestampadd(");
                out.push_str(unit);
                out.push_str(", ");
                interval.format_into(out);
                out.push_str(", ");
                expr.format_into(out);
                out.push(')');
            }
            Self::TimestampDiff { unit, expr1, expr2 } => {
                out.push_str("timestampdiff(");
                out.push_str(unit);
                out.push_str(", ");
                expr1.format_into(out);
                out.push_str(", ");
                expr2.format_into(out);
                out.push(')');
            }
            Self::GetFormat { selector, expr } => {
                out.push_str("get_format(");
                out.push_str(match selector {
                    GetFormatSelector::Date => "DATE",
                    GetFormatSelector::Time => "TIME",
                    GetFormatSelector::Datetime => "DATETIME",
                });
                out.push_str(", ");
                expr.format_into(out);
                out.push(')');
            }
            Self::In { expr, list, not } => {
                expr.format_into(out);
                out.push_str(if *not { " NOT IN (" } else { " IN (" });
                format_expr_list(list, out, ",");
                out.push(')');
            }
            Self::Between {
                expr,
                low,
                high,
                not,
            } => {
                expr.format_into(out);
                out.push_str(if *not { " NOT BETWEEN " } else { " BETWEEN " });
                low.format_into(out);
                out.push_str(" AND ");
                high.format_into(out);
            }
            Self::Like {
                expr,
                pattern,
                not,
                ilike,
                escape,
            } => {
                expr.format_into(out);
                out.push_str(match (*not, *ilike) {
                    (true, true) => " NOT ILIKE ",
                    (true, false) => " NOT LIKE ",
                    (false, true) => " ILIKE ",
                    (false, false) => " LIKE ",
                });
                pattern.format_into(out);
                if let Some(escape) = escape {
                    out.push_str(" ESCAPE '");
                    if *escape != 0 {
                        out.push(*escape as char);
                    }
                    out.push('\'');
                }
            }
            Self::Regexp { expr, pattern, not } => {
                expr.format_into(out);
                out.push_str(if *not { " NOT REGEXP " } else { " REGEXP " });
                pattern.format_into(out);
            }
            Self::Is { expr, target, not } => {
                expr.format_into(out);
                out.push_str(if *not { " IS NOT " } else { " IS " });
                out.push_str(match target {
                    IsTarget::Null => "NULL",
                    IsTarget::True => "TRUE",
                    IsTarget::False => "FALSE",
                    IsTarget::Unknown => "UNKNOWN",
                });
            }
            Self::Subquery(_)
            | Self::Exists { .. }
            | Self::InSubquery { .. }
            | Self::CompareSubquery { .. } => {
                panic!("Format is not implemented for subquery expressions")
            }
            Self::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                out.push_str("CASE");
                if let Some(value) = value {
                    out.push(' ');
                    value.format_into(out);
                }
                for (condition, result) in when_clauses {
                    out.push_str(" WHEN ");
                    condition.format_into(out);
                    out.push_str(" THEN ");
                    result.format_into(out);
                }
                if let Some(expr) = else_clause {
                    out.push_str(" ELSE ");
                    expr.format_into(out);
                }
                out.push_str(" END");
            }
            Self::Cast(cast) => format_cast(cast, out),
            Self::ConvertUsing { .. } => panic!("Format is not implemented for CONVERT USING"),
            Self::Collate { expr, collation } => {
                expr.format_into(out);
                out.push_str(" COLLATE ");
                out.push_str(collation);
            }
            Self::MatchAgainst {
                columns,
                against,
                modifier,
            } => {
                out.push_str("MATCH(");
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    restore_path(column, out);
                }
                out.push_str(") AGAINST(");
                against.format_into(out);
                match modifier {
                    MatchModifier::None => {}
                    MatchModifier::BooleanMode => out.push_str(" IN BOOLEAN MODE"),
                    MatchModifier::QueryExpansion => out.push_str(" WITH QUERY EXPANSION"),
                }
                out.push(')');
            }
            Self::MemberOf { .. } => panic!("Format is not implemented for MEMBER OF"),
        }
    }

    /// Restores this expression to canonical SQL.
    pub fn restore(&self) -> String {
        let mut out = String::new();
        self.restore_into(&mut out);
        out
    }

    /// Restores this expression with Go-compatible formatting flags.
    pub fn restore_with_flags(&self, flags: RestoreFlags) -> String {
        let mut out = String::new();
        self.restore_into_with_context(&mut out, &RestoreContext::new(flags));
        out
    }

    /// Fallible restore for source AST shapes whose validity is checked at
    /// restore time rather than parse time.
    pub fn try_restore(&self) -> Result<String, String> {
        if let Self::Func { name, args, .. } = self {
            if name.eq_ignore_ascii_case("JSON_MEMBEROF") && args.len() != 2 {
                return Err(
                    "Incorrect parameter count in the call to native function 'json_memberof'"
                        .to_string(),
                );
            }
        }
        Ok(self.restore())
    }

    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, &RestoreContext::default());
    }

    /// Restores an expression under the statement's source formatting
    /// context. DDL owns the caller today, but the context lives here because
    /// Go's column-name qualifier flags apply recursively inside expressions,
    /// not only to a statement's outer identifier slots.
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        match self {
            Expr::Column(path) => restore_path_with_context(path, out, context),
            Expr::ParamMarker { .. } => out.push('?'),
            // Integer literals restore as their decimal value, so leading
            // zeros are dropped (`0000` -> `0`, `01` -> `1`).
            Expr::Int(s) => out.push_str(&normalize_int(s)),
            Expr::Decimal(s) => out.push_str(&normalize_decimal(s)),
            Expr::Float(f) => out.push_str(&format_go_float(*f)),
            Expr::Hex(h) => {
                out.push_str("x'");
                out.push_str(h);
                out.push('\'');
            }
            Expr::Bit(b) => {
                out.push_str("b'");
                out.push_str(b);
                out.push('\'');
            }
            Expr::String(v) => {
                if context.flags().has_string_without_charset()
                    || context.flags().has_string_without_default_charset()
                {
                    out.push('\'');
                    out.push_str(&escape_string_literal(v));
                    out.push('\'');
                } else {
                    // Go writes the introducer's charset name through
                    // `ctx.WriteKeyWord` (`pkg/types/parser_driver/value_expr.go`),
                    // so its case follows the keyword flag: `_UTF8MB4` under the
                    // default uppercase flags, and `_utf8mb4` under the lowercase
                    // flags `pkg/ddl/add_column.go` restores a generated column's
                    // expression with -- which is the spelling `SHOW CREATE TABLE`
                    // prints.
                    out.push('_');
                    restore_charset_name(out, "utf8mb4", context);
                    out.push('\'');
                    out.push_str(&escape_string_literal(v));
                    out.push('\'');
                }
            }
            Expr::RawString(v) => {
                out.push('\'');
                out.push_str(&escape_string_literal(v));
                out.push('\'');
            }
            Expr::CharsetString { charset, value } => {
                let omit_charset = context.flags().has_string_without_charset()
                    || (context.flags().has_string_without_default_charset()
                        && charset.eq_ignore_ascii_case("utf8mb4"));
                if !omit_charset {
                    out.push('_');
                    restore_charset_name(out, charset, context);
                }
                out.push('\'');
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
            Expr::CharsetBinary { charset, value } => {
                let omit_charset = context.flags().has_string_without_charset()
                    || (context.flags().has_string_without_default_charset()
                        && charset.eq_ignore_ascii_case("utf8mb4"));
                if !omit_charset {
                    out.push('_');
                    restore_charset_name(out, charset, context);
                    out.push(' ');
                }
                value.restore_into_with_context(out, context);
            }
            Expr::Null => out.push_str("NULL"),
            Expr::Bool(true) => out.push_str("TRUE"),
            Expr::Bool(false) => out.push_str("FALSE"),
            Expr::Default(column) => {
                out.push_str("DEFAULT");
                if let Some(path) = column {
                    out.push('(');
                    restore_path_with_context(path, out, context);
                    out.push(')');
                }
            }
            Expr::UserVar(name) => {
                out.push('@');
                out.push_str(&back_quote(name));
            }
            Expr::SysVar { scope, name } => {
                out.push_str("@@");
                match scope {
                    Some(SysVarScope::Global) => out.push_str("GLOBAL."),
                    Some(SysVarScope::Session) => out.push_str("SESSION."),
                    Some(SysVarScope::Instance) => out.push_str("INSTANCE."),
                    None => {}
                }
                out.push_str(&back_quote(name));
            }
            Expr::Assign { name, value } => {
                out.push('@');
                out.push_str(&back_quote(name));
                out.push_str(":=");
                value.restore_into_with_context(out, context);
            }
            Expr::Unary(op, e) => {
                out.push_str(op.restore());
                e.restore_into_with_context(out, context);
            }
            Expr::Binary(op, l, r) => {
                let bracket = context.flags().has_bracket_around_binary_operation();
                if bracket {
                    out.push('(');
                }
                restore_binary_operand(out, l, context, bracket);
                if context.flags().has_spaces_around_binary_operation() {
                    out.push(' ');
                    out.push_str(op.restore().trim());
                    out.push(' ');
                } else {
                    out.push_str(op.restore());
                }
                restore_binary_operand(out, r, context, bracket);
                if bracket {
                    out.push(')');
                }
            }
            Expr::Paren(e) => {
                out.push('(');
                e.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::Row(values) => {
                out.push_str("ROW(");
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    v.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::Func { name, args, .. } => {
                if name.eq_ignore_ascii_case("JSON_MEMBEROF") && args.len() == 2 {
                    args[0].restore_into_with_context(out, context);
                    out.push_str(" MEMBER OF (");
                    args[1].restore_into_with_context(out, context);
                    out.push(')');
                    return;
                }
                if context.flags().has_keyword_lowercase()
                    || !context.flags().has_keyword_uppercase()
                {
                    out.push_str(&identifier_to_lower(name));
                } else {
                    out.push_str(&name.to_ascii_uppercase());
                }
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::GenericFuncCall {
                schema, name, args, ..
            } => {
                out.push_str(&back_quote(schema));
                out.push('.');
                out.push_str(&back_quote(name));
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::Aggregate {
                name,
                distinct,
                args,
            } => {
                out.push_str(name);
                out.push('(');
                if *distinct {
                    out.push_str("DISTINCT ");
                }
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::GroupConcat {
                distinct,
                args,
                order_by,
                separator,
            } => {
                out.push_str("GROUP_CONCAT(");
                if *distinct {
                    out.push_str("DISTINCT ");
                }
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                if !order_by.is_empty() {
                    out.push_str(" ORDER BY ");
                    for (i, item) in order_by.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        item.restore_into(out);
                    }
                }
                out.push_str(" SEPARATOR '");
                out.push_str(&escape_string_literal(separator));
                out.push_str("')");
            }
            Expr::Window {
                name,
                args,
                distinct,
                ignore_nulls,
                from_last,
                over,
            } => {
                out.push_str(name);
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    } else if *distinct {
                        out.push_str("DISTINCT ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push(')');
                if *from_last {
                    out.push_str(" FROM LAST");
                }
                if *ignore_nulls {
                    out.push_str(" IGNORE NULLS");
                }
                out.push_str(" OVER ");
                match over {
                    // A bare name has NO enclosing parentheses at all —
                    // confirmed via `godump restore` this restores
                    // DIFFERENTLY from `OVER (name)`, even though both are
                    // semantically identical.
                    WindowOver::Name(name) => out.push_str(&back_quote(name)),
                    WindowOver::Def(def) => {
                        out.push('(');
                        restore_window_def(def, out);
                        out.push(')');
                    }
                }
            }
            Expr::Interval { value, unit } => {
                out.push_str("INTERVAL ");
                value.restore_into_with_context(out, context);
                out.push(' ');
                out.push_str(unit);
            }
            Expr::Extract { unit, value } => {
                out.push_str("EXTRACT(");
                out.push_str(unit);
                out.push_str(" FROM ");
                value.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::Position { substr, str } => {
                out.push_str("POSITION(");
                substr.restore_into_with_context(out, context);
                out.push_str(" IN ");
                str.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::WeightString { expr, as_type } => {
                out.push_str("WEIGHT_STRING(");
                expr.restore_into_with_context(out, context);
                if let Some((ty, len)) = as_type {
                    out.push_str(" AS ");
                    out.push_str(match ty {
                        WeightStringType::Char => "CHAR",
                        WeightStringType::Binary => "BINARY",
                    });
                    out.push('(');
                    out.push_str(&len.to_string());
                    out.push(')');
                }
                out.push(')');
            }
            Expr::Trim {
                expr,
                remstr,
                direction,
            } => {
                out.push_str("TRIM(");
                if let Some(d) = direction {
                    out.push_str(match d {
                        TrimDirection::Both => "BOTH ",
                        TrimDirection::Leading => "LEADING ",
                        TrimDirection::Trailing => "TRAILING ",
                    });
                }
                if let Some(r) = remstr {
                    // An explicit `NULL` remstr is OMITTED from restore —
                    // a real, narrow quirk (checked by VALUE, not by
                    // whether the source wrote anything at all) — see
                    // this variant's own doc.
                    if !matches!(r.as_ref(), Expr::Null) {
                        r.restore_into_with_context(out, context);
                        out.push(' ');
                    }
                    out.push_str("FROM ");
                }
                expr.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::TimestampAdd {
                unit,
                interval,
                expr,
            } => {
                out.push_str("TIMESTAMPADD(");
                out.push_str(unit);
                out.push_str(", ");
                interval.restore_into_with_context(out, context);
                out.push_str(", ");
                expr.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::TimestampDiff { unit, expr1, expr2 } => {
                out.push_str("TIMESTAMPDIFF(");
                out.push_str(unit);
                out.push_str(", ");
                expr1.restore_into_with_context(out, context);
                out.push_str(", ");
                expr2.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::GetFormat { selector, expr } => {
                out.push_str("GET_FORMAT(");
                out.push_str(match selector {
                    GetFormatSelector::Date => "DATE",
                    GetFormatSelector::Time => "TIME",
                    GetFormatSelector::Datetime => "DATETIME",
                });
                out.push_str(", ");
                expr.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::In { expr, list, not } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT IN (" } else { " IN (" });
                for (i, e) in list.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    e.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::Between {
                expr,
                low,
                high,
                not,
            } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT BETWEEN " } else { " BETWEEN " });
                low.restore_into_with_context(out, context);
                out.push_str(" AND ");
                high.restore_into_with_context(out, context);
            }
            Expr::Like {
                expr,
                pattern,
                not,
                ilike,
                escape,
            } => {
                expr.restore_into_with_context(out, context);
                out.push_str(match (*not, *ilike) {
                    (true, true) => " NOT ILIKE ",
                    (true, false) => " NOT LIKE ",
                    (false, true) => " ILIKE ",
                    (false, false) => " LIKE ",
                });
                pattern.restore_into_with_context(out, context);
                // `None` also covers an explicit `ESCAPE '\'` matching
                // the default — see this variant's own doc.
                if let Some(esc) = escape {
                    out.push_str(" ESCAPE '");
                    if *esc != 0 {
                        out.push_str(&escape_string_literal(&(*esc as char).to_string()));
                    }
                    out.push('\'');
                }
            }
            Expr::Regexp { expr, pattern, not } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT REGEXP " } else { " REGEXP " });
                pattern.restore_into_with_context(out, context);
            }
            Expr::Is { expr, target, not } => {
                expr.restore_into_with_context(out, context);
                out.push_str(" IS ");
                if *not {
                    out.push_str("NOT ");
                }
                out.push_str(match target {
                    IsTarget::Null => "NULL",
                    IsTarget::True => "TRUE",
                    IsTarget::False => "FALSE",
                    IsTarget::Unknown => "UNKNOWN",
                });
            }
            Expr::Subquery(s) => {
                out.push('(');
                s.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::Exists { subquery, not } => {
                if *not {
                    out.push_str("NOT ");
                }
                out.push_str("EXISTS (");
                subquery.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT IN (" } else { " IN (" });
                subquery.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::CompareSubquery {
                op,
                left,
                all,
                subquery,
            } => {
                left.restore_into_with_context(out, context);
                out.push_str(op.restore());
                out.push_str(if *all { "ALL (" } else { "ANY (" });
                subquery.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                out.push_str("CASE");
                if let Some(v) = value {
                    out.push(' ');
                    v.restore_into_with_context(out, context);
                }
                for (cond, result) in when_clauses {
                    out.push_str(" WHEN ");
                    cond.restore_into_with_context(out, context);
                    out.push_str(" THEN ");
                    result.restore_into_with_context(out, context);
                }
                if let Some(e) = else_clause {
                    out.push_str(" ELSE ");
                    e.restore_into_with_context(out, context);
                }
                out.push_str(" END");
            }
            Expr::Cast(cast) => match cast.style {
                CastStyle::Cast => {
                    out.push_str("CAST(");
                    cast.expr.restore_into_with_context(out, context);
                    out.push_str(" AS ");
                    restore_cast_type(&cast.cast_type, cast.array, out);
                    out.push(')');
                }
                CastStyle::Convert => {
                    out.push_str("CONVERT(");
                    cast.expr.restore_into_with_context(out, context);
                    out.push_str(", ");
                    restore_cast_type(&cast.cast_type, cast.array, out);
                    out.push(')');
                }
                CastStyle::BinaryOperator => {
                    out.push_str("BINARY ");
                    cast.expr.restore_into_with_context(out, context);
                }
                CastStyle::DateLiteral => restore_typed_literal("DATE", &cast.expr, out),
                CastStyle::TimeLiteral => restore_typed_literal("TIME", &cast.expr, out),
                CastStyle::TimestampLiteral => restore_typed_literal("TIMESTAMP", &cast.expr, out),
                CastStyle::JsonSumCrc32 => {
                    out.push_str("JSON_SUM_CRC32(");
                    cast.expr.restore_into_with_context(out, context);
                    out.push_str(" AS ");
                    restore_cast_type(&cast.cast_type, cast.array, out);
                    out.push(')');
                }
            },
            Expr::ConvertUsing { expr, charset } => {
                out.push_str("CONVERT(");
                expr.restore_into_with_context(out, context);
                out.push_str(" USING '");
                out.push_str(&escape_string_literal(charset));
                out.push_str("')");
            }
            Expr::Collate { expr, collation } => {
                expr.restore_into_with_context(out, context);
                out.push_str(" COLLATE ");
                out.push_str(collation);
            }
            Expr::MatchAgainst {
                columns,
                against,
                modifier,
            } => {
                out.push_str("MATCH (");
                for (i, path) in columns.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    restore_path_with_context(path, out, context);
                }
                out.push_str(") AGAINST (");
                against.restore_into_with_context(out, context);
                match modifier {
                    MatchModifier::None => {}
                    MatchModifier::BooleanMode => out.push_str(" IN BOOLEAN MODE"),
                    MatchModifier::QueryExpansion => out.push_str(" WITH QUERY EXPANSION"),
                }
                out.push(')');
            }
            Expr::MemberOf { expr, array } => {
                expr.restore_into_with_context(out, context);
                out.push_str(" MEMBER OF (");
                array.restore_into_with_context(out, context);
                out.push(')');
            }
        }
    }
}
