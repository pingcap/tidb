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

//! Go `format.go`'s restore state: `RestoreFlags`, `RestoreCtx` and
//! `CTERestorer`.

use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt::{self, Write};
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, Deref, DerefMut, Not};

use tidb_mysql::{to_lowercase_cow, to_uppercase_cow};

/// Unicode version used by Go's simple-rune case mapping in this transcreation.
pub const GO_SIMPLE_CASE_UNICODE_VERSION: &str = tidb_mysql::GO_UNICODE_VERSION;

/// Formatting switches used while restoring SQL text.
///
/// Bit positions are part of the source contract. Mutually exclusive groups
/// use source priority: single before double quotes, uppercase before
/// lowercase, and double quotes before back quotes for names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RestoreFlags(u64);

impl RestoreFlags {
    /// Creates a flag set from the source bit representation.
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Quote strings with single quotes.
    pub const STRING_SINGLE_QUOTES: Self = Self(1 << 0);
    /// Quote strings with double quotes.
    pub const STRING_DOUBLE_QUOTES: Self = Self(1 << 1);
    /// Escape string backslashes by doubling them.
    pub const STRING_ESCAPE_BACKSLASH: Self = Self(1 << 2);
    /// Restore keywords in uppercase.
    pub const KEYWORD_UPPERCASE: Self = Self(1 << 3);
    /// Restore keywords in lowercase.
    pub const KEYWORD_LOWERCASE: Self = Self(1 << 4);
    /// Restore names in uppercase.
    pub const NAME_UPPERCASE: Self = Self(1 << 5);
    /// Restore names in lowercase.
    pub const NAME_LOWERCASE: Self = Self(1 << 6);
    /// Quote names with double quotes.
    pub const NAME_DOUBLE_QUOTES: Self = Self(1 << 7);
    /// Quote names with back quotes.
    pub const NAME_BACK_QUOTES: Self = Self(1 << 8);
    /// Add spaces around binary operations.
    pub const SPACES_AROUND_BINARY_OPERATION: Self = Self(1 << 9);
    /// Add brackets around binary operations.
    pub const BRACKET_AROUND_BINARY_OPERATION: Self = Self(1 << 10);
    /// Omit string charset introducers.
    pub const STRING_WITHOUT_CHARSET: Self = Self(1 << 11);
    /// Omit only the default string charset introducer.
    pub const STRING_WITHOUT_DEFAULT_CHARSET: Self = Self(1 << 12);
    /// Wrap TiDB-only syntax in a special comment.
    pub const TIDB_SPECIAL_COMMENT: Self = Self(1 << 13);
    /// Skip placement rules during restore.
    pub const SKIP_PLACEMENT_RULE_FOR_RESTORE: Self = Self(1 << 14);
    /// Force `TTL_ENABLE='OFF'` while restoring a TTL table.
    pub const WITH_TTL_ENABLE_OFF: Self = Self(1 << 15);
    /// Omit schema qualifiers.
    pub const WITHOUT_SCHEMA_NAME: Self = Self(1 << 16);
    /// Omit table qualifiers.
    pub const WITHOUT_TABLE_NAME: Self = Self(1 << 17);
    /// Restore for the non-prepared plan cache.
    pub const FOR_NON_PREP_PLAN_CACHE: Self = Self(1 << 18);
    /// Add brackets around a `BETWEEN` expression.
    pub const BRACKET_AROUND_BETWEEN_EXPR: Self = Self(1 << 19);
    /// Omit redundant expression parentheses.
    pub const SKIP_REDUNDANT_PARENTHESES: Self = Self(1 << 20);
    /// The source default: single-quoted strings, uppercase keywords, and
    /// back-quoted names.
    pub const DEFAULT: Self =
        Self(Self::STRING_SINGLE_QUOTES.0 | Self::KEYWORD_UPPERCASE.0 | Self::NAME_BACK_QUOTES.0);

    /// Returns the raw source bit set.
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Returns whether every bit in `flag` is enabled.
    pub const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }

    /// Removes every bit present in `flag`.
    pub fn remove(&mut self, flag: Self) {
        self.0 &= !flag.0;
    }

    /// Returns this flag set without every bit present in `flag`.
    pub const fn without(self, flag: Self) -> Self {
        Self(self.0 & !flag.0)
    }

    /// Returns whether schema qualifiers are omitted.
    pub const fn has_without_schema_name(self) -> bool {
        self.contains(Self::WITHOUT_SCHEMA_NAME)
    }

    /// Returns whether table qualifiers are omitted.
    pub const fn has_without_table_name(self) -> bool {
        self.contains(Self::WITHOUT_TABLE_NAME)
    }

    /// Returns whether strings use single quotes.
    pub const fn has_string_single_quotes(self) -> bool {
        self.contains(Self::STRING_SINGLE_QUOTES)
    }

    /// Returns whether strings use double quotes.
    pub const fn has_string_double_quotes(self) -> bool {
        self.contains(Self::STRING_DOUBLE_QUOTES)
    }

    /// Returns whether string backslashes are escaped.
    pub const fn has_string_escape_backslash(self) -> bool {
        self.contains(Self::STRING_ESCAPE_BACKSLASH)
    }

    /// Returns whether keywords use uppercase.
    pub const fn has_keyword_uppercase(self) -> bool {
        self.contains(Self::KEYWORD_UPPERCASE)
    }

    /// Returns whether keywords use lowercase.
    pub const fn has_keyword_lowercase(self) -> bool {
        self.contains(Self::KEYWORD_LOWERCASE)
    }

    /// Returns whether names use uppercase.
    pub const fn has_name_uppercase(self) -> bool {
        self.contains(Self::NAME_UPPERCASE)
    }

    /// Returns whether names use lowercase.
    pub const fn has_name_lowercase(self) -> bool {
        self.contains(Self::NAME_LOWERCASE)
    }

    /// Returns whether names use double quotes.
    pub const fn has_name_double_quotes(self) -> bool {
        self.contains(Self::NAME_DOUBLE_QUOTES)
    }

    /// Returns whether names use back quotes.
    pub const fn has_name_back_quotes(self) -> bool {
        self.contains(Self::NAME_BACK_QUOTES)
    }

    /// Returns whether binary operations use surrounding spaces.
    pub const fn has_spaces_around_binary_operation(self) -> bool {
        self.contains(Self::SPACES_AROUND_BINARY_OPERATION)
    }

    /// Returns whether binary operations use brackets.
    pub const fn has_bracket_around_binary_operation(self) -> bool {
        self.contains(Self::BRACKET_AROUND_BINARY_OPERATION)
    }

    /// Returns whether the default string charset is omitted.
    pub const fn has_string_without_default_charset(self) -> bool {
        self.contains(Self::STRING_WITHOUT_DEFAULT_CHARSET)
    }

    /// Returns whether `BETWEEN` expressions use brackets.
    pub const fn has_bracket_around_between_expr(self) -> bool {
        self.contains(Self::BRACKET_AROUND_BETWEEN_EXPR)
    }

    /// Returns whether redundant expression parentheses are omitted.
    pub const fn has_skip_redundant_parentheses(self) -> bool {
        self.contains(Self::SKIP_REDUNDANT_PARENTHESES)
    }

    /// Returns whether all string charset introducers are omitted.
    pub const fn has_string_without_charset(self) -> bool {
        self.contains(Self::STRING_WITHOUT_CHARSET)
    }

    /// Returns whether TiDB special comments are enabled.
    pub const fn has_tidb_special_comment(self) -> bool {
        self.contains(Self::TIDB_SPECIAL_COMMENT)
    }

    /// Returns whether placement rules are skipped.
    pub const fn has_skip_placement_rule_for_restore(self) -> bool {
        self.contains(Self::SKIP_PLACEMENT_RULE_FOR_RESTORE)
    }

    /// Returns whether TTL restore forces `TTL_ENABLE='OFF'`.
    pub const fn has_with_ttl_enable_off(self) -> bool {
        self.contains(Self::WITH_TTL_ENABLE_OFF)
    }

    /// Returns whether non-prepared plan-cache restore is enabled.
    pub const fn has_for_non_prep_plan_cache(self) -> bool {
        self.contains(Self::FOR_NON_PREP_PLAN_CACHE)
    }
}

impl BitAnd for RestoreFlags {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl BitAndAssign for RestoreFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0;
    }
}

impl BitOr for RestoreFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for RestoreFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl Not for RestoreFlags {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(!self.0)
    }
}

/// A writer that accepts restored string fragments.
pub trait RestoreWriter: Write {}

impl<T: Write + ?Sized> RestoreWriter for T {}

/// CTE-name state shared while restoring nested `WITH` clauses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CteRestorer {
    /// Lowercase CTE names visible in the current restore scope.
    pub cte_names: Vec<String>,
}

impl CteRestorer {
    /// Returns whether `name_lowercase` is a visible CTE name.
    pub fn is_cte_table_name(&self, name_lowercase: &str) -> bool {
        self.cte_names.iter().any(|name| name == name_lowercase)
    }

    /// Records one lowercase CTE name in the current scope.
    pub fn record_cte_name(&mut self, name_lowercase: impl Into<String>) {
        self.cte_names.push(name_lowercase.into());
    }

    /// Opens a nested CTE scope. Dropping the returned guard restores exactly
    /// the name count visible before the scope was opened.
    pub fn scope(&mut self) -> CteScope<'_> {
        let retained = self.cte_names.len();
        CteScope {
            restorer: self,
            retained,
        }
    }
}

/// RAII guard implementing the source `RestoreCTEFunc` scope contract.
pub struct CteScope<'a> {
    restorer: &'a mut CteRestorer,
    retained: usize,
}

impl Deref for CteScope<'_> {
    type Target = CteRestorer;

    fn deref(&self) -> &Self::Target {
        self.restorer
    }
}

impl DerefMut for CteScope<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.restorer
    }
}

impl Drop for CteScope<'_> {
    fn drop(&mut self) {
        self.restorer.cte_names.truncate(self.retained);
    }
}

/// Writer-bound restore context matching the source `RestoreCtx` fields.
pub struct RestoreCtx<W> {
    /// Active source restore flags.
    pub flags: RestoreFlags,
    /// Destination for restored fragments.
    pub writer: W,
    /// Default database used by restore callers.
    pub default_db: String,
    /// Parent binary opcode; zero means no parent.
    pub parent_binary_op: i32,
    /// Side of the parent binary operation.
    pub parent_binary_side: i32,
    /// Whether the current expression is a unary operand.
    pub in_unary_operation: bool,
    /// CTE names visible to the current restore operation.
    pub cte_restorer: CteRestorer,
}

impl<W: RestoreWriter> RestoreCtx<W> {
    /// Creates a writer-bound restore context.
    pub fn new(flags: RestoreFlags, writer: W) -> Self {
        Self {
            flags,
            writer,
            default_db: String::new(),
            parent_binary_op: 0,
            parent_binary_side: 0,
            in_unary_operation: false,
            cte_restorer: CteRestorer::default(),
        }
    }

    /// Consumes the context and returns its writer.
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// Writes a keyword using source uppercase/lowercase priority.
    pub fn write_keyword(&mut self, keyword: &str) {
        let _ = if self.flags.has_keyword_uppercase() {
            self.writer.write_str(&to_uppercase_cow(keyword))
        } else if self.flags.has_keyword_lowercase() {
            self.writer.write_str(&to_lowercase_cow(keyword))
        } else {
            self.writer.write_str(keyword)
        };
    }

    /// Writes a special-comment envelope when enabled. If `write` returns an
    /// error, the exact error is returned and the closing delimiter is omitted.
    pub fn write_with_special_comments<E>(
        &mut self,
        feature_id: &str,
        write: impl FnOnce(&mut Self) -> Result<(), E>,
    ) -> Result<(), E> {
        if !self.flags.has_tidb_special_comment() {
            return write(self);
        }
        self.write_plain("/*T!");
        if !feature_id.is_empty() {
            self.write_plain_fmt(format_args!("[{feature_id}]"));
        }
        self.write_plain(" ");
        write(self)?;
        self.write_plain(" */");
        Ok(())
    }

    /// Writes a keyword, optionally wrapped in a special comment.
    pub fn write_keyword_with_special_comments(&mut self, feature_id: &str, keyword: &str) {
        let _: Result<(), Infallible> = self.write_with_special_comments(feature_id, |ctx| {
            ctx.write_keyword(keyword);
            Ok(())
        });
    }

    /// Writes a string using source backslash and quote priority.
    pub fn write_string(&mut self, value: &str) {
        let escape_backslash = self.flags.has_string_escape_backslash();
        let quote = if self.flags.has_string_single_quotes() {
            "'"
        } else if self.flags.has_string_double_quotes() {
            "\""
        } else {
            ""
        };
        if let (true, [quote_byte]) = (escape_backslash, quote.as_bytes()) {
            let mut matches = memchr::memchr2_iter(b'\\', *quote_byte, value.as_bytes());
            match matches.next() {
                None => return self.write_quoted(quote, value),
                Some(first) if value.len() <= 256 => {
                    return self.write_quoted(quote, &double_at_positions(value, first, matches));
                }
                Some(_) => {}
            }
        }
        let mut value = Cow::Borrowed(value);
        if escape_backslash && value.as_bytes().contains(&b'\\') {
            value = Cow::Owned(value.replace('\\', "\\\\"));
        }
        if self.flags.has_string_single_quotes() {
            if value.as_bytes().contains(&b'\'') {
                value = Cow::Owned(value.replace('\'', "''"));
            }
        } else if self.flags.has_string_double_quotes() && value.as_bytes().contains(&b'"') {
            value = Cow::Owned(value.replace('"', "\"\""));
        }
        self.write_quoted(quote, &value);
    }

    fn write_quoted(&mut self, quote: &str, value: &str) {
        let _ = self.writer.write_str(quote);
        let _ = self.writer.write_str(value);
        let _ = self.writer.write_str(quote);
    }

    /// Writes a name using source case and quote priority.
    pub fn write_name(&mut self, name: &str) {
        let name = if self.flags.has_name_uppercase() {
            to_uppercase_cow(name)
        } else if self.flags.has_name_lowercase() {
            to_lowercase_cow(name)
        } else {
            Cow::Borrowed(name)
        };
        let (quote, doubled) = if self.flags.has_name_double_quotes() {
            ("\"", "\"\"")
        } else if self.flags.has_name_back_quotes() {
            ("`", "``")
        } else {
            ("", "")
        };
        let name = match quote.as_bytes().first() {
            Some(&byte) => double_quotes(&name, byte, doubled),
            None => Cow::Borrowed(name.as_ref()),
        };
        self.write_quoted(quote, &name);
    }

    /// Writes text without transformation.
    pub fn write_plain(&mut self, text: &str) {
        let _ = self.writer.write_str(text);
    }

    /// Writes preformatted text without transformation.
    pub fn write_plain_fmt(&mut self, arguments: fmt::Arguments<'_>) {
        let _ = self.writer.write_fmt(arguments);
    }
}

fn double_quotes<'a>(value: &'a str, quote: u8, doubled: &str) -> Cow<'a, str> {
    let mut matches = memchr::memchr_iter(quote, value.as_bytes());
    let Some(first) = matches.next() else {
        return Cow::Borrowed(value);
    };
    if value.len() > 32 {
        return Cow::Owned(value.replace(char::from(quote), doubled));
    }
    double_at_positions(value, first, matches)
}

fn double_at_positions(
    value: &str,
    first: usize,
    matches: impl Iterator<Item = usize>,
) -> Cow<'_, str> {
    let mut result = String::with_capacity(2 * value.len());
    let mut start = 0;
    for index in std::iter::once(first).chain(matches) {
        result.push_str(&value[start..=index]);
        start = index;
    }
    result.push_str(&value[start..]);
    Cow::Owned(result)
}
