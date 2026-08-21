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

use super::{ColumnResolver, ConstantFoldMode};
use crate::expression::Expression;
use tidb_datatype::{FieldType, SessionTimeZone};

/// A child-scope resolver carrying Go's expression-rewriter fold counters.
pub(super) struct FoldModeResolver<'a> {
    base: &'a dyn ColumnResolver,
    mode: ConstantFoldMode,
}

impl<'a> FoldModeResolver<'a> {
    pub(super) fn new(base: &'a dyn ColumnResolver, requested: ConstantFoldMode) -> Self {
        let mode = match (base.fold_mode(), requested) {
            (ConstantFoldMode::Disabled, _) | (_, ConstantFoldMode::Disabled) => {
                ConstantFoldMode::Disabled
            }
            (ConstantFoldMode::Try, _) | (_, ConstantFoldMode::Try) => ConstantFoldMode::Try,
            _ => ConstantFoldMode::Normal,
        };
        Self { base, mode }
    }

    pub(super) fn for_function(base: &'a dyn ColumnResolver, name: &str) -> Self {
        let mode = match name {
            "benchmark" => ConstantFoldMode::Disabled,
            "if" | "ifnull" | "coalesce" | "interval" => ConstantFoldMode::Try,
            _ => ConstantFoldMode::Normal,
        };
        Self::new(base, mode)
    }
}

impl ColumnResolver for FoldModeResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        self.base.resolve(path)
    }

    /// Forwarded, not inherited: the default rebuilds a `Column` from
    /// `resolve`'s three fields, which would drop the base resolver's `ID`,
    /// `OrigName`, `IsHidden` and `VirtualExpr` for every column that happens
    /// to sit under a function -- Go's `toColumn` hands back the schema column
    /// itself at every depth. Callers that read `VirtualExpr` off a built
    /// expression (`pkg/ddl/copr`'s `GetCondition`) depend on this.
    fn resolve_column(&self, path: &[String]) -> Option<crate::column::Column> {
        self.base.resolve_column(path)
    }

    fn orig_name(&self, path: &[String]) -> Option<String> {
        self.base.orig_name(path)
    }

    fn resolve_constant(&self, path: &[String]) -> Option<Expression> {
        self.base.resolve_constant(path)
    }

    fn has_resolved_constants(&self) -> bool {
        self.base.has_resolved_constants()
    }

    fn resolve_default(&self, path: &[String]) -> Option<Expression> {
        self.base.resolve_default(path)
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.base.time_zone()
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.base.date_modes()
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.base.connection_charset_info()
    }

    fn tidb_info_len(&self) -> usize {
        self.base.tidb_info_len()
    }

    fn like_default_escape(&self) -> u8 {
        self.base.like_default_escape()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.base.no_unsigned_subtraction()
    }

    fn div_precision_increment(&self) -> u32 {
        self.base.div_precision_increment()
    }

    fn current_database(&self) -> Option<String> {
        self.base.current_database()
    }

    fn fold_mode(&self) -> ConstantFoldMode {
        self.mode
    }

    fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
        self.base.fold_constant(expression, mode);
    }
}
