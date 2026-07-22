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

//! `CREATE TABLE` envelope parser.
//!
//! This owns statement ordering. Column definitions, constraints, options,
//! partitioning, and creation-side SPLIT remain shared parser leaves.

use tidb_ast::{
    CreateTableAsQuery, CreateTableOnDuplicate, CreateTableStmt, CreateTableTemporary, QueryStmt,
};

use tidb_lexer::{is_builtin_function_keyword, TokenKind};

use crate::{PResult, Parser};

use super::partition;

#[path = "create/elements.rs"]
mod elements;

impl Parser {
    /// Parses Go's `HandParser.parseCreateTableStmt` statement envelope.
    pub(crate) fn parse_create_table(&mut self) -> PResult<CreateTableStmt> {
        self.expect_kw("CREATE")?;
        let temporary = if self.is_kw("TEMPORARY") {
            self.bump();
            CreateTableTemporary::Local
        } else if self.is_kw("GLOBAL") {
            self.bump();
            self.expect_kw("TEMPORARY")?;
            CreateTableTemporary::Global
        } else {
            CreateTableTemporary::None
        };
        self.expect_kw("TABLE")?;
        let if_not_exists = if self.is_kw("IF") {
            self.bump();
            self.expect_kw("NOT")?;
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_create_table_name_path()?;
        if self.is_kw("LIKE") {
            self.bump();
            let like_table = self.parse_name_path()?;
            let on_commit_delete = self.parse_global_temporary_on_commit(temporary)?;
            let splits = self.parse_create_table_splits()?;
            return Ok(CreateTableStmt {
                temporary,
                on_commit_delete,
                if_not_exists,
                name,
                like_table: Some(like_table),
                columns: Vec::new(),
                table_constraints: Vec::new(),
                table_options: Vec::new(),
                partitioning: None,
                splits,
                ctas: None,
            });
        }
        let (columns, table_constraints) = if self.is_op("(") {
            self.bump();
            let elements = self.parse_table_element_list()?;
            self.expect_op(")")?;
            elements
        } else {
            (Vec::new(), Vec::new())
        };
        let mut table_options = Vec::new();
        while let Some(option) = self.parse_table_option()? {
            table_options.push(option);
            if self.is_op(",") {
                self.bump();
            }
        }
        let partitioning = if self.is_kw("PARTITION") {
            Some(partition::parse_table_partitioning(self)?)
        } else {
            None
        };
        let on_duplicate = if self.is_kw("IGNORE") {
            self.bump();
            CreateTableOnDuplicate::Ignore
        } else if self.is_kw("REPLACE") {
            self.bump();
            CreateTableOnDuplicate::Replace
        } else {
            CreateTableOnDuplicate::Error
        };
        if self.is_kw("AS") {
            self.bump();
        }
        let ctas = self.parse_create_table_result_source(on_duplicate)?;
        let on_commit_delete = self.parse_global_temporary_on_commit(temporary)?;
        let splits = self.parse_create_table_splits()?;
        Ok(CreateTableStmt {
            temporary,
            on_commit_delete,
            if_not_exists,
            name,
            like_table: None,
            columns,
            table_constraints,
            table_options,
            partitioning,
            splits,
            ctas,
        })
    }

    /// Parses the CREATE TABLE name with Go's builtin-function token boundary.
    ///
    /// TiDB's scanner emits `COUNT`/`BIT_AND`/`NOW` as builtin-function
    /// keywords only when the opening `(` is directly adjacent. Go's
    /// `isIdentLike` rejects that token in a table-name slot, while the same
    /// spelling remains valid with intervening whitespace, backticks, or a
    /// qualification. The generic name path deliberately stays broader for
    /// expression and non-CREATE grammar contexts.
    fn parse_create_table_name_path(&mut self) -> PResult<Vec<String>> {
        let mut path = vec![self.parse_create_table_name_segment()?];
        while self.is_op(".") && self.is_name_path_segment(self.peek_n(1)) {
            self.bump();
            path.push(self.parse_create_table_name_segment()?);
        }
        Ok(path)
    }

    fn parse_create_table_name_segment(&mut self) -> PResult<String> {
        let token = self.peek();
        if token.kind == TokenKind::Keyword
            && is_builtin_function_keyword(&token.text)
            && token.end_offset == self.peek_n(1).offset
            && self.is_op_at(1, "(")
        {
            return Err(self.err_here("builtin function cannot be a bare table name"));
        }
        self.parse_name_or_keyword()
    }

    fn is_name_path_segment(&self, token: &tidb_lexer::Token) -> bool {
        token.kind == TokenKind::Ident
            || (token.kind == TokenKind::Keyword && !tidb_lexer::is_reserved(&token.text))
    }

    /// Parses the `ResultSetNode` stored directly on Go's
    /// `CreateTableStmt`, preserving the CTAS source and outer parentheses.
    fn parse_create_table_result_source(
        &mut self,
        on_duplicate: CreateTableOnDuplicate,
    ) -> PResult<Option<CreateTableAsQuery>> {
        let parenthesized = if self.is_op("(") {
            self.bump();
            true
        } else {
            false
        };
        let query = if self.is_kw("WITH") {
            Some(self.parse_with_select()?)
        } else if self.is_kw("SELECT") {
            Some(self.parse_select_or_setopr()?)
        } else if self.is_kw("TABLE") {
            Some(QueryStmt::Select(Box::new(self.parse_table_statement()?)))
        } else if self.is_kw("VALUES") {
            Some(QueryStmt::Select(Box::new(self.parse_values_statement()?)))
        } else {
            None
        };
        if parenthesized {
            if query.is_none() {
                return Err(self.err_here("expected CTAS result source"));
            }
            self.expect_op(")")?;
        }
        Ok(query.map(|query| CreateTableAsQuery {
            on_duplicate,
            query: tidb_ast::NodeBox::new(query),
            parenthesized,
        }))
    }
}
