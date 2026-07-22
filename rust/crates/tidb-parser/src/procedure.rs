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

//! Stored-procedure grammar transcreated from `pkg/parser/procedure_parser.go`.

use tidb_ast::{
    CreateProcedureStmt, DropProcedureStmt, ProcedureDeclaration, ProcedureHandlerAction,
    ProcedureHandlerCondition, ProcedureParameter, ProcedureParameterMode, ProcedureStatement,
    ProcedureWhen,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, prec, PResult, Parser};

impl Parser {
    pub(crate) fn parse_create_procedure(&mut self) -> PResult<CreateProcedureStmt> {
        self.expect_kw("CREATE")?;
        self.expect_kw("PROCEDURE")?;
        let if_not_exists = if self.is_kw("IF") {
            self.bump();
            self.expect_kw("NOT")?;
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_name_path()?;
        self.expect_op("(")?;
        let mut parameters = Vec::new();
        if !self.is_op(")") {
            loop {
                let mode = if self.is_kw("INOUT") {
                    self.bump();
                    ProcedureParameterMode::InOut
                } else if self.is_kw("OUT") {
                    self.bump();
                    ProcedureParameterMode::Out
                } else {
                    if self.is_kw("IN") {
                        self.bump();
                    }
                    ProcedureParameterMode::In
                };
                let name = self.parse_name_or_keyword()?;
                let ty = self.parse_column_type()?;
                parameters.push(ProcedureParameter { mode, name, ty });
                if !self.is_op(",") {
                    break;
                }
                self.bump();
            }
        }
        self.expect_op(")")?;
        let body_start = self.peek().offset;
        let body = self.parse_procedure_statement()?;
        let body_end = if self.at_eof() {
            self.source.len()
        } else {
            self.peek().offset
        };
        let mut body = tidb_ast::NodeBox::new(body);
        if body_end > body_start {
            body.set_text(
                None,
                self.source[body_start..body_end].trim().as_bytes().to_vec(),
            );
        }
        Ok(CreateProcedureStmt {
            if_not_exists,
            name,
            parameters,
            body,
        })
    }

    pub(crate) fn parse_drop_procedure(&mut self) -> PResult<DropProcedureStmt> {
        self.expect_kw("DROP")?;
        self.expect_kw("PROCEDURE")?;
        let if_exists = if self.is_kw("IF") {
            self.bump();
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        Ok(DropProcedureStmt {
            if_exists,
            name: self.parse_name_path()?,
        })
    }

    fn parse_procedure_statement(&mut self) -> PResult<ProcedureStatement> {
        if self.is_kw("BEGIN") {
            return self.parse_procedure_block();
        }
        if self.is_kw("IF") {
            return self.parse_procedure_if();
        }
        if self.is_kw("CASE") {
            return self.parse_procedure_case();
        }
        if self.is_kw("WHILE") {
            return self.parse_procedure_while();
        }
        if self.is_kw("REPEAT") {
            return self.parse_procedure_repeat();
        }
        if self.is_kw("OPEN") {
            self.bump();
            return Ok(ProcedureStatement::OpenCursor(
                self.parse_name_or_keyword()?,
            ));
        }
        if self.is_kw("CLOSE") {
            self.bump();
            return Ok(ProcedureStatement::CloseCursor(
                self.parse_name_or_keyword()?,
            ));
        }
        if self.is_kw("FETCH") {
            self.bump();
            let cursor = self.parse_name_or_keyword()?;
            self.expect_kw("INTO")?;
            let mut variables = vec![self.parse_name_or_keyword()?];
            while self.is_op(",") {
                self.bump();
                variables.push(self.parse_name_or_keyword()?);
            }
            return Ok(ProcedureStatement::FetchInto { cursor, variables });
        }
        if self.is_kw("LEAVE") || self.is_kw("ITERATE") {
            let leave = self.is_kw("LEAVE");
            self.bump();
            return Ok(ProcedureStatement::Jump {
                leave,
                name: self.parse_name_or_keyword()?,
            });
        }
        if self.peek_n(1).text == ":" {
            let name = self.parse_name_or_keyword()?;
            self.expect_op(":")?;
            let statement = Box::new(self.parse_procedure_statement()?);
            if self.peek().kind == TokenKind::Ident || self.peek().kind == TokenKind::Keyword {
                let closing = self.parse_name_or_keyword()?;
                if !closing.eq_ignore_ascii_case(&name) {
                    return Err(self.err_here("procedure label names do not match"));
                }
            }
            return Ok(ProcedureStatement::Label { name, statement });
        }
        Ok(ProcedureStatement::Sql(Box::new(self.parse_statement()?)))
    }

    fn parse_procedure_block(&mut self) -> PResult<ProcedureStatement> {
        self.expect_kw("BEGIN")?;
        let mut declarations = Vec::new();
        while self.is_kw("DECLARE") {
            declarations.push(self.parse_procedure_declaration()?);
            self.expect_op(";")?;
        }
        let statements = self.parse_procedure_list_until(&["END"])?;
        self.expect_kw("END")?;
        Ok(ProcedureStatement::Block {
            declarations,
            statements,
        })
    }

    fn parse_procedure_declaration(&mut self) -> PResult<ProcedureDeclaration> {
        self.expect_kw("DECLARE")?;
        if self.is_kw("CONTINUE") || self.is_kw("EXIT") {
            let action = if self.is_kw("CONTINUE") {
                ProcedureHandlerAction::Continue
            } else {
                ProcedureHandlerAction::Exit
            };
            self.bump();
            self.expect_kw("HANDLER")?;
            self.expect_kw("FOR")?;
            let mut conditions = vec![self.parse_procedure_handler_condition()?];
            while self.is_op(",") {
                self.bump();
                conditions.push(self.parse_procedure_handler_condition()?);
            }
            let body = Box::new(self.parse_procedure_statement()?);
            return Ok(ProcedureDeclaration::Handler {
                action,
                conditions,
                body,
            });
        }

        let first_name = self.parse_name_or_keyword()?;
        if self.is_kw("CURSOR") {
            self.bump();
            self.expect_kw("FOR")?;
            return Ok(ProcedureDeclaration::Cursor {
                name: first_name,
                query: Box::new(self.parse_statement()?),
            });
        }
        let mut names = vec![first_name];
        while self.is_op(",") {
            self.bump();
            names.push(self.parse_name_or_keyword()?);
        }
        let ty = self.parse_column_type()?;
        let default = if self.is_kw("DEFAULT") {
            self.bump();
            Some(Box::new(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ProcedureDeclaration::Variable { names, ty, default })
    }

    fn parse_procedure_handler_condition(&mut self) -> PResult<ProcedureHandlerCondition> {
        if self.peek().kind == TokenKind::IntLit {
            let token = self.bump();
            let code = token
                .text
                .parse::<i64>()
                .map_err(|_| self.err_here("expected handler error code"))?;
            return Ok(ProcedureHandlerCondition::ErrorCode(code));
        }
        if self.is_kw("SQLSTATE") {
            self.bump();
            let token = self.bump();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected SQLSTATE string"));
            }
            return Ok(ProcedureHandlerCondition::SqlState(decode_string(
                &token.text,
            )));
        }
        if self.is_kw("SQLWARNING") {
            self.bump();
            return Ok(ProcedureHandlerCondition::SqlWarning);
        }
        if self.is_kw("NOT") {
            self.bump();
            self.expect_kw("FOUND")?;
            return Ok(ProcedureHandlerCondition::NotFound);
        }
        if self.is_kw("SQLEXCEPTION") {
            self.bump();
            return Ok(ProcedureHandlerCondition::SqlException);
        }
        Err(self.err_here("expected procedure handler condition"))
    }

    fn parse_procedure_if(&mut self) -> PResult<ProcedureStatement> {
        self.expect_kw("IF")?;
        let mut branches = Vec::new();
        loop {
            let condition = self.parse_expr(prec::NONE)?;
            self.expect_kw("THEN")?;
            let statements = self.parse_procedure_list_until(&["ELSEIF", "ELSE", "END"])?;
            branches.push((condition, statements));
            if !self.is_kw("ELSEIF") {
                break;
            }
            self.bump();
        }
        let else_statements = if self.is_kw("ELSE") {
            self.bump();
            self.parse_procedure_list_until(&["END"])?
        } else {
            Vec::new()
        };
        self.expect_kw("END")?;
        self.expect_kw("IF")?;
        Ok(ProcedureStatement::If {
            branches,
            else_statements,
        })
    }

    fn parse_procedure_case(&mut self) -> PResult<ProcedureStatement> {
        self.expect_kw("CASE")?;
        let value = if self.is_kw("WHEN") {
            None
        } else {
            Some(self.parse_expr(prec::NONE)?)
        };
        let mut when = Vec::new();
        while self.is_kw("WHEN") {
            self.bump();
            let expression = self.parse_expr(prec::NONE)?;
            self.expect_kw("THEN")?;
            let statements = self.parse_procedure_list_until(&["WHEN", "ELSE", "END"])?;
            when.push(ProcedureWhen {
                expression,
                statements,
            });
        }
        let else_statements = if self.is_kw("ELSE") {
            self.bump();
            self.parse_procedure_list_until(&["END"])?
        } else {
            Vec::new()
        };
        self.expect_kw("END")?;
        self.expect_kw("CASE")?;
        Ok(if let Some(value) = value {
            ProcedureStatement::SimpleCase {
                value,
                when,
                else_statements,
            }
        } else {
            ProcedureStatement::SearchedCase {
                when,
                else_statements,
            }
        })
    }

    fn parse_procedure_while(&mut self) -> PResult<ProcedureStatement> {
        self.expect_kw("WHILE")?;
        let condition = self.parse_expr(prec::NONE)?;
        self.expect_kw("DO")?;
        let body = self.parse_procedure_list_until(&["END"])?;
        self.expect_kw("END")?;
        self.expect_kw("WHILE")?;
        Ok(ProcedureStatement::While { condition, body })
    }

    fn parse_procedure_repeat(&mut self) -> PResult<ProcedureStatement> {
        self.expect_kw("REPEAT")?;
        let body = self.parse_procedure_list_until(&["UNTIL"])?;
        self.expect_kw("UNTIL")?;
        let condition = self.parse_expr(prec::NONE)?;
        self.expect_kw("END")?;
        self.expect_kw("REPEAT")?;
        Ok(ProcedureStatement::Repeat { body, condition })
    }

    fn parse_procedure_list_until(
        &mut self,
        terminators: &[&str],
    ) -> PResult<Vec<ProcedureStatement>> {
        let mut statements = Vec::new();
        while !terminators.iter().any(|keyword| self.is_kw(keyword)) {
            statements.push(self.parse_procedure_statement()?);
            self.expect_op(";")?;
        }
        Ok(statements)
    }
}
