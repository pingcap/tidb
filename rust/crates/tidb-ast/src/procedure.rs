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

//! Stored-procedure AST transcreated from `pkg/parser/ast/procedure.go`.

use crate::util::{back_quote, escape_string_literal, push_name_path};
use crate::{ColumnType, Expr, Stmt};

/// A stored-procedure parameter direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcedureParameterMode {
    /// Input parameter and the source default when no mode is written.
    In,
    /// Output parameter.
    Out,
    /// Input/output parameter.
    InOut,
}

/// One parameter in a `CREATE PROCEDURE` signature.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcedureParameter {
    /// Parameter direction.
    pub mode: ProcedureParameterMode,
    /// Parameter name.
    pub name: String,
    /// Declared SQL type.
    pub ty: ColumnType,
}

impl ProcedureParameter {
    fn restore_into(&self, out: &mut String) {
        out.push_str(match self.mode {
            ProcedureParameterMode::In => " IN ",
            ProcedureParameterMode::Out => " OUT ",
            ProcedureParameterMode::InOut => " INOUT ",
        });
        out.push_str(&back_quote(&self.name));
        out.push(' ');
        self.ty.restore_compact_into(out);
    }
}

/// A handler operation in a procedure declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcedureHandlerAction {
    /// Continue after the handler body.
    Continue,
    /// Exit the declaring block after the handler body.
    Exit,
}

/// A condition selected by a procedure handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcedureHandlerCondition {
    /// Numeric MySQL error code.
    ErrorCode(i64),
    /// Five-character SQLSTATE value.
    SqlState(String),
    /// `SQLWARNING`.
    SqlWarning,
    /// `NOT FOUND`.
    NotFound,
    /// `SQLEXCEPTION`.
    SqlException,
}

impl ProcedureHandlerCondition {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::ErrorCode(code) => out.push_str(&code.to_string()),
            Self::SqlState(state) => {
                out.push_str("SQLSTATE '");
                out.push_str(&escape_string_literal(state));
                out.push('\'');
            }
            Self::SqlWarning => out.push_str("SQLWARNING"),
            Self::NotFound => out.push_str("NOT FOUND"),
            Self::SqlException => out.push_str("SQLEXCEPTION"),
        }
    }
}

/// A declaration at the start of a procedure block.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcedureDeclaration {
    /// `DECLARE name [, name ...] type [DEFAULT expr]`.
    Variable {
        /// Declared names.
        names: Vec<String>,
        /// Shared SQL type.
        ty: ColumnType,
        /// Optional default expression.
        default: Option<Box<Expr>>,
    },
    /// `DECLARE name CURSOR FOR query`.
    Cursor {
        /// Cursor name.
        name: String,
        /// Cursor query.
        query: Box<Stmt>,
    },
    /// `DECLARE {CONTINUE | EXIT} HANDLER FOR ... statement`.
    Handler {
        /// Handler action.
        action: ProcedureHandlerAction,
        /// Matched conditions.
        conditions: Vec<ProcedureHandlerCondition>,
        /// Handler body.
        body: Box<ProcedureStatement>,
    },
}

impl ProcedureDeclaration {
    fn restore_into(&self, out: &mut String) {
        out.push_str("DECLARE ");
        match self {
            Self::Variable { names, ty, default } => {
                for (index, name) in names.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&back_quote(name));
                }
                out.push(' ');
                ty.restore_compact_into(out);
                if let Some(default) = default {
                    out.push_str(" DEFAULT ");
                    default.restore_into(out);
                }
            }
            Self::Cursor { name, query } => {
                out.push_str(&name.to_ascii_uppercase());
                out.push_str(" CURSOR FOR ");
                query.restore_into(out);
            }
            Self::Handler {
                action,
                conditions,
                body,
            } => {
                out.push_str(match action {
                    ProcedureHandlerAction::Continue => "CONTINUE ",
                    ProcedureHandlerAction::Exit => "EXIT ",
                });
                out.push_str("HANDLER FOR ");
                for (index, condition) in conditions.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    condition.restore_into(out);
                }
                out.push(' ');
                body.restore_into(out);
            }
        }
    }
}

/// One `WHEN expression THEN statements` arm.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcedureWhen {
    /// Arm expression.
    pub expression: Expr,
    /// Arm statements.
    pub statements: Vec<ProcedureStatement>,
}

impl ProcedureWhen {
    fn restore_into(&self, out: &mut String) {
        out.push_str("WHEN ");
        self.expression.restore_into(out);
        out.push_str(" THEN ");
        restore_statement_list(out, &self.statements);
    }
}

/// Stored-procedure body statements.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcedureStatement {
    /// `BEGIN declarations; statements; END`.
    Block {
        /// Declarations, which precede executable statements.
        declarations: Vec<ProcedureDeclaration>,
        /// Executable statements.
        statements: Vec<ProcedureStatement>,
    },
    /// `IF ... THEN ... [ELSEIF ...] [ELSE ...] END IF`.
    If {
        /// `(condition, statements)` branches in source order.
        branches: Vec<(Expr, Vec<ProcedureStatement>)>,
        /// Optional `ELSE` body.
        else_statements: Vec<ProcedureStatement>,
    },
    /// Simple `CASE value WHEN ... END CASE`.
    SimpleCase {
        /// Compared value.
        value: Expr,
        /// `WHEN` arms.
        when: Vec<ProcedureWhen>,
        /// Optional `ELSE` body.
        else_statements: Vec<ProcedureStatement>,
    },
    /// Searched `CASE WHEN predicate ... END CASE`.
    SearchedCase {
        /// Predicate arms.
        when: Vec<ProcedureWhen>,
        /// Optional `ELSE` body.
        else_statements: Vec<ProcedureStatement>,
    },
    /// `REPEAT ... UNTIL condition END REPEAT`.
    Repeat {
        /// Repeated body.
        body: Vec<ProcedureStatement>,
        /// Termination condition.
        condition: Expr,
    },
    /// `WHILE condition DO ... END WHILE`.
    While {
        /// Loop condition.
        condition: Expr,
        /// Loop body.
        body: Vec<ProcedureStatement>,
    },
    /// `OPEN cursor`.
    OpenCursor(String),
    /// `CLOSE cursor`.
    CloseCursor(String),
    /// `FETCH cursor INTO variable [, variable ...]`.
    FetchInto {
        /// Cursor name.
        cursor: String,
        /// Destination variables.
        variables: Vec<String>,
    },
    /// A labeled block or loop. Go canonical restore repeats the opening
    /// label at the end even when it was omitted in source.
    Label {
        /// Label name.
        name: String,
        /// Labeled statement.
        statement: Box<ProcedureStatement>,
    },
    /// `LEAVE` or `ITERATE`.
    Jump {
        /// `true` for `LEAVE`, `false` for `ITERATE`.
        leave: bool,
        /// Target label.
        name: String,
    },
    /// An ordinary SQL statement accepted inside a stored procedure.
    Sql(Box<Stmt>),
}

impl ProcedureStatement {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Block {
                declarations,
                statements,
            } => {
                out.push_str("BEGIN ");
                for declaration in declarations {
                    declaration.restore_into(out);
                    out.push(';');
                }
                restore_statement_list(out, statements);
                out.push_str(" END");
            }
            Self::If {
                branches,
                else_statements,
            } => {
                out.push_str("IF ");
                for (index, (condition, statements)) in branches.iter().enumerate() {
                    if index > 0 {
                        out.push_str("ELSEIF ");
                    }
                    condition.restore_into(out);
                    out.push_str(" THEN ");
                    restore_statement_list(out, statements);
                }
                if !else_statements.is_empty() {
                    out.push_str("ELSE ");
                    restore_statement_list(out, else_statements);
                }
                out.push_str("END IF");
            }
            Self::SimpleCase {
                value,
                when,
                else_statements,
            } => {
                out.push_str("CASE ");
                value.restore_into(out);
                out.push(' ');
                restore_when_and_else(out, when, else_statements);
            }
            Self::SearchedCase {
                when,
                else_statements,
            } => {
                out.push_str("CASE ");
                restore_when_and_else(out, when, else_statements);
            }
            Self::Repeat { body, condition } => {
                out.push_str("REPEAT ");
                restore_statement_list(out, body);
                out.push_str("UNTIL ");
                condition.restore_into(out);
                out.push_str(" END REPEAT");
            }
            Self::While { condition, body } => {
                out.push_str("WHILE ");
                condition.restore_into(out);
                out.push_str(" DO ");
                restore_statement_list(out, body);
                out.push_str("END WHILE");
            }
            Self::OpenCursor(name) => {
                out.push_str("OPEN ");
                out.push_str(&name.to_ascii_uppercase());
            }
            Self::CloseCursor(name) => {
                out.push_str("CLOSE ");
                out.push_str(&name.to_ascii_uppercase());
            }
            Self::FetchInto { cursor, variables } => {
                out.push_str("FETCH ");
                out.push_str(&cursor.to_ascii_uppercase());
                out.push_str(" INTO ");
                for (index, variable) in variables.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&variable.to_ascii_uppercase());
                }
            }
            Self::Label { name, statement } => {
                out.push_str(&back_quote(name));
                out.push_str(": ");
                statement.restore_into(out);
                out.push(' ');
                out.push_str(&back_quote(name));
            }
            Self::Jump { leave, name } => {
                out.push_str(if *leave { "LEAVE " } else { "ITERATE " });
                out.push('\'');
                out.push_str(&escape_string_literal(name));
                out.push('\'');
            }
            Self::Sql(statement) => statement.restore_into(out),
        }
    }
}

fn restore_statement_list(out: &mut String, statements: &[ProcedureStatement]) {
    for statement in statements {
        statement.restore_into(out);
        out.push(';');
    }
}

fn restore_when_and_else(
    out: &mut String,
    when: &[ProcedureWhen],
    else_statements: &[ProcedureStatement],
) {
    for arm in when {
        arm.restore_into(out);
    }
    if !else_statements.is_empty() {
        out.push_str(" ELSE ");
        restore_statement_list(out, else_statements);
    }
    out.push_str(" END CASE");
}

/// `CREATE PROCEDURE`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateProcedureStmt {
    /// Whether `IF NOT EXISTS` was written.
    pub if_not_exists: bool,
    /// Procedure name path.
    pub name: Vec<String>,
    /// Signature parameters.
    pub parameters: Vec<ProcedureParameter>,
    /// Procedure body.
    pub body: crate::NodeBox<ProcedureStatement>,
}

impl CreateProcedureStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE PROCEDURE ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        push_name_path(out, &self.name);
        out.push('(');
        for (index, parameter) in self.parameters.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            parameter.restore_into(out);
        }
        out.push_str(") ");
        self.body.restore_into(out);
    }
}

/// `DROP PROCEDURE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropProcedureStmt {
    /// Whether `IF EXISTS` was written.
    pub if_exists: bool,
    /// Procedure name path.
    pub name: Vec<String>,
}

impl DropProcedureStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP PROCEDURE ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        push_name_path(out, &self.name);
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for ProcedureParameterMode {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::In => {}
            Self::Out => {}
            Self::InOut => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ProcedureParameter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { mode, name, ty } = self;
        if !crate::Visitable::accept(mode, visitor) {
            return false;
        }
        if !crate::Visitable::accept(ty, visitor) {
            return false;
        }
        let _ = mode;
        let _ = name;
        let _ = ty;
        visitor.leave(self)
    }
}

impl crate::Visitable for ProcedureHandlerAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Continue => {}
            Self::Exit => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ProcedureHandlerCondition {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::ErrorCode(field_0) => {
                let _ = field_0;
            }
            Self::SqlState(field_0) => {
                let _ = field_0;
            }
            Self::SqlWarning => {}
            Self::NotFound => {}
            Self::SqlException => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ProcedureDeclaration {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Variable { names, ty, default } => {
                if !crate::Visitable::accept(ty, visitor) {
                    return false;
                }
                if let Some(value) = default.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                let _ = names;
                let _ = ty;
                let _ = default;
            }
            Self::Cursor { name, query } => {
                if !crate::Visitable::accept(query.as_mut(), visitor) {
                    return false;
                }
                let _ = name;
                let _ = query;
            }
            Self::Handler {
                action,
                conditions,
                body,
            } => {
                if !crate::Visitable::accept(action, visitor) {
                    return false;
                }
                for value in conditions.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                if !crate::Visitable::accept(body.as_mut(), visitor) {
                    return false;
                }
                let _ = action;
                let _ = conditions;
                let _ = body;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ProcedureWhen {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            expression,
            statements,
        } = self;
        if !crate::Visitable::accept(expression, visitor) {
            return false;
        }
        for value in statements.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = expression;
        let _ = statements;
        visitor.leave(self)
    }
}

impl crate::Visitable for ProcedureStatement {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Block {
                declarations,
                statements,
            } => {
                for value in declarations.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                for value in statements.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = declarations;
                let _ = statements;
            }
            Self::If {
                branches,
                else_statements,
            } => {
                for value in branches.iter_mut() {
                    if !crate::Visitable::accept(&mut value.0, visitor) {
                        return false;
                    }
                    for value in &mut value.1.iter_mut() {
                        if !crate::Visitable::accept(value, visitor) {
                            return false;
                        }
                    }
                }
                for value in else_statements.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = branches;
                let _ = else_statements;
            }
            Self::SimpleCase {
                value,
                when,
                else_statements,
            } => {
                if !crate::Visitable::accept(value, visitor) {
                    return false;
                }
                for value in when.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                for value in else_statements.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = value;
                let _ = when;
                let _ = else_statements;
            }
            Self::SearchedCase {
                when,
                else_statements,
            } => {
                for value in when.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                for value in else_statements.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = when;
                let _ = else_statements;
            }
            Self::Repeat { body, condition } => {
                for value in body.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                if !crate::Visitable::accept(condition, visitor) {
                    return false;
                }
                let _ = body;
                let _ = condition;
            }
            Self::While { condition, body } => {
                if !crate::Visitable::accept(condition, visitor) {
                    return false;
                }
                for value in body.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = condition;
                let _ = body;
            }
            Self::OpenCursor(field_0) => {
                let _ = field_0;
            }
            Self::CloseCursor(field_0) => {
                let _ = field_0;
            }
            Self::FetchInto { cursor, variables } => {
                let _ = cursor;
                let _ = variables;
            }
            Self::Label { name, statement } => {
                if !crate::Visitable::accept(statement.as_mut(), visitor) {
                    return false;
                }
                let _ = name;
                let _ = statement;
            }
            Self::Jump { leave, name } => {
                let _ = leave;
                let _ = name;
            }
            Self::Sql(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateProcedureStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_not_exists,
            name,
            parameters,
            body,
        } = self;
        for value in parameters.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(body.as_mut(), visitor) {
            return false;
        }
        let _ = if_not_exists;
        let _ = name;
        let _ = parameters;
        let _ = body;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropProcedureStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { if_exists, name } = self;
        let _ = if_exists;
        let _ = name;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
