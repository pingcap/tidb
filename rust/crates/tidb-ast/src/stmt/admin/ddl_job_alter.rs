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

//! Typed payload for Go's `ADMIN ALTER DDL JOBS` parser branch.

use crate::Expr;

/// One ordered option in an `ADMIN ALTER DDL JOBS` request.
///
/// Go keeps the option name as a lower-cased string and accepts a
/// `SignedLiteral` value. Semantic validation of known option names belongs to
/// the DDL job subsystem, not this parser AST.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminAlterDdlJobOption {
    /// Lower-cased option name as stored by Go's hand parser.
    pub name: String,
    /// Literal value, including an optional unary sign.
    pub value: Expr,
}

impl AdminAlterDdlJobOption {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(&self.name);
        out.push_str(" = ");
        self.value.restore_into(out);
    }
}

/// `ADMIN ALTER DDL JOBS job_id option = literal [, ...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminAlterDdlJobsStmt {
    /// DDL job identifier targeted by the request.
    pub job_number: i64,
    /// Options in source order; Go preserves duplicates and ordering.
    pub options: Vec<AdminAlterDdlJobOption>,
}

impl AdminAlterDdlJobsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN ALTER DDL JOBS ");
        out.push_str(&self.job_number.to_string());
        for (index, option) in self.options.iter().enumerate() {
            if index == 0 {
                out.push(' ');
            } else {
                out.push_str(", ");
            }
            option.restore_into(out);
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AdminAlterDdlJobOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, value } = self;
        if !crate::Visitable::accept(value, visitor) {
            return false;
        }
        let _ = name;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminAlterDdlJobsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            job_number,
            options,
        } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = job_number;
        let _ = options;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
