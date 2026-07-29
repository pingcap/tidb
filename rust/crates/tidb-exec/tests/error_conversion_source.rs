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

#![allow(missing_docs)]

use tidb_exec::{
    exec_error_descriptor, exec_error_kind, ExecError, RenderedExecError, StatementKind,
    StatementStatus,
};
use tidb_expr::EvalError;
use tidb_protocol::{ErrorDescriptor, ErrorKind};

#[test]
fn source_errno_categories_are_structural_and_message_is_unchanged() {
    // Source: pkg/parser/parser_api.go:30-31 (ErrParse),
    // pkg/planner/core/planbuilder.go:4265-4287 (ErrUnknownColumn),
    // pkg/planner/core/logical_plan_builder.go:5499 (ErrNoSuchTable),
    // pkg/executor/write.go:454-458 and pkg/executor/insert_common.go:302-321
    // (ErrDataTooLong/ErrWarnDataOutOfRange), and
    // pkg/parser/mysql/{errcode,state}.go (code/state pairs).
    let cases = [
        (
            ExecError::Parse {
                message: "parser detail".to_owned(),
                offset: 4,
            },
            ErrorKind::Parse,
        ),
        (
            ExecError::UnknownTable("app.t".to_owned()),
            ErrorKind::UnknownTable,
        ),
        (
            ExecError::UnknownColumn("missing".to_owned()),
            ErrorKind::UnknownColumn,
        ),
        (
            ExecError::DuplicateIndex("idx".to_owned()),
            ErrorKind::DuplicateIndex,
        ),
        (ExecError::DuplicateKey, ErrorKind::DuplicateKey),
        (
            ExecError::ColumnCountMismatch,
            ErrorKind::ColumnCountMismatch,
        ),
        (
            ExecError::DataTooLong("name".to_owned()),
            ErrorKind::DataTooLong,
        ),
        (
            ExecError::OutOfRange("amount".to_owned()),
            ErrorKind::OutOfRange,
        ),
    ];

    for (error, expected_kind) in cases {
        let descriptor = exec_error_descriptor(&error, [0xff, 0x00, b'\x80', b'!']);
        assert_eq!(exec_error_kind(&error), expected_kind);
        assert_eq!(descriptor.kind, expected_kind);
        assert_eq!(descriptor.message, [0xff, 0x00, b'\x80', b'!']);
    }
}

#[test]
fn broad_categories_do_not_guess_not_supported_or_foreign_key_errno() {
    // Source: pkg/server/conn.go:1740-1742 falls back to ErrUnknown for an
    // error without a terror.Error.  The Rust executor categories below do
    // not carry enough source class/context to claim a narrower errno.
    let cases = [
        (ExecError::NotSelect, ErrorKind::NotSelect),
        (ExecError::RequiresTable, ErrorKind::RequiresTable),
        (ExecError::Wildcard, ErrorKind::Wildcard),
        (
            ExecError::Unsupported("MATCH AGAINST"),
            ErrorKind::Unsupported,
        ),
        (
            ExecError::Protocol("bad packet".to_owned()),
            ErrorKind::Protocol,
        ),
        (
            ExecError::UnknownSavepoint("sp".to_owned()),
            ErrorKind::UnknownSavepoint,
        ),
        (ExecError::WriteConflict, ErrorKind::WriteConflict),
        (
            ExecError::UngroupedColumn("a".to_owned()),
            ErrorKind::UngroupedColumn,
        ),
        (
            ExecError::ForeignKeyViolation,
            ErrorKind::ForeignKeyViolation,
        ),
        (ExecError::Eval(EvalError::IntOverflow), ErrorKind::Eval),
    ];

    for (error, expected_kind) in cases {
        let descriptor = exec_error_descriptor(&error, b"source-rendered context");
        assert_eq!(
            descriptor,
            ErrorDescriptor::new(expected_kind, b"source-rendered context")
        );
    }
}

#[test]
fn parser_offset_and_variant_payloads_stay_with_executor_context() {
    // `ErrorDescriptor` intentionally carries only the rendered message and
    // category.  The parser offset and table/column names remain available to
    // the executor/session that rendered the message; this adapter must not
    // replace them with a guessed format.
    let error = ExecError::Parse {
        message: "near 'FROM'".to_owned(),
        offset: 17,
    };
    let descriptor = exec_error_descriptor(&error, b"syntax error near FROM at byte 17");
    assert_eq!(descriptor.kind, ErrorKind::Parse);
    assert_eq!(descriptor.message, b"syntax error near FROM at byte 17");

    let error = ExecError::UnknownTable("app.orders".to_owned());
    let descriptor = exec_error_descriptor(&error, b"Table 'app.orders' doesn't exist");
    assert_eq!(descriptor.kind, ErrorKind::UnknownTable);
    assert_eq!(descriptor.message, b"Table 'app.orders' doesn't exist");
}

#[test]
fn rendered_error_attaches_only_published_statement_context() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:792-809 (error handling),
    // 1129-1170 (message/warning publication), and
    // pkg/server/conn.go:1725-1768 (writeError consumes the rendered error).
    // A parse/transport failure may have no published status; it must remain
    // explicitly absent instead of receiving a guessed zero-valued context.
    let parse = ExecError::Parse {
        message: "near SELECT".to_owned(),
        offset: 2,
    };
    let detached = RenderedExecError::new(&parse, [0xff, 0x00, 0x80]);
    assert_eq!(detached.status(), None);
    assert_eq!(detached.descriptor().message, [0xff, 0x00, 0x80]);

    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.add_affected_rows(3);
    status.warn("source warning");
    status.set_message("Rows matched: 3");
    let published = status.finish_statement();

    let attached = RenderedExecError::with_status(
        &ExecError::UnknownColumn("secret".to_owned()),
        [b'c', 0xff, b't'],
        &published,
    );
    assert_eq!(attached.descriptor().kind, ErrorKind::UnknownColumn);
    assert_eq!(attached.descriptor().message, [b'c', 0xff, b't']);
    assert_eq!(attached.status(), Some(&published));

    let replaced = detached.attach_status(&published);
    assert_eq!(replaced.status(), Some(&published));
    assert_eq!(replaced.descriptor().message, [0xff, 0x00, 0x80]);
}
