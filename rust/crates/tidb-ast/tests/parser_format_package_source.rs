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

//! Complete restore-context half of `pkg/parser/format`.

use tidb_ast::{CteRestorer, RestoreContext, RestoreCtx, RestoreFlags};

#[test]
fn original_test_restore_ctx_and_source_priority() {
    let cases = [
        (
            RestoreFlags::from_bits(0),
            "key`.'\"Word\\ str`.'\"ing\\ na`.'\"Me\\",
        ),
        (
            RestoreFlags::STRING_SINGLE_QUOTES,
            "key`.'\"Word\\ 'str`.''\"ing\\' na`.'\"Me\\",
        ),
        (
            RestoreFlags::STRING_DOUBLE_QUOTES,
            "key`.'\"Word\\ \"str`.'\"\"ing\\\" na`.'\"Me\\",
        ),
        (
            RestoreFlags::STRING_ESCAPE_BACKSLASH,
            "key`.'\"Word\\ str`.'\"ing\\\\ na`.'\"Me\\",
        ),
        (
            RestoreFlags::KEYWORD_UPPERCASE,
            "KEY`.'\"WORD\\ str`.'\"ing\\ na`.'\"Me\\",
        ),
        (
            RestoreFlags::KEYWORD_LOWERCASE,
            "key`.'\"word\\ str`.'\"ing\\ na`.'\"Me\\",
        ),
        (
            RestoreFlags::NAME_UPPERCASE,
            "key`.'\"Word\\ str`.'\"ing\\ NA`.'\"ME\\",
        ),
        (
            RestoreFlags::NAME_LOWERCASE,
            "key`.'\"Word\\ str`.'\"ing\\ na`.'\"me\\",
        ),
        (
            RestoreFlags::NAME_DOUBLE_QUOTES,
            "key`.'\"Word\\ str`.'\"ing\\ \"na`.'\"\"Me\\\"",
        ),
        (
            RestoreFlags::NAME_BACK_QUOTES,
            "key`.'\"Word\\ str`.'\"ing\\ `na``.'\"Me\\`",
        ),
        (
            RestoreFlags::DEFAULT,
            "KEY`.'\"WORD\\ 'str`.''\"ing\\' `na``.'\"Me\\`",
        ),
        (
            RestoreFlags::STRING_SINGLE_QUOTES | RestoreFlags::STRING_DOUBLE_QUOTES,
            "key`.'\"Word\\ 'str`.''\"ing\\' na`.'\"Me\\",
        ),
        (
            RestoreFlags::KEYWORD_UPPERCASE | RestoreFlags::KEYWORD_LOWERCASE,
            "KEY`.'\"WORD\\ str`.'\"ing\\ na`.'\"Me\\",
        ),
        (
            RestoreFlags::NAME_UPPERCASE | RestoreFlags::NAME_LOWERCASE,
            "key`.'\"Word\\ str`.'\"ing\\ NA`.'\"ME\\",
        ),
        (
            RestoreFlags::NAME_DOUBLE_QUOTES | RestoreFlags::NAME_BACK_QUOTES,
            "key`.'\"Word\\ str`.'\"ing\\ \"na`.'\"\"Me\\\"",
        ),
    ];

    for (flags, expected) in cases {
        let mut ctx = RestoreCtx::new(flags, String::new());
        ctx.write_keyword("key`.'\"Word\\");
        ctx.write_plain(" ");
        ctx.write_string("str`.'\"ing\\");
        ctx.write_plain(" ");
        ctx.write_name("na`.'\"Me\\");
        assert_eq!(ctx.into_inner(), expected, "flags={:#x}", flags.bits());
    }
}

#[test]
fn all_restore_flags_keep_their_source_bits() {
    let flags = [
        RestoreFlags::STRING_SINGLE_QUOTES,
        RestoreFlags::STRING_DOUBLE_QUOTES,
        RestoreFlags::STRING_ESCAPE_BACKSLASH,
        RestoreFlags::KEYWORD_UPPERCASE,
        RestoreFlags::KEYWORD_LOWERCASE,
        RestoreFlags::NAME_UPPERCASE,
        RestoreFlags::NAME_LOWERCASE,
        RestoreFlags::NAME_DOUBLE_QUOTES,
        RestoreFlags::NAME_BACK_QUOTES,
        RestoreFlags::SPACES_AROUND_BINARY_OPERATION,
        RestoreFlags::BRACKET_AROUND_BINARY_OPERATION,
        RestoreFlags::STRING_WITHOUT_CHARSET,
        RestoreFlags::STRING_WITHOUT_DEFAULT_CHARSET,
        RestoreFlags::TIDB_SPECIAL_COMMENT,
        RestoreFlags::SKIP_PLACEMENT_RULE_FOR_RESTORE,
        RestoreFlags::WITH_TTL_ENABLE_OFF,
        RestoreFlags::WITHOUT_SCHEMA_NAME,
        RestoreFlags::WITHOUT_TABLE_NAME,
        RestoreFlags::FOR_NON_PREP_PLAN_CACHE,
        RestoreFlags::BRACKET_AROUND_BETWEEN_EXPR,
        RestoreFlags::SKIP_REDUNDANT_PARENTHESES,
    ];
    for (bit, flag) in flags.into_iter().enumerate() {
        assert_eq!(flag.bits(), 1_u64 << bit);
    }
    assert_eq!(
        RestoreFlags::DEFAULT.bits(),
        (1_u64 << 0) | (1_u64 << 3) | (1_u64 << 8)
    );
    assert_eq!(RestoreFlags::default().bits(), 0);

    let mut algebra = RestoreFlags::DEFAULT | RestoreFlags::TIDB_SPECIAL_COMMENT;
    algebra &= !RestoreFlags::STRING_SINGLE_QUOTES;
    assert!(!algebra.has_string_single_quotes());
    assert!(algebra.has_keyword_uppercase());
    algebra.remove(RestoreFlags::TIDB_SPECIAL_COMMENT);
    assert!(!algebra.has_tidb_special_comment());
    assert_eq!(
        RestoreFlags::DEFAULT
            .without(RestoreFlags::NAME_BACK_QUOTES)
            .bits(),
        (1_u64 << 0) | (1_u64 << 3)
    );
}

#[test]
fn original_test_restore_special_comment_and_error_semantics() {
    let mut ctx = RestoreCtx::new(RestoreFlags::TIDB_SPECIAL_COMMENT, String::new());
    let result: Result<(), &'static str> = ctx.write_with_special_comments("fea_id", |ctx| {
        ctx.write_plain("content");
        Ok(())
    });
    assert_eq!(result, Ok(()));
    assert_eq!(ctx.writer, "/*T![fea_id] content */");

    ctx.writer.clear();
    let result: Result<(), &'static str> = ctx.write_with_special_comments("", |ctx| {
        ctx.write_plain("shard_row_id_bits");
        Ok(())
    });
    assert_eq!(result, Ok(()));
    assert_eq!(ctx.writer, "/*T! shard_row_id_bits */");

    ctx.writer.clear();
    let error = ctx.write_with_special_comments("", |_ctx| Err("xxxx"));
    assert_eq!(error, Err("xxxx"));
    assert_eq!(ctx.writer, "/*T! ");

    let mut plain = RestoreCtx::new(RestoreFlags::from_bits(0), String::new());
    let error = plain.write_with_special_comments("ignored", |ctx| {
        ctx.write_plain("body");
        Err("same error")
    });
    assert_eq!(error, Err("same error"));
    assert_eq!(plain.writer, "body");
}

#[test]
fn restore_context_fields_and_keyword_helper_match_source_defaults() {
    let mut ctx = RestoreCtx::new(RestoreFlags::DEFAULT, String::new());
    assert_eq!(ctx.default_db, "");
    assert_eq!(ctx.parent_binary_op, 0);
    assert_eq!(ctx.parent_binary_side, 0);
    assert!(!ctx.in_unary_operation);
    ctx.default_db = "test".to_owned();
    ctx.parent_binary_op = 7;
    ctx.parent_binary_side = 2;
    ctx.in_unary_operation = true;
    ctx.write_keyword_with_special_comments("unused", "select");
    assert_eq!(ctx.into_inner(), "SELECT");

    assert_eq!(RestoreContext::default().flags(), RestoreFlags::DEFAULT);
}

#[test]
fn cte_scopes_restore_the_exact_visible_prefix() {
    let mut restorer = CteRestorer::default();
    assert!(!restorer.is_cte_table_name("outer"));
    restorer.record_cte_name("outer");
    {
        let mut outer_scope = restorer.scope();
        outer_scope.record_cte_name("inner");
        outer_scope.record_cte_name("inner");
        assert!(outer_scope.is_cte_table_name("outer"));
        assert!(outer_scope.is_cte_table_name("inner"));
        {
            let mut nested_scope = outer_scope.scope();
            nested_scope.record_cte_name("nested");
            assert!(nested_scope.is_cte_table_name("nested"));
        }
        assert!(!outer_scope.is_cte_table_name("nested"));
        assert_eq!(outer_scope.cte_names, ["outer", "inner", "inner"]);
    }
    assert_eq!(restorer.cte_names, ["outer"]);

    let mut empty = CteRestorer::default();
    {
        let mut scope = empty.scope();
        scope.record_cte_name("temporary");
    }
    assert!(empty.cte_names.is_empty());
}
