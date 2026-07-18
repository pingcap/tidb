# TiDB parser Test Case Map (pkg/parser)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/parser

### Tests
- `pkg/parser/bench_test.go` - parser: Tests sysbench select.
- `pkg/parser/consistent_test.go` - parser: Tests keyword consistent.
- `pkg/parser/digester_test.go` - parser: Tests normalize.
- `pkg/parser/hintparser_test.go` - parser: Tests parse hint.
- `pkg/parser/keywords_test.go` - parser: Tests keywords.
- `pkg/parser/lexer_test.go` - parser: Tests token ID.
- `pkg/parser/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/parser/parser_test.go` - parser: Tests simple.
- `pkg/parser/reserved_words_test.go` - parser: Tests compare reserved words with MySQL.

## pkg/parser/ast

### Tests
- `pkg/parser/ast/base_test.go` - parser/ast: Tests node set text.
- `pkg/parser/ast/ddl_test.go` - parser/ast: Tests DDL visitor cover.
- `pkg/parser/ast/dml_test.go` - parser/ast: Tests DML visitor cover.
- `pkg/parser/ast/expressions_test.go` - parser/ast: Tests expressions visitor cover.
- `pkg/parser/ast/flag_test.go` - parser/ast: Tests has agg flag.
- `pkg/parser/ast/format_test.go` - parser/ast: Tests ast format.
- `pkg/parser/ast/functions_test.go` - parser/ast: Tests functions visitor cover.
- `pkg/parser/ast/misc_test.go` - parser/ast: Tests misc visitor cover.
- `pkg/parser/ast/model_test.go` - parser/ast: Tests model.
- `pkg/parser/ast/procedure_test.go` - parser/ast: Tests procedure visitor cover.
- `pkg/parser/ast/sem_test.go` - parser/ast: Tests show command.
- `pkg/parser/ast/stats_test.go` - parser/ast: Tests refresh stats stmt.
- `pkg/parser/ast/util_test.go` - parser/ast: Tests cacheable.

## pkg/parser/auth

### Tests
- `pkg/parser/auth/caching_sha2_test.go` - parser/auth: Tests check sha password good.
- `pkg/parser/auth/mysql_native_password_test.go` - parser/auth: Tests encode password.
- `pkg/parser/auth/tidb_sm3_test.go` - parser/auth: Tests SM3.

## pkg/parser/charset

### Tests
- `pkg/parser/charset/charset_test.go` - parser/charset: Tests valid charset.
- `pkg/parser/charset/encoding_test.go` - parser/charset: Tests encoding.

## pkg/parser/duration

### Tests
- `pkg/parser/duration/duration_test.go` - parser/duration: Tests parse duration.

## pkg/parser/format

### Tests
- `pkg/parser/format/format_test.go` - parser/format: Tests format.

## pkg/parser/generate_keyword

### Tests
- `pkg/parser/generate_keyword/genkeyword_test.go` - parser/generate_keyword: Tests parse line.

## pkg/parser/mysql

### Tests
- `pkg/parser/mysql/const_test.go` - parser/mysql: Tests SQL mode.
- `pkg/parser/mysql/error_test.go` - parser/mysql: Tests SQL error.
- `pkg/parser/mysql/privs_test.go` - parser/mysql: Tests priv string.
- `pkg/parser/mysql/type_test.go` - parser/mysql: Tests flags.

## pkg/parser/opcode

### Tests
- `pkg/parser/opcode/opcode_test.go` - parser/opcode: Tests opcode.

## pkg/parser/terror

### Tests
- `pkg/parser/terror/terror_test.go` - parser/terror: Tests error code.

## pkg/parser/types

### Tests
- `pkg/parser/types/etc_test.go` - parser/types: Tests str to type.
- `pkg/parser/types/field_type_test.go` - parser/types: Tests field type.
