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

//! Source-first ports of `pkg/expression.part5`'s vectorized regexp harnesses
//! (`builtin_regexp_test.go::TestRegexpLikeVec/TestRegexpSubstrVec/
//! TestRegexpInStrVec/TestRegexpReplaceVec`),
//! `builtin_regexp_vec_const_test.go::TestVectorizedBuiltinRegexpForConstants`,
//! and the memoization contract `builtin_regexp_test.go::TestRegexpCache`
//! pins on Go's side. The scalar value tables themselves were ported earlier
//! (`crate::builtin_ext::regexp::tests`, `tests::regexp_like`); this module
//! re-derives the HARNESS dimensions those Go tests add on top — their exact
//! generator arrays swept as cross-tier agreements, plus the constant-pattern
//! corpus invariant.

use super::*;

/// Renders a SQL single-quoted literal the way MySQL tokenizes one: a doubled
/// quote inside, every backslash escaped, newlines passed through raw (MySQL
/// string literals may span lines).
fn sql_literal(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "''");
    format!("'{escaped}'")
}

/// GO PORT of `pkg/expression/builtin_regexp_test.go:298 TestRegexpLikeVec`.
///
/// The Go harness sweeps EXACT generator arrays over REGEXP_LIKE —
/// `expr × pattern × matchType` plus constant/null-operand arm variants — and
/// requires each vectorized row to equal its scalar evaluation. This tier
/// keeps ONE evaluator per expression shape, so the harness invariant becomes
/// cross-tier agreement: the AST/value tier and the rewritten chunk tier must
/// answer identically across the complete sweep (96 combos), errors included
/// (`icc` stays legal — the rightmost rule makes the trailing flag win).
#[test]
fn regexp_like_vec_generator_matrix_agrees_across_tiers() {
    let exprs = ["abc", "aBc", "Good\nday", "\n"];
    let patterns = ["abc", "od$", "^day", "day$", "."];
    let match_types = ["m", "i", "icc", "cii", "s", "msi"];
    for text in exprs {
        for pattern in patterns {
            // Without-match-type arm.
            let plain = format!(
                "regexp_like({}, {})",
                sql_literal(text),
                sql_literal(pattern)
            );
            assert_eq!(e(&plain), chunk_e(&plain), "{plain}");
            for match_type in match_types {
                let sql = format!(
                    "regexp_like({}, {}, {})",
                    sql_literal(text),
                    sql_literal(pattern),
                    sql_literal(match_type)
                );
                assert_eq!(e(&sql), chunk_e(&sql), "{sql}");
            }
        }
    }
    // Null-expr/pattern/matchType arms stand-ins stay NULL everywhere.
    for null_arm in [
        "regexp_like(NULL, 'abc', 'm')",
        "regexp_like('abc', NULL, 'm')",
        "regexp_like('abc', 'abc', NULL)",
    ] {
        assert_eq!(chunk_e(null_arm), "NULL", "{null_arm}");
    }
}

/// GO PORT of `pkg/expression/builtin_regexp_test.go:545 TestRegexpSubstrVec`.
///
/// Sweep of Go's exact generator arrays:
/// `expr[5] x pattern[10] x position{1,5} x occurrence{-1,10} x matchType[6]`.
/// Positions can legitimately fall out of range ("position 5" against
/// one-character inputs); both tiers share the source's `ErrRegexp`
/// index-out-of-bounds behavior there, and the agreement assertion covers that
/// instead of duplicating it.
#[test]
fn regexp_substr_vec_generator_matrix_agrees_across_tiers() {
    let exprs = [
        "abc abd abe",
        "你好啊啊啊啊啊",
        "好的 好滴 好~",
        "Good\nday",
        "\n\n\n\n\n\n",
    ];
    let patterns = [
        "^$", "ab.", "aB.", "abc", "好", "好.", "od$", "^day", "day$", ".",
    ];
    let positions = [1_i64, 5];
    let occurrences = [-1_i64, 10];
    let match_types = ["m", "i", "icc", "cii", "s", "msi"];
    for text in exprs {
        for pattern in patterns {
            for position in positions {
                for occurrence in occurrences {
                    for match_type in match_types {
                        let sql = format!(
                            "regexp_substr({}, {}, {}, {}, {})",
                            sql_literal(text),
                            sql_literal(pattern),
                            position,
                            occurrence,
                            sql_literal(match_type)
                        );
                        assert_eq!(e(&sql), chunk_e(&sql), "{sql}");
                    }
                }
            }
        }
    }
}

/// GO PORT of `pkg/expression/builtin_regexp_test.go:849 TestRegexpInStrVec`.
///
/// Same harness shape as SUBSTR with return-option `{0, 1}` inserted:
/// `expr[5] x pattern[10] x position{1,5} x occurrence{-1,10} x
/// returnOption{0,1} x matchType[6]`, asserted as exhaustive tier agreement.
#[test]
fn regexp_instr_vec_generator_matrix_agrees_across_tiers() {
    let exprs = [
        "abc abd abe",
        "你好啊啊啊啊啊",
        "好的 好滴 好~",
        "Good\nday",
        "\n\n\n\n\n\n",
    ];
    let patterns = [
        "^$", "ab.", "aB.", "abc", "好", "好.", "od$", "^day", "day$", ".",
    ];
    let positions = [1_i64, 5];
    let occurrences = [-1_i64, 10];
    let return_options = [0_i64, 1];
    let match_types = ["m", "i", "icc", "cii", "s", "msi"];
    for text in exprs {
        for pattern in patterns {
            for position in positions {
                for occurrence in occurrences {
                    for return_option in return_options {
                        for match_type in match_types {
                            let sql = format!(
                                "regexp_instr({}, {}, {}, {}, {}, {})",
                                sql_literal(text),
                                sql_literal(pattern),
                                position,
                                occurrence,
                                return_option,
                                sql_literal(match_type)
                            );
                            assert_eq!(e(&sql), chunk_e(&sql), "{sql}");
                        }
                    }
                }
            }
        }
    }
}

/// GO PORT of `pkg/expression/builtin_regexp_test.go:1154
/// TestRegexpReplaceVec`: `expr[5] x pattern[12] x replacement[3] (including
/// the capture reference `a\12`) x position{1,5} x occurrence{-1,5} x
/// matchType[6]`. Capture references past the group count surface the
/// signature's own `ErrRegexp` substitution error; the sweep asserts both
/// tiers agree row for row.
#[test]
fn regexp_replace_vec_generator_matrix_agrees_across_tiers() {
    let exprs = [
        "abc abd abe",
        "你好啊啊啊啊啊",
        "好的 好滴 好~",
        "Good\nday",
        "seafood fool",
    ];
    let patterns = [
        "(^$)", "(a)b.", "a(B).", "(ab)c", "(好)", "(好).", "(o)d$", "^da(y)", "(d)ay$", "(.)",
        "foo(.?)", "foo(d|l)",
    ];
    let replacements = ["cc", "的", "a\\12"];
    let positions = [1_i64, 5];
    let occurrences = [-1_i64, 5];
    let match_types = ["m", "i", "icc", "cii", "s", "msi"];
    for text in exprs {
        for pattern in patterns {
            for replacement in replacements {
                for position in positions {
                    for occurrence in occurrences {
                        for match_type in match_types {
                            let sql = format!(
                                "regexp_replace({}, {}, {}, {}, {}, {})",
                                sql_literal(text),
                                sql_literal(pattern),
                                sql_literal(replacement),
                                position,
                                occurrence,
                                sql_literal(match_type)
                            );
                            assert_eq!(e(&sql), chunk_e(&sql), "{sql}");
                        }
                    }
                }
            }
        }
    }
}

/// GO PORT of
/// `pkg/expression/builtin_regexp_vec_const_test.go:63
/// TestVectorizedBuiltinRegexpForConstants`.
///
/// Go fills 1024 random rows (length 10–20) of an ETString column against one
/// CONSTANT pattern `\A[A-Za-z]{3,5}\d{1,5}[[:alpha:]]*\z` and requires the
/// batch result to equal the per-row scalar evaluation exactly (same non-null
/// mask, same ints). A deterministic corpus standing in for the generator is
/// driven through the production chunk tier and compared against the shared
/// scalar matcher — including mismatching, digit-heavy, and boundary-length
/// rows so both outcomes are exercised.
#[test]
fn regexp_constant_pattern_corpus_matches_scalar_evaluation() {
    let pattern = r"\A[A-Za-z]{3,5}\d{1,5}[[:alpha:]]*\z";
    let corpus = [
        "Abcde12345ghijk",   // full match: 5 letters, 5 digits, tail letters
        "abc1z",             // minimum-ish lengths
        "ABCDE99999WXYZ ab", // trailing space breaks the \z anchor
        "abcdefghij",        // no digits
        "abc12",             // ends after digits ([[:alpha:]]* accepts empty)
        "12345abcde",        // starts with digits: anchor fails
        "Abc-123defgh",      // dash outside the classes
        "Abcde123456f",      // six digits exceeds {1,5}
        "aB3ZZZzzzz",        // mixed case everywhere
    ];
    for text in corpus {
        let sql = format!(
            "regexp_like({}, {})",
            sql_literal(text),
            sql_literal(pattern)
        );
        assert_eq!(e(&sql), chunk_e(&sql), "{sql}");
    }
    // Anchor behavior spot checks so the corpus cannot hide a dropped anchor.
    assert_eq!(
        chunk_e(&format!(
            "regexp_like({},{})",
            sql_literal("abc12"),
            sql_literal(pattern)
        )),
        "INT:1"
    );
    assert_eq!(
        chunk_e(&format!(
            "regexp_like({},{})",
            sql_literal(" abc12"),
            sql_literal(pattern)
        )),
        "INT:0"
    );
    // POSIX classes are honored, not matched literally.
    assert_eq!(
        chunk_e("regexp_like('abc99x', '[[:alpha:]]+[[:digit:]]+[[:alpha:]]')"),
        "INT:1"
    );
}

/// GO PORT of `pkg/expression/builtin_regexp_test.go:1228 TestRegexpCache`.
///
/// Go requires the compiled pattern to be REUSED only when both pattern and
/// match type are build-time constants, keyed by statement context id, and
/// verified by pointer identity across calls. The value behaviors riding on
/// that cache (identical results whether cached or not) are fully pinned by
/// the sweeps above; the caching itself has no Rust counterpart — this tier
/// compiles per evaluation and carries no statement-context registry.
///
/// go-parity-gap: regexp memoization keyed on statement context id (and the
/// tryVecMemorizedRegexp seam) is unported; observable matches covered by the
/// matrix tests above.
#[test]
#[ignore = "go-parity-gap: statement-context-keyed compiled-regexp memoization is unmodeled"]
fn regexp_cache_identity_by_statement_context_gap() {}

/// go-parity-gap: Go testing.B microbenchmarks behind the vec regexp cases
/// (`BenchmarkVectorizedBuiltinRegexpForConstants`,
/// `BenchmarkVectorizedBuiltinOtherFunc`, and the generated
/// EVALONEVEC/FUNC benchmark twins) have no nextest equivalent; the gate's
/// `/bench/` filter excludes them by construction.
#[test]
#[ignore = "go-parity-gap: testing.B benchmarks excluded from the gate"]
fn regexp_and_other_vec_benchmark_gap() {}
