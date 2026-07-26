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

use super::*;

fn t(schema: &str, name: &str) -> Table {
    Table::new(schema, name)
}

fn rules(
    do_dbs: &[&str],
    ignore_dbs: &[&str],
    do_tables: &[(&str, &str)],
    ignore_tables: &[(&str, &str)],
) -> Rules {
    Rules {
        do_dbs: do_dbs.iter().map(|s| (*s).to_owned()).collect(),
        ignore_dbs: ignore_dbs.iter().map(|s| (*s).to_owned()).collect(),
        do_tables: do_tables.iter().map(|(s, n)| t(s, n)).collect(),
        ignore_tables: ignore_tables.iter().map(|(s, n)| t(s, n)).collect(),
        ..Default::default()
    }
}

// Go TestFilterOnSchema (uses ApplyOn).
#[test]
fn filter_on_schema() {
    struct C {
        cs: bool,
        rules: Option<Rules>,
        input: Vec<Table>,
        output: Vec<Table>,
    }
    let cases = vec![
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[])),
            input: vec![],
            output: vec![],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[])),
            input: vec![t("foo", "bar"), t("foo", "")],
            output: vec![t("foo", "bar"), t("foo", "")],
        },
        // schema-only rules
        C {
            cs: false,
            rules: Some(rules(&["foo"], &["foo"], &[], &[])),
            input: vec![
                t("foo", "bar"),
                t("foo", ""),
                t("foo1", "bar"),
                t("foo1", ""),
            ],
            output: vec![t("foo", "bar"), t("foo", "")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &["foo1"], &[], &[])),
            input: vec![
                t("foo", "bar"),
                t("foo", ""),
                t("foo1", "bar"),
                t("foo1", ""),
            ],
            output: vec![t("foo", "bar"), t("foo", "")],
        },
        // DoTable rules (without regex)
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[("foo", "bar1")], &[])),
            input: vec![
                t("foo", "bar"),
                t("foo", "bar1"),
                t("foo", ""),
                t("fff", "bar1"),
            ],
            output: vec![t("foo", "bar1"), t("foo", "")],
        },
        // ignoreTable rules (without regex)
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("foo", "bar")])),
            input: vec![
                t("foo", "bar"),
                t("foo", "bar1"),
                t("foo", ""),
                t("fff", "bar1"),
            ],
            output: vec![t("foo", "bar1"), t("foo", ""), t("fff", "bar1")],
        },
        // all regexp
        C {
            cs: false,
            rules: Some(rules(&["~^foo"], &[], &[], &[("~^foo", "~^sbtest-\\d")])),
            input: vec![
                t("foo", "sbtest"),
                t("foo1", "sbtest-1"),
                t("foo2", ""),
                t("fff", "bar"),
            ],
            output: vec![t("foo", "sbtest"), t("foo2", "")],
        },
        // rules with * or ? (glob patterns handled by the trie)
        C {
            cs: false,
            rules: Some(rules(&[], &["foo[bar]", "foo?", "special\\"], &[], &[])),
            input: vec![
                t("foor", "a"),
                t("foo[bar]", "b"),
                t("fo", "c"),
                t("foo?", "d"),
                t("special\\", "e"),
            ],
            output: vec![t("foo[bar]", "b"), t("fo", "c")],
        },
        // ensure non case-insensitive
        C {
            cs: false,
            rules: Some(rules(&[], &["~^FOO"], &[], &[("~.*", "~FoO$")])),
            input: vec![
                t("FOO1", "a"),
                t("foo2", "b"),
                t("BoO3", "cFoO"),
                t("Foo4", "dfoo"),
                t("5", "5"),
            ],
            output: vec![t("5", "5")],
        },
        // ensure case-insensitive
        C {
            cs: true,
            rules: Some(rules(&[], &["~^FOO"], &[], &[("~.*", "~FoO$")])),
            input: vec![
                t("FOO1", "a"),
                t("foo2", "b"),
                t("BoO3", "cFoo"),
                t("Foo4", "dfoo"),
                t("5", "5"),
            ],
            output: vec![
                t("foo2", "b"),
                t("BoO3", "cFoo"),
                t("Foo4", "dfoo"),
                t("5", "5"),
            ],
        },
        // schema part not regex, table part regex
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("a?b?", "~f[0-9]")])),
            input: vec![
                t("abbd", "f1"),
                t("aaaa", "f2"),
                t("5", "5"),
                t("abbc", "fa"),
            ],
            output: vec![t("aaaa", "f2"), t("5", "5"), t("abbc", "fa")],
        },
        // schema part regex, table part not regex
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("~t[0-8]", "a??")])),
            input: vec![t("t1", "a01"), t("t9", "a02"), t("5", "5"), t("t9", "a001")],
            output: vec![t("t9", "a02"), t("5", "5"), t("t9", "a001")],
        },
        C {
            cs: true,
            rules: Some(rules(&[], &[], &[], &[("a*", "A*")])),
            input: vec![t("aB", "Ab"), t("AaB", "aab"), t("acB", "Afb")],
            output: vec![t("AaB", "aab")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("a*", "A*")])),
            input: vec![t("aB", "Ab"), t("AaB", "aab"), t("acB", "Afb")],
            output: vec![],
        },
    ];

    for (i, c) in cases.into_iter().enumerate() {
        let f = Filter::new(c.cs, c.rules).unwrap();
        let got = f.apply_on(&c.input);
        assert_eq!(got, c.output, "case {i}");
    }
}

// Go TestCaseSensitiveApply (uses Apply, returns original tables).
#[test]
fn case_sensitive_apply() {
    struct C {
        cs: bool,
        rules: Option<Rules>,
        input: Vec<Table>,
        output: Vec<Table>,
    }
    let cases = vec![
        C {
            cs: false,
            rules: Some(rules(&["foo"], &["foo"], &[], &[])),
            input: vec![
                t("foo", "bar"),
                t("foo", ""),
                t("foo1", "bar"),
                t("foo1", ""),
            ],
            output: vec![t("foo", "bar"), t("foo", "")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &["foo1"], &[], &[])),
            input: vec![
                t("foo", "bar"),
                t("foo", ""),
                t("foo1", "bar"),
                t("foo1", ""),
            ],
            output: vec![t("foo", "bar"), t("foo", "")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("Foo", "bAr")])),
            input: vec![
                t("foo", "bar"),
                t("foo", "bar1"),
                t("foo", ""),
                t("fff", "bar1"),
            ],
            output: vec![t("foo", "bar1"), t("foo", ""), t("fff", "bar1")],
        },
        C {
            cs: false,
            rules: Some(rules(&["~^foo"], &[], &[], &[("~^foo", "~^sbtest-\\d")])),
            input: vec![
                t("foo", "sbtest"),
                t("foo1", "sbtest-1"),
                t("foo2", ""),
                t("fff", "bar"),
            ],
            output: vec![t("foo", "sbtest"), t("foo2", "")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &["foo[bar]", "foo?", "special\\"], &[], &[])),
            input: vec![
                t("foor", "a"),
                t("foo[bar]", "b"),
                t("Fo", "c"),
                t("foo?", "d"),
                t("special\\", "e"),
            ],
            output: vec![t("foo[bar]", "b"), t("Fo", "c")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &["~^FOO"], &[], &[("~.*", "~FoO$")])),
            input: vec![
                t("FOO1", "a"),
                t("foo2", "b"),
                t("BoO3", "cFoO"),
                t("Foo4", "dfoo"),
                t("5", "5"),
            ],
            output: vec![t("5", "5")],
        },
        C {
            cs: true,
            rules: Some(rules(&[], &["~^FOO"], &[], &[("~.*", "~FoO$")])),
            input: vec![
                t("FOO1", "a"),
                t("foo2", "b"),
                t("BoO3", "cFoo"),
                t("Foo4", "dfoo"),
                t("5", "5"),
            ],
            output: vec![
                t("foo2", "b"),
                t("BoO3", "cFoo"),
                t("Foo4", "dfoo"),
                t("5", "5"),
            ],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("a?b?", "~f[0-9]")])),
            input: vec![
                t("abBd", "f1"),
                t("aAAa", "f2"),
                t("5", "5"),
                t("abbc", "FA"),
            ],
            output: vec![t("aAAa", "f2"), t("5", "5"), t("abbc", "FA")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("~t[0-8]", "A??")])),
            input: vec![t("t1", "a01"), t("t9", "A02"), t("5", "5"), t("T9", "a001")],
            output: vec![t("t9", "A02"), t("5", "5"), t("T9", "a001")],
        },
        C {
            cs: true,
            rules: Some(rules(&[], &[], &[], &[("a*", "A*")])),
            input: vec![t("aB", "Ab"), t("AaB", "aab"), t("acB", "Afb")],
            output: vec![t("AaB", "aab")],
        },
        C {
            cs: false,
            rules: Some(rules(&[], &[], &[], &[("a*", "A*")])),
            input: vec![t("aB", "Ab"), t("AaB", "aab"), t("acB", "Afb")],
            output: vec![],
        },
    ];

    for (i, c) in cases.into_iter().enumerate() {
        let f = Filter::new(c.cs, c.rules).unwrap();
        let got = f.apply(&c.input);
        assert_eq!(got, c.output, "case {i}");
    }
}

// Go TestMaxBox.
#[test]
fn max_box() {
    let r = Filter::new(
        false,
        Some(rules(&[], &[], &[("test1", "t1")], &[("test1", "t2")])),
    )
    .unwrap();
    let x = t("test1", "");
    let res = r.apply_on(std::slice::from_ref(&x));
    assert_eq!(res.len(), 1);
    assert_eq!(res[0], x);
}

// Go TestCaseSensitive.
#[test]
fn case_sensitive() {
    let r = Filter::new(true, Some(rules(&[], &["~^FOO"], &[], &[("~.*", "~FoO$")]))).unwrap();
    let input = vec![
        t("FOO1", "a"),
        t("foo2", "b"),
        t("BoO3", "cFoO"),
        t("Foo4", "dfoo"),
        t("5", "5"),
    ];
    let actual = r.apply_on(&input);
    assert_eq!(actual, vec![t("foo2", "b"), t("Foo4", "dfoo"), t("5", "5")]);
    assert!(!r.matches(&t("FOO", "a")));

    let r = Filter::new(false, Some(rules(&["BAR"], &[], &[], &[]))).unwrap();
    assert!(r.matches(&t("bar", "a")));
    assert!(r.matches(&t("BAR", "a")));
}

// Go TestInvalidRegex: look-around is rejected by the regex engine.
#[test]
fn invalid_regex() {
    for pat in ["~^t[0-9]+((?!_copy).)*$", "~^t[0-9]+sp(?=copy).*"] {
        let r = Filter::new(true, Some(rules(&[pat], &[], &[], &[])));
        assert!(r.is_err(), "{pat} should fail to compile");
    }
}

// Go TestMatchReturnsBool.
#[test]
fn match_returns_bool() {
    let f = Filter::new(true, Some(rules(&["sns"], &[], &[], &[]))).unwrap();
    assert!(f.matches(&t("sns", "")));
    assert!(!f.matches(&t("other", "")));

    let f = Filter::new(true, None).unwrap();
    assert!(f.matches(&t("other", "")));
}

// Go TestIsSystemSchema (schema.go).
#[test]
fn system_schema() {
    assert!(is_system_schema("dm_heartbeat"));
    assert!(is_system_schema("inspection_schema"));
    assert!(is_system_schema("information_schema"));
    assert!(is_system_schema("mysql"));
    assert!(!is_system_schema("test"));
}
