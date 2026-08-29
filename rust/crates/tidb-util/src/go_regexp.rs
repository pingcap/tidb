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

//! Crate-private compatibility with Go's `regexp` character-class semantics.

use regex::Regex;
use regex_syntax::ast::parse::Parser;
use regex_syntax::ast::{self, Ast, Visitor};

#[derive(Clone, Copy)]
struct RegexReplacement {
    start: usize,
    end: usize,
    value: &'static str,
}

#[derive(Default)]
struct GoRegexpVisitor {
    replacements: Vec<RegexReplacement>,
}

impl GoRegexpVisitor {
    fn perl_class(class: &ast::ClassPerl) -> RegexReplacement {
        let value = match (&class.kind, class.negated) {
            (ast::ClassPerlKind::Digit, false) => "[0-9]",
            (ast::ClassPerlKind::Digit, true) => "[^0-9]",
            (ast::ClassPerlKind::Space, false) => "[\\t\\n\\f\\r ]",
            (ast::ClassPerlKind::Space, true) => "[^\\t\\n\\f\\r ]",
            (ast::ClassPerlKind::Word, false) => "[0-9A-Za-z_]",
            (ast::ClassPerlKind::Word, true) => "[^0-9A-Za-z_]",
        };
        RegexReplacement {
            start: class.span.start.offset,
            end: class.span.end.offset,
            value,
        }
    }
}

impl Visitor for GoRegexpVisitor {
    type Output = Vec<RegexReplacement>;
    type Err = std::convert::Infallible;

    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(self.replacements)
    }

    fn visit_pre(&mut self, node: &Ast) -> Result<(), Self::Err> {
        match node {
            Ast::ClassPerl(class) => self.replacements.push(Self::perl_class(class)),
            Ast::Assertion(assertion) => {
                let value = match assertion.kind {
                    ast::AssertionKind::WordBoundary => Some("(?-u:\\b)"),
                    ast::AssertionKind::NotWordBoundary => Some("(?-u:\\B)"),
                    _ => None,
                };
                if let Some(value) = value {
                    self.replacements.push(RegexReplacement {
                        start: assertion.span.start.offset,
                        end: assertion.span.end.offset,
                        value,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn visit_class_set_item_pre(&mut self, item: &ast::ClassSetItem) -> Result<(), Self::Err> {
        if let ast::ClassSetItem::Perl(class) = item {
            self.replacements.push(Self::perl_class(class));
        }
        Ok(())
    }
}

// Go's regexp package defines Perl character classes and word boundaries over
// ASCII. Rust regex deliberately makes the same spellings Unicode-aware.
// Rewrite only those constructs; Unicode literals, `.`, and `\p{...}` retain
// their normal rune semantics.
fn rewrite_pattern(pattern: &str) -> Result<String, String> {
    let ast = Parser::new()
        .parse(pattern)
        .map_err(|error| error.to_string())?;
    let mut replacements =
        ast::visit(&ast, GoRegexpVisitor::default()).expect("the regexp visitor is infallible");
    if replacements.is_empty() {
        return Ok(pattern.to_owned());
    }
    replacements.sort_unstable_by_key(|replacement| std::cmp::Reverse(replacement.start));
    let mut result = pattern.to_owned();
    for replacement in replacements {
        result.replace_range(replacement.start..replacement.end, replacement.value);
    }
    Ok(result)
}

pub(crate) fn compile(pattern: &str, case_sensitive: bool) -> Result<Regex, String> {
    let pattern = rewrite_pattern(pattern)?;
    let pattern = if case_sensitive {
        pattern
    } else {
        format!("(?i){pattern}")
    };
    Regex::new(&pattern).map_err(|error| error.to_string())
}
