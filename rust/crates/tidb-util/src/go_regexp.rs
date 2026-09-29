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

//! Shared compatibility with Go's `regexp` character-class semantics.

use regex::{Regex, RegexBuilder};
use regex_syntax::ast::parse::ParserBuilder;
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
    type Err = String;

    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(self.replacements)
    }

    fn visit_pre(&mut self, node: &Ast) -> Result<(), Self::Err> {
        match node {
            Ast::Flags(flags) => validate_flags(&flags.flags)?,
            Ast::Group(group) => {
                if let ast::GroupKind::NonCapturing(flags) = &group.kind {
                    validate_flags(flags)?;
                }
            }
            Ast::Repetition(repetition) => {
                // Go rejects adjacent repeat operators, but permits grouped
                // repeats. The AST preserves that distinction.
                if matches!(repetition.ast.as_ref(), Ast::Repetition(_)) {
                    return Err("invalid nested repetition operator".to_owned());
                }
                validate_repeats(node)?;
            }
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
    let normalized = normalize_pattern(pattern)?;
    let pattern = normalized.as_str();
    let ast = ParserBuilder::new()
        .octal(true)
        .nest_limit(1000)
        .build()
        .parse(pattern)
        .map_err(|error| error.to_string())?;
    let mut replacements = ast::visit(&ast, GoRegexpVisitor::default())?;
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

fn validate_flags(flags: &ast::Flags) -> Result<(), String> {
    for item in &flags.items {
        if let ast::FlagsItemKind::Flag(flag) = &item.kind {
            if !matches!(
                flag,
                ast::Flag::CaseInsensitive
                    | ast::Flag::MultiLine
                    | ast::Flag::DotMatchesNewLine
                    | ast::Flag::SwapGreed
            ) {
                return Err("invalid or unsupported Perl syntax".to_owned());
            }
        }
    }
    Ok(())
}

// Go regexp/syntax.repeatIsValid limits the product of nested counted
// repetitions to 1000, independently of the execution engine's size limit.
fn validate_repeats(root: &Ast) -> Result<(), String> {
    let mut pending = vec![(root, 1000u32)];
    while let Some((node, mut budget)) = pending.pop() {
        match node {
            Ast::Repetition(repetition) => {
                if let ast::RepetitionKind::Range(range) = &repetition.op.kind {
                    let count = match range {
                        ast::RepetitionRange::Exactly(n) | ast::RepetitionRange::AtLeast(n) => *n,
                        ast::RepetitionRange::Bounded(_, n) => *n,
                    };
                    if count == 0 {
                        continue;
                    }
                    if count > budget {
                        return Err("invalid repeat count".to_owned());
                    }
                    budget /= count;
                }
                pending.push((&repetition.ast, budget));
            }
            Ast::Group(group) => pending.push((&group.ast, budget)),
            Ast::Alternation(parts) => pending.extend(parts.asts.iter().map(|node| (node, budget))),
            Ast::Concat(parts) => pending.extend(parts.asts.iter().map(|node| (node, budget))),
            _ => {}
        }
    }
    Ok(())
}

// Normalize Go's bracket grammar before the backend AST parser: '&', '~',
// and nested '[' are ordinary class literals, never Rust set operations.
// Preserve POSIX classes and escapes as syntactic units. Go's quoted literal
// regions are escaped once here, before any class/flag rewriting.
fn normalize_pattern(pattern: &str) -> Result<String, String> {
    let mut output = String::new();
    let mut chars = pattern.chars().peekable();
    let mut in_class = false;
    let mut first = false;
    let mut negated = false;
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let escaped = chars
                .next()
                .ok_or_else(|| "trailing backslash".to_owned())?;
            if escaped == 'Q' && !in_class {
                let mut quoted = String::new();
                while let Some(ch) = chars.next() {
                    if ch == '\\' && chars.peek() == Some(&'E') {
                        chars.next();
                        break;
                    }
                    quoted.push(ch);
                }
                output.push_str(&regex::escape(&quoted));
            } else {
                if matches!(escaped, 'u' | 'U')
                    || (('1'..='7').contains(&escaped)
                        && !chars.peek().is_some_and(|ch| ('0'..='7').contains(ch)))
                {
                    return Err("invalid escape sequence".to_owned());
                }
                output.push('\\');
                output.push(escaped);
            }
            first = false;
        } else if in_class {
            match ch {
                ']' if !first => {
                    output.push(ch);
                    in_class = false;
                }
                '^' if first && !negated => {
                    negated = true;
                    output.push(ch);
                    continue;
                }
                '[' if chars.peek() == Some(&':') => {
                    output.push(ch);
                    for part in chars.by_ref() {
                        output.push(part);
                        if part == ']' {
                            break;
                        }
                    }
                }
                '-' if first => output.push_str(r"\x2D"),
                '-' if chars.peek() == Some(&'-') => {
                    // The first hyphen is Go's range operator; the second
                    // is its literal endpoint, never set subtraction.
                    chars.next();
                    output.push_str(r"-\x2D");
                }
                '[' | ']' | '&' | '~' => {
                    output.push('\\');
                    output.push(ch);
                }
                _ => output.push(ch),
            }
            first = false;
        } else {
            if ch == '[' {
                in_class = true;
                first = true;
                negated = false;
            }
            output.push(ch);
        }
    }
    Ok(output)
}

/// Compile using Go's character classes and syntax boundary, with options
/// supplied by the caller's signature. All utility and SQL users share this.
pub fn compile_with_flags(
    pattern: &str,
    case_insensitive: bool,
    multi_line: bool,
    dot_matches_new_line: bool,
) -> Result<Regex, String> {
    let pattern = rewrite_pattern(pattern)?;
    RegexBuilder::new(&pattern)
        .octal(true)
        .case_insensitive(case_insensitive)
        .multi_line(multi_line)
        .dot_matches_new_line(dot_matches_new_line)
        .build()
        .map_err(|error| error.to_string())
}

pub(crate) fn compile(pattern: &str, case_sensitive: bool) -> Result<Regex, String> {
    compile_with_flags(pattern, !case_sensitive, false, false)
}

#[cfg(test)]
mod tests {
    use super::compile;

    #[test]
    fn go_syntax_and_character_contracts() {
        for (pattern, text, expected) in [
            (r"\d", "١", false),
            (r"\w", "é", false),
            (r"\s", "\u{a0}", false),
            (r"\bé\b", "é", false),
            (r"[a-z&&b]", "a", true),
            (r"[[a]", "[", true),
            (r"[--a]", "0", true),
            (r"[^^]", "a", true),
            (r"[^^]", "^", false),
            (r"\Q(a)+\E", "(a)+", true),
            (r"\141", "a", true),
            (r"(?i:a)", "A", true),
            (r"\p{Greek}", "Σ", true),
        ] {
            assert_eq!(
                compile(pattern, true).unwrap().is_match(text),
                expected,
                "{pattern}"
            );
        }
        for pattern in [
            r"[a--b]",
            r"(?u)a",
            r"(?x)a",
            r"\u{61}",
            r"a++",
            r"a{1001}",
            r"(a{20}){51}",
            r"\1",
        ] {
            assert!(compile(pattern, true).is_err(), "{pattern}");
        }
        assert!(compile(r"(a{20}){50}", true).is_ok());
    }
}
