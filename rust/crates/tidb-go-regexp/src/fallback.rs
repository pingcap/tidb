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

//! Lower-cost first-use matching for general boolean patterns.

use std::cell::RefCell;

use memchr::memmem;
use regex_automata::nfa::thompson::{
    backtrack::{BoundedBacktracker, Cache},
    Compiler, WhichCaptures, NFA,
};
use regex_automata::util::syntax;
use regex_syntax::hir::{Hir, HirKind};

use super::CompileFlags;

const MAX_LITE_TEXT_BYTES: usize = 64;
const MAX_BACKTRACK_TEXT_BYTES: usize = 1024;
const MAX_BACKTRACK_NFA_BYTES: usize = 64 * 1024;

struct BacktrackSearch {
    compiler: Compiler,
    cache: Option<Cache>,
}

impl BacktrackSearch {
    fn new() -> Self {
        let mut compiler = NFA::compiler();
        compiler.configure(
            NFA::config()
                .which_captures(WhichCaptures::None)
                .nfa_size_limit(Some(MAX_BACKTRACK_NFA_BYTES)),
        );
        Self {
            compiler,
            cache: None,
        }
    }

    fn is_match(&mut self, text: &str, pattern: &str, flags: CompileFlags) -> Option<bool> {
        self.compiler.syntax(
            syntax::Config::new()
                .octal(true)
                .case_insensitive(flags.case_insensitive)
                .multi_line(flags.multi_line)
                .dot_matches_new_line(flags.dot_matches_new_line),
        );
        let nfa = self.compiler.build(pattern).ok()?;
        let matcher = BoundedBacktracker::builder().build_from_nfa(nfa).ok()?;
        if let Some(cache) = &mut self.cache {
            cache.reset(&matcher);
        }
        let cache = self.cache.get_or_insert_with(|| matcher.create_cache());
        matcher.try_is_match(cache, text).ok()
    }
}

thread_local! {
    static BACKTRACK_SEARCH: RefCell<BacktrackSearch> = RefCell::new(BacktrackSearch::new());
}

pub(super) fn try_match(text: &str, pattern: &str, flags: CompileFlags) -> Option<bool> {
    if text.len() > MAX_BACKTRACK_TEXT_BYTES {
        return reject_without_literal(text, pattern, flags).then_some(false);
    }
    if text.len() <= MAX_LITE_TEXT_BYTES && can_use_lite(text, pattern, flags.case_insensitive) {
        if let Ok(matcher) = regex_lite::RegexBuilder::new(pattern)
            .case_insensitive(flags.case_insensitive)
            .multi_line(flags.multi_line)
            .dot_matches_new_line(flags.dot_matches_new_line)
            .build()
        {
            return Some(matcher.is_match(text));
        }
    }
    BACKTRACK_SEARCH
        .try_with(|search| search.borrow_mut().is_match(text, pattern, flags))
        .ok()
        .flatten()
}

fn reject_without_literal(text: &str, pattern: &str, flags: CompileFlags) -> bool {
    // Leave counted repetitions and braced classes to the full engine,
    // where even small patterns can hit the compiled program size limit.
    if pattern.contains('{') {
        return false;
    }
    let Ok(hir) = regex_syntax::ParserBuilder::new()
        .octal(true)
        .case_insensitive(flags.case_insensitive)
        .multi_line(flags.multi_line)
        .dot_matches_new_line(flags.dot_matches_new_line)
        .build()
        .parse(pattern)
    else {
        return false;
    };
    required_literal(&hir).is_some_and(|literal| memmem::find(text.as_bytes(), literal).is_none())
}

fn required_literal(hir: &Hir) -> Option<&[u8]> {
    match hir.kind() {
        HirKind::Literal(literal) => Some(&literal.0),
        HirKind::Capture(capture) => required_literal(&capture.sub),
        HirKind::Concat(parts) => parts
            .iter()
            .filter_map(required_literal)
            .max_by_key(|s| s.len()),
        HirKind::Repetition(repeat) if repeat.min > 0 => required_literal(&repeat.sub),
        _ => None,
    }
}

fn can_use_lite(text: &str, pattern: &str, folded: bool) -> bool {
    let inline_fold = pattern.match_indices("(?").any(|(index, _)| {
        pattern[index + 2..]
            .bytes()
            .take_while(|byte| matches!(byte, b'i' | b'm' | b's' | b'U' | b'-' | b'u'))
            .any(|byte| byte == b'i')
    });
    // Folding can join an ASCII rune with a Unicode rune. The lightweight
    // engine is exact when both sides use ASCII and the pattern cannot
    // introduce an additional Unicode rune through a hex escape. Folded
    // POSIX complements also require the full case-fold-before-negation rule.
    !folded && !inline_fold
        || text.is_ascii()
            && pattern.is_ascii()
            && !pattern.contains(r"\x{")
            && !pattern.contains("[:^")
}
