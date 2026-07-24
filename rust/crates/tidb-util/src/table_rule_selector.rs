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

//! Complete transcreation of Go `pkg/util/table-rule-selector` (Go package
//! `selector`, file `trie_selector.go`).
//!
//! A two-level (schema then table) wildcard trie that stores rules for fast
//! retrieval. Patterns support `*` (zero or more, must be last), `?` (exactly
//! one), and character ranges like `[abc]`, `[a-z]`, `[!a-c]`.
//!
//! Go builds this from a pointer graph: `*node`s linked through an `item`
//! interface (`baseItem` and the range-carrying `rangeItem`), with each item
//! owning a `child` sub-trie and, at the schema level, a `nextLevel` table
//! sub-trie. Rust's ownership model fights that shape, so the graph is flattened
//! into two arenas ([`Node`] and [`Item`]) indexed by [`NodeId`]/[`ItemId`];
//! the `item` interface collapses into [`Item`]'s `kind` field. Semantics —
//! insertion order, match order, range parsing edge cases, and cache behavior —
//! are preserved exactly. The Go `any` rule payload becomes the generic `R`.

use std::collections::HashMap;
use std::fmt;

// 1. asterisk (`*`) matches zero or more characters and must be the last
//    character of a wildcard word;
// 2. question mark (`?`) matches exactly one character.
const ASTERISK: u8 = b'*';
const QUESTION: u8 = b'?';
const RANGE_OPEN: u8 = b'[';
const RANGE_CLOSE: u8 = b']';
const RANGE_NOT: u8 = b'!';
const RANGE_BETWEEN: u8 = b'-';

const MAX_CACHE_NUM: usize = 1024;

/// The kind of insert operation, mirroring Go's `Insert`/`Replace`/`Append`
/// constants.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InsertType {
    /// Insert a new rule; errors if a rule already exists at the pattern.
    Insert,
    /// Replace any existing rule with this one.
    Replace,
    /// Append this rule to any existing rules at the pattern.
    Append,
}

/// Error returned by selector operations.
#[derive(Debug, Clone)]
pub struct SelectorError(String);

impl SelectorError {
    fn new(msg: impl Into<String>) -> Self {
        SelectorError(msg.into())
    }

    fn annotate(self, context: &str) -> Self {
        SelectorError(format!("{context}: {}", self.0))
    }
}

impl fmt::Display for SelectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SelectorError {}

/// Stores rules of schema/table for easy retrieval.
pub trait Selector<R> {
    /// Inserts one rule into the trie. If `table` is empty the rule goes to the
    /// schema level, otherwise to the table level. A `None` rule is rejected.
    fn insert(
        &mut self,
        schema: &str,
        table: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<(), SelectorError>;

    /// Returns all matched rules.
    fn match_rules(&mut self, schema: &str, table: &str) -> Vec<R>;

    /// Removes one rule.
    fn remove(&mut self, schema: &str, table: &str) -> Result<(), SelectorError>;

    /// Returns all rules: schema-level rules and table-level rules.
    #[allow(clippy::type_complexity)]
    fn all_rules(
        &self,
    ) -> (
        HashMap<String, Vec<R>>,
        HashMap<String, HashMap<String, Vec<R>>>,
    );
}

type NodeId = usize;
type ItemId = usize;

/// A single range like `a`, `a-z`, from a `[...]` pattern.
#[derive(Clone, Copy)]
struct Ran {
    start: u8,
    end: u8,
    has_between: bool,
}

/// The parsed content of a `[...]` range pattern.
#[derive(Clone)]
struct RangeSpec {
    has_not: bool,
    ranges: Vec<Ran>,
}

impl RangeSpec {
    fn equal(&self, other: &RangeSpec) -> bool {
        self.matches(other) && other.matches(self)
    }

    fn matches(&self, other: &RangeSpec) -> bool {
        if self.has_not != other.has_not {
            return false;
        }
        for r in &self.ranges {
            let mut matched = false;
            for r2 in &other.ranges {
                if r2.start <= r.start && r.end <= r2.end {
                    matched = true;
                    break;
                }
            }
            if !matched {
                return false;
            }
        }
        true
    }

    fn match_char(&self, c: u8) -> bool {
        for r in &self.ranges {
            if r.start <= c && c <= r.end {
                return !self.has_not;
            }
        }
        self.has_not
    }

    fn str(&self) -> String {
        let mut ret = String::from("[");
        if self.has_not {
            ret.push(RANGE_NOT as char);
        }
        for r in &self.ranges {
            if r.has_between {
                ret.push(r.start as char);
                ret.push(RANGE_BETWEEN as char);
                ret.push(r.end as char);
            } else {
                ret.push(r.start as char);
            }
        }
        ret.push(RANGE_CLOSE as char);
        ret
    }
}

/// The flattened equivalent of Go's `item` interface: a `baseItem`, or a
/// `rangeItem` that additionally carries a [`RangeSpec`].
enum ItemKind {
    Base,
    Range(RangeSpec),
}

/// An arena entry combining Go's `baseItem` fields with the `item` kind.
struct Item<R> {
    /// The child sub-trie (`child`/`setChild`).
    child: Option<NodeId>,
    /// The rules attached at this item (`getRule`/`setRule`/`appendRule`).
    rule: Vec<R>,
    /// The schema-level -> table-level link (`nextLevel`).
    next_level: Option<NodeId>,
    kind: ItemKind,
}

impl<R> Item<R> {
    fn new() -> Self {
        Item {
            child: None,
            rule: Vec::new(),
            next_level: None,
            kind: ItemKind::Base,
        }
    }
}

/// An arena trie node.
#[derive(Default)]
struct Node {
    characters: HashMap<u8, ItemId>,
    asterisk: Option<ItemId>,
    question: Option<ItemId>,
    r_items: Vec<ItemId>,
}

/// Accumulates matched rules and the next-level nodes discovered while matching.
struct MatchedResult<R> {
    nodes: Vec<NodeId>,
    rules: Vec<R>,
}

impl<R> MatchedResult<R> {
    fn new() -> Self {
        MatchedResult {
            nodes: Vec::new(),
            rules: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.rules.is_empty()
    }
}

/// A trie [`Selector`].
pub struct TrieSelector<R> {
    nodes: Vec<Node>,
    items: Vec<Item<R>>,
    root: NodeId,
    cache: HashMap<String, Vec<R>>,
}

impl<R: Clone> Default for TrieSelector<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Clone> TrieSelector<R> {
    /// Returns a new trie selector.
    #[must_use]
    pub fn new() -> Self {
        let mut s = TrieSelector {
            nodes: Vec::new(),
            items: Vec::new(),
            root: 0,
            cache: HashMap::new(),
        };
        s.root = s.new_node();
        s
    }

    fn new_node(&mut self) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Node::default());
        id
    }

    fn new_item(&mut self) -> ItemId {
        let id = self.items.len();
        self.items.push(Item::new());
        id
    }

    fn insert_schema(
        &mut self,
        schema: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<(), SelectorError> {
        let root = self.root;
        self.insert_inner(root, schema, rule, insert_type)
            .map_err(|e| e.annotate("insert into schema selector"))?;
        Ok(())
    }

    fn insert_table(
        &mut self,
        schema: &str,
        table: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<(), SelectorError> {
        let root = self.root;
        let schema_entity = self
            .insert_inner(root, schema, None, InsertType::Insert)
            .map_err(|e| e.annotate("insert into schema selector"))?;

        if self.items[schema_entity].next_level.is_none() {
            let nl = self.new_node();
            self.items[schema_entity].next_level = Some(nl);
        }

        let nl = self.items[schema_entity].next_level.unwrap();
        self.insert_inner(nl, table, rule, insert_type)
            .map_err(|e| e.annotate("insert into table selector"))?;
        Ok(())
    }

    /// If `rule` is `None`, just extract/create the nodes for `pattern` and
    /// return its leaf item without attaching a rule.
    fn insert_inner(
        &mut self,
        root: NodeId,
        pattern: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<ItemId, SelectorError> {
        let p = pattern.as_bytes();
        let mut n = root;
        let mut had_asterisk = false;
        let mut entity: Option<ItemId> = None;

        let mut i = 0;
        while i < p.len() {
            if had_asterisk {
                return Err(SelectorError::new(format!(
                    "pattern {pattern} is not valid"
                )));
            }

            let mut r_spec: Option<RangeSpec> = None;
            // Go declares `nextI` per-iteration, defaulting to 0; only the range
            // branch sets it, and -1 marks "no closing bracket".
            let mut next_i: i64 = 0;

            match p[i] {
                ASTERISK => {
                    entity = self.nodes[n].asterisk;
                    had_asterisk = true;
                }
                QUESTION => {
                    entity = self.nodes[n].question;
                }
                RANGE_OPEN => {
                    let (spec, ni) = get_range_item(&p[i..]);
                    next_i = ni;
                    if ni == -1 {
                        entity = self.nodes[n].characters.get(&p[i]).copied();
                    } else {
                        r_spec = spec;
                        entity = None;
                        let spec_ref = r_spec.as_ref().unwrap();
                        for &nr in &self.nodes[n].r_items {
                            if let ItemKind::Range(existing) = &self.items[nr].kind {
                                if existing.equal(spec_ref) {
                                    entity = Some(nr);
                                    break;
                                }
                            }
                        }
                    }
                }
                _ => {
                    entity = self.nodes[n].characters.get(&p[i]).copied();
                }
            }

            if entity.is_none() {
                let new_id = self.new_item();
                match p[i] {
                    ASTERISK => self.nodes[n].asterisk = Some(new_id),
                    QUESTION => self.nodes[n].question = Some(new_id),
                    RANGE_OPEN => {
                        if next_i == -1 {
                            self.nodes[n].characters.insert(p[i], new_id);
                        } else {
                            self.items[new_id].kind = ItemKind::Range(r_spec.take().unwrap());
                            self.nodes[n].r_items.push(new_id);
                        }
                    }
                    _ => {
                        self.nodes[n].characters.insert(p[i], new_id);
                    }
                }
                entity = Some(new_id);
            }

            let eid = entity.unwrap();
            if self.items[eid].child.is_none() {
                let child = self.new_node();
                self.items[eid].child = Some(child);
            }
            n = self.items[eid].child.unwrap();

            if next_i != -1 {
                i += next_i as usize;
            }
            i += 1;
        }

        if let Some(rule) = rule {
            let eid = entity.expect("non-empty pattern yields a leaf item");
            if insert_type == InsertType::Insert && !self.items[eid].rule.is_empty() {
                return Err(SelectorError::new(format!(
                    "pattern {pattern} already exists"
                )));
            }
            if insert_type == InsertType::Replace {
                self.items[eid].rule = vec![rule];
            } else {
                self.items[eid].rule.push(rule);
            }
            self.clear_cache();
        }

        Ok(entity.expect("non-empty pattern yields a leaf item"))
    }

    fn match_node(&self, start: NodeId, s: &str, mr: &mut MatchedResult<R>) {
        let sb = s.as_bytes();
        let mut n = start;
        let mut entity: Option<ItemId> = None;
        let mut returned = false;

        for i in 0..sb.len() {
            let asterisk = self.nodes[n].asterisk;
            let question = self.nodes[n].question;
            let r_items = self.nodes[n].r_items.clone();
            let next = self.nodes[n].characters.get(&sb[i]).copied();

            if let Some(a) = asterisk {
                self.append_matched_item(a, mr);
            }

            if let Some(q) = question {
                if i == sb.len() - 1 {
                    self.append_matched_item(q, mr);
                }
                if let Some(qc) = self.items[q].child {
                    self.match_node(qc, &s[i + 1..], mr);
                }
            }

            for ri in r_items {
                let matched = match &self.items[ri].kind {
                    ItemKind::Range(spec) => spec.match_char(sb[i]),
                    ItemKind::Base => false,
                };
                if matched {
                    if i == sb.len() - 1 {
                        self.append_matched_item(ri, mr);
                    }
                    if let Some(rc) = self.items[ri].child {
                        self.match_node(rc, &s[i + 1..], mr);
                    }
                }
            }

            entity = next;
            match entity {
                None => {
                    returned = true;
                    break;
                }
                Some(e) => {
                    n = self.items[e].child.expect("inserted item has a child");
                }
            }
        }

        if returned {
            return;
        }

        if let Some(e) = entity {
            self.append_matched_item(e, mr);
        }

        if let Some(a) = self.nodes[n].asterisk {
            self.append_matched_item(a, mr);
        }
    }

    fn append_matched_item(&self, id: ItemId, mr: &mut MatchedResult<R>) {
        let it = &self.items[id];
        if !it.rule.is_empty() {
            mr.rules.extend(it.rule.iter().cloned());
        }
        if let Some(nl) = it.next_level {
            mr.nodes.push(nl);
        }
    }

    fn track(&self, start: NodeId, pattern: &str) -> Result<Vec<ItemId>, SelectorError> {
        let p = pattern.as_bytes();
        let mut n = start;
        let mut items: Vec<ItemId> = Vec::new();

        let not_found = || SelectorError::new(format!("pattern {pattern} not found"));

        let mut i = 0;
        while i < p.len() {
            match p[i] {
                ASTERISK => {
                    let a = self.nodes[n].asterisk.ok_or_else(not_found)?;
                    if i != p.len() - 1 {
                        return Err(SelectorError::new(format!(
                            "pattern {pattern} is not valid"
                        )));
                    }
                    items.push(a);
                }
                QUESTION => {
                    let q = self.nodes[n].question.ok_or_else(not_found)?;
                    items.push(q);
                    n = self.items[q].child.expect("inserted item has a child");
                }
                RANGE_OPEN => {
                    let (spec, next_i) = get_range_item(&p[i..]);
                    if next_i == -1 {
                        let it = self.nodes[n]
                            .characters
                            .get(&p[i])
                            .copied()
                            .ok_or_else(not_found)?;
                        items.push(it);
                        n = self.items[it].child.expect("inserted item has a child");
                    } else {
                        let spec = spec.unwrap();
                        let mut matched: Option<ItemId> = None;
                        for &ri in &self.nodes[n].r_items {
                            if let ItemKind::Range(existing) = &self.items[ri].kind {
                                if existing.equal(&spec) {
                                    matched = Some(ri);
                                    break;
                                }
                            }
                        }
                        let ri = matched.ok_or_else(not_found)?;
                        items.push(ri);
                        n = self.items[ri].child.expect("inserted item has a child");
                        i += next_i as usize;
                    }
                }
                _ => {
                    let it = self.nodes[n]
                        .characters
                        .get(&p[i])
                        .copied()
                        .ok_or_else(not_found)?;
                    items.push(it);
                    n = self.items[it].child.expect("inserted item has a child");
                }
            }
            i += 1;
        }

        Ok(items)
    }

    #[allow(clippy::type_complexity)]
    fn travel(
        &self,
        n: NodeId,
        word: Vec<u8>,
        rules: &mut HashMap<String, Vec<R>>,
        mut nodes: Option<&mut HashMap<String, NodeId>>,
    ) {
        let asterisk = self.nodes[n].asterisk;
        let question = self.nodes[n].question;
        let r_items = self.nodes[n].r_items.clone();
        let chars: Vec<(u8, ItemId)> = self.nodes[n]
            .characters
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect();

        if let Some(a) = asterisk {
            let mut pattern = word.clone();
            pattern.push(ASTERISK);
            self.insert_matched_item_into_map(&pattern, a, rules, nodes.as_deref_mut());
        }

        if let Some(q) = question {
            let mut pattern = word.clone();
            pattern.push(QUESTION);
            self.insert_matched_item_into_map(&pattern, q, rules, nodes.as_deref_mut());
            if let Some(qc) = self.items[q].child {
                self.travel(qc, pattern, rules, nodes.as_deref_mut());
            }
        }

        for ri in r_items {
            let mut pattern = word.clone();
            if let ItemKind::Range(spec) = &self.items[ri].kind {
                pattern.extend_from_slice(spec.str().as_bytes());
            }
            self.insert_matched_item_into_map(&pattern, ri, rules, nodes.as_deref_mut());
            if let Some(rc) = self.items[ri].child {
                self.travel(rc, pattern, rules, nodes.as_deref_mut());
            }
        }

        for (ch, base) in chars {
            let mut pattern = word.clone();
            pattern.push(ch);
            self.insert_matched_item_into_map(&pattern, base, rules, nodes.as_deref_mut());
            if let Some(cc) = self.items[base].child {
                self.travel(cc, pattern, rules, nodes.as_deref_mut());
            }
        }
    }

    fn insert_matched_item_into_map(
        &self,
        pattern: &[u8],
        id: ItemId,
        rules: &mut HashMap<String, Vec<R>>,
        nodes: Option<&mut HashMap<String, NodeId>>,
    ) {
        let it = &self.items[id];
        let key = String::from_utf8(pattern.to_vec()).expect("patterns are valid UTF-8");
        if !it.rule.is_empty() {
            rules.insert(key.clone(), it.rule.clone());
        }
        if let Some(nodes) = nodes {
            if let Some(nl) = it.next_level {
                nodes.insert(key, nl);
            }
        }
    }

    fn add_to_cache(&mut self, key: String, rules: Vec<R>) {
        self.cache.insert(key, rules);
        if self.cache.len() > MAX_CACHE_NUM {
            if let Some(k) = self.cache.keys().next().cloned() {
                self.cache.remove(&k);
            }
        }
    }

    fn clear_cache(&mut self) {
        self.cache.clear();
    }
}

impl<R: Clone> Selector<R> for TrieSelector<R> {
    fn insert(
        &mut self,
        schema: &str,
        table: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<(), SelectorError> {
        if schema.is_empty() || rule.is_none() {
            return Err(SelectorError::new(format!(
                "schema pattern {schema} or rule can't be empty"
            )));
        }

        if table.is_empty() {
            self.insert_schema(schema, rule, insert_type)
        } else {
            self.insert_table(schema, table, rule, insert_type)
        }
    }

    fn match_rules(&mut self, schema: &str, table: &str) -> Vec<R> {
        let cache_key = quote_schema_table(schema, table);
        if let Some(rules) = self.cache.get(&cache_key) {
            return rules.clone();
        }

        let root = self.root;
        let mut matched_schema = MatchedResult::new();
        self.match_node(root, schema, &mut matched_schema);

        // Not found in the schema level.
        if matched_schema.is_empty() {
            self.add_to_cache(cache_key, Vec::new());
            return Vec::new();
        }

        let mut rules: Vec<R> = matched_schema.rules.clone();
        for &si in &matched_schema.nodes {
            let mut matched_table = MatchedResult::new();
            self.match_node(si, table, &mut matched_table);
            rules.extend(matched_table.rules.iter().cloned());
        }

        self.add_to_cache(cache_key, rules.clone());
        rules
    }

    fn remove(&mut self, schema: &str, table: &str) -> Result<(), SelectorError> {
        if schema.is_empty() {
            return Err(SelectorError::new(format!(
                "schema/table {schema}/{table} is not valid"
            )));
        }

        let root = self.root;
        let schema_items = self.track(root, schema).map_err(|e| {
            e.annotate(&format!(
                "track schema/table {schema}/{table} in schema level"
            ))
        })?;
        let schema_leaf = *schema_items.last().unwrap();

        if !table.is_empty() {
            let nl = self.items[schema_leaf].next_level.ok_or_else(|| {
                SelectorError::new(format!(
                    "table level while we track schema/table {schema}/{table} not found"
                ))
            })?;

            let table_items = self.track(nl, table).map_err(|e| {
                e.annotate(&format!(
                    "track schema/table {schema}/{table} in table level"
                ))
            })?;
            let table_leaf = *table_items.last().unwrap();

            if self.items[table_leaf].rule.is_empty() {
                return Err(SelectorError::new(format!(
                    "schema/table {schema}/{table} in table level not found"
                )));
            }

            self.items[table_leaf].rule.clear();
            self.clear_cache();
            return Ok(());
        }

        if self.items[schema_leaf].rule.is_empty() {
            return Err(SelectorError::new(format!(
                "schema/table {schema}/{table} in schema level not found"
            )));
        }

        self.items[schema_leaf].rule.clear();
        self.clear_cache();
        Ok(())
    }

    fn all_rules(
        &self,
    ) -> (
        HashMap<String, Vec<R>>,
        HashMap<String, HashMap<String, Vec<R>>>,
    ) {
        let mut table_rules: HashMap<String, HashMap<String, Vec<R>>> = HashMap::new();
        let mut schema_nodes: HashMap<String, NodeId> = HashMap::new();
        let mut schema_rules: HashMap<String, Vec<R>> = HashMap::new();

        self.travel(
            self.root,
            Vec::new(),
            &mut schema_rules,
            Some(&mut schema_nodes),
        );

        for (schema, &n) in &schema_nodes {
            let mut rules: HashMap<String, Vec<R>> = HashMap::new();
            self.travel(n, Vec::new(), &mut rules, None);
            if !rules.is_empty() {
                table_rules.insert(schema.clone(), rules);
            }
        }

        (schema_rules, table_rules)
    }
}

/// Parses the `[...]` range at the start of `p`. Returns the parsed spec and the
/// index of the closing `]` within `p`, or `(None, -1)` when there is no closing
/// bracket (the `[` is then treated as a literal character).
fn get_range_item(p: &[u8]) -> (Option<RangeSpec>, i64) {
    let mut next_i: i64 = -1;
    for (i, &b) in p.iter().enumerate() {
        if b == RANGE_CLOSE {
            next_i = i as i64;
            break;
        }
    }
    if next_i == -1 {
        return (None, next_i);
    }
    let next = next_i as usize;

    let mut has_not = false;
    let mut ranges: Vec<Ran> = Vec::new();
    let mut start_i = 1;
    if p[start_i] == RANGE_NOT {
        start_i += 1;
        has_not = true;
    }

    let mut i = start_i;
    while i < next {
        if p[i + 1] == RANGE_BETWEEN && i + 2 < next {
            ranges.push(Ran {
                start: p[i],
                end: p[i + 2],
                has_between: true,
            });
            i += 2;
        } else {
            ranges.push(Ran {
                start: p[i],
                end: p[i],
                has_between: false,
            });
        }
        i += 1;
    }

    // Change `[!]` to a range that matches the literal `!`.
    if ranges.is_empty() && has_not {
        has_not = false;
        ranges.push(Ran {
            start: RANGE_NOT,
            end: RANGE_NOT,
            has_between: false,
        });
    }

    (Some(RangeSpec { has_not, ranges }), next_i)
}

fn quote_schema_table(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        return String::new();
    }
    if !table.is_empty() {
        return format!("`{schema}`.`{table}`");
    }
    format!("`{schema}`")
}

#[cfg(test)]
mod tests {
    use super::{quote_schema_table, InsertType, Selector, TrieSelector};
    use std::collections::HashMap;
    use std::rc::Rc;

    #[derive(Debug, PartialEq, Eq)]
    struct DummyRule {
        description: String,
    }

    type Rule = Rc<DummyRule>;

    fn dummy(description: &str) -> Rule {
        Rc::new(DummyRule {
            description: description.to_string(),
        })
    }

    // (schema pattern -> table patterns), mirroring `ts.tables`.
    fn tables() -> Vec<(&'static str, Vec<&'static str>)> {
        vec![
            ("t*", vec!["test*"]),
            ("schema*", vec!["", "test*", "abc*", "xyz"]),
            ("?bc", vec!["t1_abc", "t1_ab?", "abc*"]),
            ("a?c", vec!["t2_abc", "t2_ab*", "a?b"]),
            ("ab?", vec!["t3_ab?", "t3_ab*", "ab?"]),
            ("ab*", vec!["t4_abc", "t4_abc*", "ab*"]),
            ("abc", vec!["abc"]),
            ("abd", vec!["abc"]),
            ("ik[hjkl]", vec!["ik[!zxc]"]),
            ("ik[f-h]", vec!["ik[!a-ce-g]"]),
            ("i[x-z][1-3]", vec!["i?[x-z]", "ix*"]),
            // [\!-\!], [a-a\--\-], [a-c\--\-f-f].
            ("[!]", vec!["[a-]", "[a-c-f]"]),
            // [!a-c\!-\!f-g]
            ("[!a-c!f-g]", vec!["*"]),
            // [] matches nothing.
            ("[]*", vec!["*"]),
        ]
    }

    struct MatchCase {
        schema: &'static str,
        table: &'static str,
        matched_num: usize,
        // schema, table, schema, table, ...
        matched_rules: Vec<&'static str>,
    }

    fn match_cases() -> Vec<MatchCase> {
        let mk = |schema, table, matched_num, matched_rules: &[&'static str]| MatchCase {
            schema,
            table,
            matched_num,
            matched_rules: matched_rules.to_vec(),
        };
        vec![
            // test one level
            mk("dbc", "t1_abc", 2, &["?bc", "t1_ab?", "?bc", "t1_abc"]),
            mk("adc", "t2_abc", 2, &["a?c", "t2_ab*", "a?c", "t2_abc"]),
            mk("abd", "t3_abc", 2, &["ab?", "t3_ab*", "ab?", "t3_ab?"]),
            mk("abc", "t4_abc", 2, &["ab*", "t4_abc", "ab*", "t4_abc*"]),
            mk(
                "abc",
                "abc",
                4,
                &["?bc", "abc*", "ab*", "ab*", "ab?", "ab?", "abc", "abc"],
            ),
            // test only schema rule
            mk("schema1", "xxx", 1, &["schema*", ""]),
            mk("schema1", "", 1, &["schema*", ""]),
            // test table rule
            mk("schema1", "test1", 2, &["schema*", "", "schema*", "test*"]),
            mk("t1", "test1", 1, &["t*", "test*"]),
            mk("schema1", "abc1", 2, &["schema*", "", "schema*", "abc*"]),
            mk("ikj", "ikb", 1, &["ik[hjkl]", "ik[!zxc]"]),
            mk(
                "ikh",
                "iky",
                2,
                &["ik[hjkl]", "ik[!zxc]", "ik[f-h]", "ik[!a-ce-g]"],
            ),
            mk(
                "iz3",
                "ixz",
                2,
                &["i[x-z][1-3]", "i?[x-z]", "i[x-z][1-3]", "ix*"],
            ),
            mk("!", "-", 2, &["[!]", "[a-]", "[!]", "[a-c-f]"]),
            mk("!", "c", 1, &["[!]", "[a-c-f]"]),
            mk("d", "zxcv", 1, &["[!a-c!f-g]", "*"]),
        ]
    }

    fn remove_cases() -> Vec<&'static str> {
        // schema, table, schema, table, ...
        vec![
            "schema*",
            "",
            "a?c",
            "t2_ab*",
            "i[x-z][1-3]",
            "i?[x-z]",
            "[!]",
            "[a-c-f]",
        ]
    }

    #[allow(clippy::type_complexity)]
    fn generate_expected_rules() -> (
        HashMap<String, Vec<Rule>>,
        HashMap<String, HashMap<String, Vec<Rule>>>,
    ) {
        let mut schema_rules: HashMap<String, Vec<Rule>> = HashMap::new();
        let mut table_rules: HashMap<String, HashMap<String, Vec<Rule>>> = HashMap::new();
        for (schema, tbls) in tables() {
            table_rules.entry(schema.to_string()).or_default();
            for table in tbls {
                if table.is_empty() {
                    schema_rules.insert(
                        schema.to_string(),
                        vec![dummy(&quote_schema_table(schema, ""))],
                    );
                } else {
                    table_rules.get_mut(schema).unwrap().insert(
                        table.to_string(),
                        vec![dummy(&quote_schema_table(schema, table))],
                    );
                }
            }
        }
        (schema_rules, table_rules)
    }

    fn sort_rules(rules: &mut [Rule]) {
        rules.sort_by(|a, b| a.description.cmp(&b.description));
    }

    fn test_insert(
        s: &mut TrieSelector<Rule>,
        expected_schema_rules: &HashMap<String, Vec<Rule>>,
        expected_table_rules: &HashMap<String, HashMap<String, Vec<Rule>>>,
    ) {
        for rules in expected_schema_rules.values() {
            let schema = &rules[0].description;
            // Recover the schema pattern from the quoted description `` `schema` ``.
            let schema = schema.trim_matches('`');
            s.insert(schema, "", Some(rules[0].clone()), InsertType::Insert)
                .unwrap();
            // duplicate error
            assert!(s
                .insert(schema, "", Some(rules[0].clone()), InsertType::Insert)
                .is_err());
            // simple replace
            s.insert(schema, "", Some(rules[0].clone()), InsertType::Replace)
                .unwrap();
        }

        for (schema, tbls) in expected_table_rules {
            for (table, rules) in tbls {
                s.insert(schema, table, Some(rules[0].clone()), InsertType::Insert)
                    .unwrap();
                assert!(s
                    .insert(schema, table, Some(rules[0].clone()), InsertType::Insert)
                    .is_err());
                s.insert(schema, table, Some(rules[0].clone()), InsertType::Replace)
                    .unwrap();
            }
        }

        // rule can't be nil
        assert!(s.insert("schema", "", None, InsertType::Replace).is_err());
        // asterisk must be the last character of pattern
        assert!(s
            .insert("ab**", "", Some(dummy("error")), InsertType::Replace)
            .is_err());
        assert!(s
            .insert("abcd", "ab**", Some(dummy("error")), InsertType::Replace)
            .is_err());

        let (schemas, tbls) = s.all_rules();
        assert_eq!(&schemas, expected_schema_rules);
        assert_eq!(&tbls, expected_table_rules);
    }

    fn test_match(s: &mut TrieSelector<Rule>) {
        let mut cache: HashMap<String, Vec<Rule>> = HashMap::new();
        for mc in match_cases() {
            let mut rules = s.match_rules(mc.schema, mc.table);
            let mut expected: Vec<Rule> = Vec::with_capacity(mc.matched_num);
            for i in 0..mc.matched_num {
                expected.push(dummy(&quote_schema_table(
                    mc.matched_rules[2 * i],
                    mc.matched_rules[2 * i + 1],
                )));
            }
            sort_rules(&mut expected);
            sort_rules(&mut rules);
            assert_eq!(rules, expected, "match {}/{}", mc.schema, mc.table);
            cache.insert(quote_schema_table(mc.schema, mc.table), expected);
        }

        // not matched cases
        for (schema, table) in [("t1", ""), ("t1", "abc"), ("xxx", "abc")] {
            let rule = s.match_rules(schema, table);
            assert!(rule.is_empty());
            cache.insert(quote_schema_table(schema, table), rule);
        }

        // compare the internal cache (sorting each entry, like the Go test).
        let mut actual_cache = s.cache.clone();
        for entry in actual_cache.values_mut() {
            sort_rules(entry);
        }
        assert_eq!(actual_cache, cache);
    }

    fn test_append(
        s: &mut TrieSelector<Rule>,
        expected_schema_rules: &mut HashMap<String, Vec<Rule>>,
        expected_table_rules: &HashMap<String, HashMap<String, Vec<Rule>>>,
    ) {
        let appended = dummy("append");
        let schemas: Vec<String> = expected_schema_rules.keys().cloned().collect();
        for schema in schemas {
            expected_schema_rules
                .get_mut(&schema)
                .unwrap()
                .push(appended.clone());
            let pattern = schema.trim_matches('`').to_string();
            s.insert(&pattern, "", Some(appended.clone()), InsertType::Append)
                .unwrap();
        }
        let (out_schemas, out_tables) = s.all_rules();
        assert_eq!(&out_schemas, expected_schema_rules);
        assert_eq!(&out_tables, expected_table_rules);
    }

    fn test_replace(
        s: &mut TrieSelector<Rule>,
        expected_schema_rules: &mut HashMap<String, Vec<Rule>>,
        expected_table_rules: &HashMap<String, HashMap<String, Vec<Rule>>>,
    ) {
        let replaced = dummy("replace");
        let schemas: Vec<String> = expected_schema_rules.keys().cloned().collect();
        for schema in schemas {
            expected_schema_rules.insert(schema.clone(), vec![replaced.clone()]);
            let pattern = schema.trim_matches('`').to_string();
            s.insert(&pattern, "", Some(replaced.clone()), InsertType::Replace)
                .unwrap();
            s.insert(&pattern, "", Some(replaced.clone()), InsertType::Replace)
                .unwrap();
            assert!(s
                .insert(&pattern, "", Some(replaced.clone()), InsertType::Insert)
                .is_err());
        }
        let (out_schemas, out_tables) = s.all_rules();
        assert_eq!(&out_schemas, expected_schema_rules);
        assert_eq!(&out_tables, expected_table_rules);
    }

    fn test_remove(
        s: &mut TrieSelector<Rule>,
        expected_schema_rules: &mut HashMap<String, Vec<Rule>>,
        expected_table_rules: &mut HashMap<String, HashMap<String, Vec<Rule>>>,
    ) {
        let cases = remove_cases();
        let mut i = 0;
        while i < cases.len() {
            let (schema, table) = (cases[i], cases[i + 1]);
            s.remove(schema, table).unwrap();
            assert!(s.remove(schema, table).is_err());

            if table.is_empty() {
                expected_schema_rules.remove(schema);
            } else {
                let rules = expected_table_rules.get_mut(schema).unwrap();
                rules.remove(table);
            }
            i += 2;
        }

        let (out_schemas, out_tables) = s.all_rules();
        assert_eq!(&out_schemas, expected_schema_rules);
        assert_eq!(&out_tables, expected_table_rules);
    }

    // Go `TestSelector` — runs the sub-tests in order over one selector, sharing
    // the mutable expected-rule state.
    #[test]
    fn selector() {
        let mut s = TrieSelector::<Rule>::new();
        let (mut expected_schema_rules, mut expected_table_rules) = generate_expected_rules();

        test_insert(&mut s, &expected_schema_rules, &expected_table_rules);
        test_match(&mut s);
        test_append(&mut s, &mut expected_schema_rules, &expected_table_rules);
        test_replace(&mut s, &mut expected_schema_rules, &expected_table_rules);
        test_remove(
            &mut s,
            &mut expected_schema_rules,
            &mut expected_table_rules,
        );
    }
}
