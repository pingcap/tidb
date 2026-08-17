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

//! Trie-based schema/table rule selector.
//!
//! Transcreation of the whole Go package `pkg/util/table-rule-selector`
//! (`package selector`, file `trie_selector.go`). Rules are indexed by a
//! two-level wildcard trie: the first level matches the schema pattern, the
//! second level (reachable through the schema item's "next level" node)
//! matches the table pattern.
//!
//! Supported wildcard syntax, exactly as documented on the Go package:
//!
//! 1. `*` matches zero or more characters, e.g. `doc*` matches `doc` and
//!    `document` but not `dodo`; `*` must be the last character of the pattern.
//! 2. `?` matches exactly one character.
//! 3. `[...]` matches one character out of a set; `[!...]` negates the set and
//!    `a-z` inside the brackets denotes an inclusive range.
//!
//! Matching operates on raw bytes, mirroring Go's `pattern[i]` indexing, so
//! multi-byte UTF-8 characters are matched byte by byte.

use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

// Wildcard marker bytes. Go: the `asterisk` .. `rangeBetween` const block.
/// asterisk \[ * \]
const ASTERISK: u8 = b'*';
/// question mark \[ ? \]
const QUESTION: u8 = b'?';
/// rangeOpen mark \[ \[ \]
const RANGE_OPEN: u8 = b'[';
/// rangeClose mark \[ \] \]
const RANGE_CLOSE: u8 = b']';
/// rangeNot mark \[ ! \]
const RANGE_NOT: u8 = b'!';
/// rangeBetween mark \[ - \]
const RANGE_BETWEEN: u8 = b'-';

/// Go: `maxCacheNum`.
const MAX_CACHE_NUM: usize = 1024;

/// How [`Selector::insert`] treats an already existing rule at the pattern.
///
/// Go: the `Insert`/`Replace`/`Append` `int` const block.
///
/// boundary: Go declares these as plain `int` constants and
/// `trieSelector.insert` compares with `==`, so any other `int` value behaves
/// like `Append`. Every in-tree caller passes one of the three constants, so
/// the enum admits exactly those three states instead of an open integer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InsertType {
    /// Insert means insert a new rule.
    Insert,
    /// Replace means update an old rule.
    Replace,
    /// Append means append to the old rules.
    ///
    /// The Go comment on this constant reads "Append means delete an old rule",
    /// which contradicts the implementation; the implementation appends.
    Append,
}

/// Errors returned by the selector.
///
/// Go builds these with `github.com/pingcap/errors` helpers; the `Display`
/// text reproduces the message those helpers render.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SelectorError {
    /// Go: `errors.Errorf("schema pattern %s or rule %v can't be empty", ...)`.
    EmptySchemaOrRule(String),
    /// Go: `errors.NotValidf`.
    NotValid(String),
    /// Go: `errors.AlreadyExistsf`.
    AlreadyExists(String),
    /// Go: `errors.NotFoundf`.
    NotFound(String),
    /// Go: `errors.Annotate` / `errors.Annotatef`.
    Annotated(String, Box<SelectorError>),
}

impl fmt::Display for SelectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectorError::EmptySchemaOrRule(msg) => write!(f, "{msg}"),
            SelectorError::NotValid(what) => write!(f, "{what} not valid"),
            SelectorError::AlreadyExists(what) => write!(f, "{what} already exists"),
            SelectorError::NotFound(what) => write!(f, "{what} not found"),
            SelectorError::Annotated(ctx, inner) => write!(f, "{ctx}: {inner}"),
        }
    }
}

impl std::error::Error for SelectorError {}

impl SelectorError {
    fn annotate(self, ctx: impl Into<String>) -> SelectorError {
        SelectorError::Annotated(ctx.into(), Box::new(self))
    }
}

/// A set of rules that got selected.
///
/// Go: `type RuleSet []any`. Go distinguishes a `nil` `RuleSet` from an empty
/// one and [`Selector::match_rules`] returns `nil` whenever nothing was
/// appended, so the Rust side models `nil` as `None` and any non-empty result
/// as `Some`. Both representations agree because Go's `append` only turns a
/// `nil` slice into a non-`nil` one when at least one element is appended.
///
/// Go's `(RuleSet).clone` (nil in, nil out; otherwise a fresh slice with the
/// same elements) is exactly `Option<Vec<R>>::clone`, so it has no separate
/// counterpart here.
///
/// boundary: Go stores `any` rules, so one selector can hold heterogeneous
/// rule types. This port is generic in `R`, i.e. one selector holds one rule
/// type. Every in-tree caller (`pkg/util/filter`, `pkg/util/column-mapping`,
/// `pkg/util/regexpr-router`) stores a single rule type per selector.
pub type RuleSet<R> = Vec<R>;

/// Rules stored for a schema pattern, keyed by pattern. Go: the first return
/// value of `Selector.AllRules`.
pub type SchemaRules<R> = HashMap<String, Vec<R>>;
/// Rules stored for a table pattern, keyed by schema pattern then table
/// pattern. Go: the second return value of `Selector.AllRules`.
pub type TableRules<R> = HashMap<String, HashMap<String, Vec<R>>>;

/// Stores rules of schema/table for easy retrieval. Go: `type Selector interface`.
pub trait Selector<R> {
    /// Inserts one rule into the trie. If `table` is empty the rule is inserted
    /// at schema level, otherwise at table level.
    ///
    /// Go: `Selector.Insert`. `rule` is `Option<R>` because Go accepts an `any`
    /// that may be `nil` and rejects it with an error.
    fn insert(
        &self,
        schema: &str,
        table: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<(), SelectorError>;

    /// Returns all matched rules.
    ///
    /// boundary: named `match_rules` because `match` is a Rust keyword; stands
    /// for Go `Selector.Match`.
    fn match_rules(&self, schema: &str, table: &str) -> Option<RuleSet<R>>;

    /// Removes one rule. Go: `Selector.Remove`.
    fn remove(&self, schema: &str, table: &str) -> Result<(), SelectorError>;

    /// Returns all rules. Go: `Selector.AllRules`.
    fn all_rules(&self) -> (SchemaRules<R>, TableRules<R>);
}

type NodeId = usize;
type ItemId = usize;

/// Go: `type node struct`.
#[derive(Default)]
struct Node {
    characters: HashMap<u8, ItemId>,
    asterisk: Option<ItemId>,
    question: Option<ItemId>,
    r_items: Vec<ItemId>,
}

/// Go: `type ran struct`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Ran {
    start: u8,
    end: u8,
    has_between: bool,
}

/// Go: the range-specific part of `type rangeItem struct`.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
struct RangeSpec {
    has_not: bool,
    ranges: Vec<Ran>,
}

impl RangeSpec {
    /// Go: `(*rangeItem).equal`.
    fn equal(&self, other: &RangeSpec) -> bool {
        self.matches(other) && other.matches(self)
    }

    /// Go: `(*rangeItem).match`.
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

    /// Go: `(*rangeItem).matchChar`.
    fn match_char(&self, c: u8) -> bool {
        for r in &self.ranges {
            if r.start <= c && c <= r.end {
                return !self.has_not;
            }
        }
        self.has_not
    }

    /// Go: `(*rangeItem).str`.
    fn str(&self) -> String {
        let mut ret = Vec::new();
        ret.push(RANGE_OPEN);
        if self.has_not {
            ret.push(RANGE_NOT);
        }
        for r in &self.ranges {
            if r.has_between {
                ret.push(r.start);
                ret.push(RANGE_BETWEEN);
                ret.push(r.end);
            } else {
                ret.push(r.start);
            }
        }
        ret.push(RANGE_CLOSE);
        // Go builds the pattern by concatenating `string([]byte{...})`, which
        // keeps raw bytes; the trie only ever stores bytes taken from the
        // original pattern, so lossy conversion preserves them here as well.
        String::from_utf8_lossy(&ret).into_owned()
    }
}

/// Go: `type baseItem struct` plus the extra fields of `type rangeItem struct`.
/// The Go code models these as an `item` interface with `baseItem` embedded in
/// `rangeItem`; a single struct with an optional range carries the same state.
struct Item<R> {
    /// Go: `baseItem.ch`.
    child: Option<NodeId>,
    /// Go: `baseItem.rule`. `None` is Go's `nil` slice, which the code tests
    /// against to detect "no rule stored here".
    rule: Option<Vec<R>>,
    /// Go: `baseItem.nextLevel`, the schema level -> table level link.
    next_level: Option<NodeId>,
    /// `Some` iff this item is a Go `*rangeItem`.
    range: Option<RangeSpec>,
}

impl<R> Default for Item<R> {
    fn default() -> Self {
        Item {
            child: None,
            rule: None,
            next_level: None,
            range: None,
        }
    }
}

/// Go: `type matchedResult struct`.
struct MatchedResult<R> {
    nodes: Vec<NodeId>,
    rules: Vec<R>,
}

impl<R> MatchedResult<R> {
    /// Go: `(*matchedResult).empty`. The Go receiver may itself be `nil`; every
    /// call site passes a freshly allocated value, so only the field test
    /// remains.
    fn empty(&self) -> bool {
        self.nodes.is_empty() && self.rules.is_empty()
    }
}

/// Trie state guarded by the selector's lock.
///
/// The Go trie is a graph of `*node`/`item` pointers. Rust stores the same
/// graph in two arenas indexed by [`NodeId`]/[`ItemId`], which reproduces the
/// aliasing the Go pointers provide (an item's `nextLevel` node is reachable
/// from a matched result while the trie is still mutable).
struct Trie<R> {
    nodes: Vec<Node>,
    items: Vec<Item<R>>,
    root: NodeId,
    cache: HashMap<String, Option<RuleSet<R>>>,
}

/// Go: `type trieSelector struct`.
pub struct TrieSelector<R> {
    // Go embeds `sync.RWMutex`; `Match` takes the read lock only for the cache
    // probe and the write lock for the rest, which this port reproduces.
    trie: RwLock<Trie<R>>,
}

impl<R: Clone> Default for TrieSelector<R> {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns a trie [`Selector`]. Go: `NewTrieSelector`.
pub fn new_trie_selector<R: Clone>() -> TrieSelector<R> {
    TrieSelector::new()
}

impl<R: Clone> TrieSelector<R> {
    /// Returns a trie selector with an empty root node and empty cache.
    pub fn new() -> Self {
        TrieSelector {
            trie: RwLock::new(Trie {
                nodes: vec![Node::default()],
                items: Vec::new(),
                root: 0,
                cache: HashMap::new(),
            }),
        }
    }
}

impl<R: Clone> Trie<R> {
    /// Go: `newNode`.
    fn new_node(&mut self) -> NodeId {
        self.nodes.push(Node::default());
        self.nodes.len() - 1
    }

    fn new_item(&mut self) -> ItemId {
        self.items.push(Item::default());
        self.items.len() - 1
    }

    /// Go: `(*trieSelector).insertSchema`.
    fn insert_schema(
        &mut self,
        schema: &str,
        rule: R,
        insert_type: InsertType,
    ) -> Result<(), SelectorError> {
        self.insert(self.root, schema, Some(rule), insert_type)
            .map_err(|err| err.annotate("insert into schema selector"))?;
        Ok(())
    }

    /// Go: `(*trieSelector).insertTable`.
    fn insert_table(
        &mut self,
        schema: &str,
        table: &str,
        rule: R,
        insert_type: InsertType,
    ) -> Result<(), SelectorError> {
        let schema_entity = self
            .insert(self.root, schema, None, InsertType::Insert)
            .map_err(|err| err.annotate("insert into schema selector"))?;
        // Go dereferences the returned `item` unconditionally; it is only `nil`
        // for an empty pattern, which `Insert` already rejects.
        let schema_entity = schema_entity.expect("schema pattern is not empty");

        if self.items[schema_entity].next_level.is_none() {
            let next = self.new_node();
            self.items[schema_entity].next_level = Some(next);
        }
        let next_level = self.items[schema_entity].next_level.unwrap();

        self.insert(next_level, table, Some(rule), insert_type)
            .map_err(|err| err.annotate("insert into table selector"))?;
        Ok(())
    }

    /// Go: `(*trieSelector).insert`. Returns the trie item the pattern ends at;
    /// `None` reproduces Go's `nil` return for an empty pattern.
    fn insert(
        &mut self,
        root: NodeId,
        pattern: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<Option<ItemId>, SelectorError> {
        let p = pattern.as_bytes();
        let mut n = root;
        let mut had_asterisk = false;
        let mut entity: Option<ItemId> = None;

        let mut i = 0usize;
        while i < p.len() {
            if had_asterisk {
                // Note: the nodes created for the characters before this point
                // stay in the trie even though the insert fails, exactly as in
                // Go, and are observable through `AllRules`.
                return Err(SelectorError::NotValid(format!("pattern {pattern}")));
            }

            let mut r_item: Option<RangeSpec> = None;
            // Go declares `var nextI int`, so it is 0 for every branch that does
            // not parse a range; `i += nextI` is then a no-op.
            let mut next_i: isize = 0;
            match p[i] {
                ASTERISK => {
                    entity = self.nodes[n].asterisk;
                    had_asterisk = true;
                }
                QUESTION => {
                    entity = self.nodes[n].question;
                }
                RANGE_OPEN => {
                    let (parsed, ni) = get_range_item(&p[i..]);
                    r_item = parsed;
                    next_i = ni;
                    if ni == -1 {
                        entity = self.nodes[n].characters.get(&p[i]).copied();
                    } else {
                        let spec = r_item.as_ref().expect("range parsed when next_i != -1");
                        entity = None;
                        for &nr in &self.nodes[n].r_items {
                            let existing = self.items[nr]
                                .range
                                .as_ref()
                                .expect("rItems only holds range items");
                            if existing.equal(spec) {
                                entity = Some(nr);
                                break;
                            }
                        }
                    }
                }
                c => {
                    entity = self.nodes[n].characters.get(&c).copied();
                }
            }

            if entity.is_none() {
                let id = self.new_item();
                match p[i] {
                    ASTERISK => self.nodes[n].asterisk = Some(id),
                    QUESTION => self.nodes[n].question = Some(id),
                    RANGE_OPEN => {
                        if next_i == -1 {
                            self.nodes[n].characters.insert(p[i], id);
                        } else {
                            self.items[id].range = r_item.take();
                            self.nodes[n].r_items.push(id);
                        }
                    }
                    c => {
                        self.nodes[n].characters.insert(c, id);
                    }
                }
                entity = Some(id);
            }

            let e = entity.expect("entity is set above");
            if self.items[e].child.is_none() {
                let child = self.new_node();
                self.items[e].child = Some(child);
            }
            n = self.items[e].child.expect("child is set above");
            if next_i != -1 {
                i += next_i as usize;
            }
            i += 1;
        }

        if let Some(rule) = rule {
            // Go dereferences `entity` here; for an empty pattern it is `nil`
            // and Go panics. `Insert` rejects an empty schema and only calls
            // `insertTable` with a non-empty table, so this cannot be reached.
            let e = entity.expect("pattern is not empty when a rule is inserted");
            if insert_type == InsertType::Insert && self.items[e].rule.is_some() {
                return Err(SelectorError::AlreadyExists(format!("pattern {pattern}")));
            }
            if insert_type == InsertType::Replace {
                // Go: `setRule(rule)` on a variadic setter, i.e. a one element slice.
                self.items[e].rule = Some(vec![rule]);
            } else {
                self.items[e].rule.get_or_insert_with(Vec::new).push(rule);
            }
            self.clear_cache();
        }

        Ok(entity)
    }

    /// Go: `(*trieSelector).track`.
    fn track(&self, n0: NodeId, pattern: &str) -> Result<Vec<ItemId>, SelectorError> {
        let p = pattern.as_bytes();
        let mut items: Vec<ItemId> = Vec::with_capacity(p.len());
        let mut n = n0;

        let mut i = 0usize;
        while i < p.len() {
            match p[i] {
                ASTERISK => {
                    let Some(a) = self.nodes[n].asterisk else {
                        return Err(SelectorError::NotFound(format!("pattern {pattern}")));
                    };
                    if i != p.len() - 1 {
                        return Err(SelectorError::NotValid(format!("pattern {pattern} ")));
                    }
                    items.push(a);
                }
                QUESTION => {
                    let Some(q) = self.nodes[n].question else {
                        return Err(SelectorError::NotFound(format!("pattern {pattern}")));
                    };
                    items.push(q);
                    n = self.items[q].child.expect("trie items always have a child");
                }
                RANGE_OPEN => {
                    let (r_item, next_i) = get_range_item(&p[i..]);
                    if next_i == -1 {
                        let Some(&item) = self.nodes[n].characters.get(&p[i]) else {
                            return Err(SelectorError::NotFound(format!("pattern {pattern}")));
                        };
                        items.push(item);
                        n = self.items[item]
                            .child
                            .expect("trie items always have a child");
                    } else {
                        let spec = r_item.expect("range parsed when next_i != -1");
                        let mut match_idx: Option<ItemId> = None;
                        for &candidate in &self.nodes[n].r_items {
                            let existing = self.items[candidate]
                                .range
                                .as_ref()
                                .expect("rItems only holds range items");
                            if existing.equal(&spec) {
                                match_idx = Some(candidate);
                                break;
                            }
                        }
                        let Some(item) = match_idx else {
                            return Err(SelectorError::NotFound(format!("pattern {pattern}")));
                        };
                        items.push(item);
                        n = self.items[item]
                            .child
                            .expect("trie items always have a child");
                        i += next_i as usize;
                    }
                }
                c => {
                    let Some(&item) = self.nodes[n].characters.get(&c) else {
                        return Err(SelectorError::NotFound(format!("pattern {pattern}")));
                    };
                    items.push(item);
                    n = self.items[item]
                        .child
                        .expect("trie items always have a child");
                }
            }
            i += 1;
        }

        Ok(items)
    }

    /// Go: `(*trieSelector).matchNode`.
    fn match_node(&self, start: Option<NodeId>, s: &[u8], mr: &mut MatchedResult<R>) {
        let Some(mut n) = start else {
            return;
        };

        let mut entity: Option<ItemId> = None;
        for i in 0..s.len() {
            let node = &self.nodes[n];
            if let Some(a) = node.asterisk {
                self.append_matched_item(a, mr);
            }

            if let Some(q) = node.question {
                if i == s.len() - 1 {
                    self.append_matched_item(q, mr);
                }
                self.match_node(self.items[q].child, &s[i + 1..], mr);
            }

            for &r in &node.r_items {
                let spec = self.items[r]
                    .range
                    .as_ref()
                    .expect("rItems only holds range items");
                if spec.match_char(s[i]) {
                    if i == s.len() - 1 {
                        self.append_matched_item(r, mr);
                    }
                    self.match_node(self.items[r].child, &s[i + 1..], mr);
                }
            }

            match node.characters.get(&s[i]) {
                None => return,
                Some(&e) => {
                    entity = Some(e);
                    n = self.items[e].child.expect("trie items always have a child");
                }
            }
        }

        if let Some(e) = entity {
            self.append_matched_item(e, mr);
        }

        if let Some(a) = self.nodes[n].asterisk {
            self.append_matched_item(a, mr);
        }
    }

    /// Go: `appendMatchedItem`.
    fn append_matched_item(&self, entity: ItemId, mr: &mut MatchedResult<R>) {
        if let Some(rules) = &self.items[entity].rule {
            mr.rules.extend(rules.iter().cloned());
        }
        if let Some(next) = self.items[entity].next_level {
            mr.nodes.push(next);
        }
    }

    /// Go: `(*trieSelector).travel`.
    fn travel(
        &self,
        n: Option<NodeId>,
        word: &[u8],
        rules: &mut HashMap<String, Vec<R>>,
        nodes: &mut Option<HashMap<String, NodeId>>,
    ) {
        let Some(n) = n else {
            return;
        };
        let node = &self.nodes[n];

        if let Some(a) = node.asterisk {
            let pattern = extend(word, &[ASTERISK]);
            self.insert_matched_item_into_map(&pattern, a, rules, nodes.as_mut());
        }

        if let Some(q) = node.question {
            let pattern = extend(word, &[QUESTION]);
            self.insert_matched_item_into_map(&pattern, q, rules, nodes.as_mut());
            self.travel(self.items[q].child, &pattern, rules, nodes);
        }

        for &r in &node.r_items {
            let spec = self.items[r]
                .range
                .as_ref()
                .expect("rItems only holds range items");
            let pattern = extend(word, spec.str().as_bytes());
            self.insert_matched_item_into_map(&pattern, r, rules, nodes.as_mut());
            self.travel(self.items[r].child, &pattern, rules, nodes);
        }

        for (&ch, &item) in &node.characters {
            let pattern = extend(word, &[ch]);
            // Go guards `if baseItem != nil`; a map lookup for a stored key
            // never yields a nil item here, the guard is defensive.
            self.insert_matched_item_into_map(&pattern, item, rules, nodes.as_mut());
            self.travel(self.items[item].child, &pattern, rules, nodes);
        }
    }

    /// Go: `insertMatchedItemIntoMap`. Go also guards `rules != nil`, but both
    /// call sites pass a non-nil map, so only the `nodes` map is optional here.
    fn insert_matched_item_into_map(
        &self,
        pattern: &[u8],
        entity: ItemId,
        rules: &mut HashMap<String, Vec<R>>,
        nodes: Option<&mut HashMap<String, NodeId>>,
    ) {
        if let Some(rule) = &self.items[entity].rule {
            rules.insert(String::from_utf8_lossy(pattern).into_owned(), rule.clone());
        }
        if let (Some(nodes), Some(next)) = (nodes, self.items[entity].next_level) {
            nodes.insert(String::from_utf8_lossy(pattern).into_owned(), next);
        }
    }

    /// Go: `(*trieSelector).addToCache`.
    fn add_to_cache(&mut self, key: String, rules: Option<RuleSet<R>>) {
        self.cache.insert(key, rules);
        if self.cache.len() > MAX_CACHE_NUM {
            // Go deletes the first key produced by map iteration, i.e. an
            // arbitrary one.
            if let Some(literal) = self.cache.keys().next().cloned() {
                self.cache.remove(&literal);
            }
        }
    }

    /// Go: `(*trieSelector).clearCache`.
    fn clear_cache(&mut self) {
        self.cache = HashMap::new();
    }
}

/// Go's `append(word, x...)` on a slice that the caller keeps using. The Go
/// code relies on the appended value being converted to a string immediately
/// and on depth-first traversal finishing before a sibling reuses the backing
/// array, so an owned copy per branch is equivalent.
fn extend(word: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(word.len() + suffix.len());
    out.extend_from_slice(word);
    out.extend_from_slice(suffix);
    out
}

/// Go: `(*trieSelector).getRangeItem`. `pattern` starts at the `[` byte.
/// Returns the parsed range and the index of the closing `]`, or `-1` when the
/// bracket is never closed (in which case the `[` is treated as a literal).
fn get_range_item(pattern: &[u8]) -> (Option<RangeSpec>, isize) {
    let mut next_i: isize = -1;
    for (i, &b) in pattern.iter().enumerate() {
        if b == RANGE_CLOSE {
            next_i = i as isize;
            break;
        }
    }
    if next_i == -1 {
        return (None, next_i);
    }
    let next_i_usize = next_i as usize;
    let mut item = RangeSpec::default();
    let mut start_i = 1usize;
    // Go indexes `pattern[1]` unconditionally; the caller only enters this
    // function on a `[`, so `]` is at index 1 or later and the byte exists.
    if pattern[start_i] == RANGE_NOT {
        start_i += 1;
        item.has_not = true;
    }
    let mut i = start_i;
    while i < next_i_usize {
        if pattern[i + 1] == RANGE_BETWEEN && i + 2 < next_i_usize {
            item.ranges.push(Ran {
                start: pattern[i],
                end: pattern[i + 2],
                has_between: true,
            });
            i += 2;
        } else {
            item.ranges.push(Ran {
                start: pattern[i],
                end: pattern[i],
                has_between: false,
            });
        }
        i += 1;
    }
    // Change the `[!]` to `[\!-\!]`.
    if item.ranges.is_empty() && item.has_not {
        item.has_not = false;
        item.ranges.push(Ran {
            start: RANGE_NOT,
            end: RANGE_NOT,
            has_between: false,
        });
    }
    (Some(item), next_i)
}

/// Go: `quoteSchemaTable`.
fn quote_schema_table(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        return String::new();
    }
    if !table.is_empty() {
        return format!("`{schema}`.`{table}`");
    }
    format!("`{schema}`")
}

impl<R: Clone> Selector<R> for TrieSelector<R> {
    fn insert(
        &self,
        schema: &str,
        table: &str,
        rule: Option<R>,
        insert_type: InsertType,
    ) -> Result<(), SelectorError> {
        if schema.is_empty() || rule.is_none() {
            // boundary: Go renders the offending rule with `%v` in
            // `errors.Errorf("schema pattern %s or rule %v can't be empty", ...)`.
            // `R` carries no formatting bound here, so the rule value is left
            // out of the message rather than constraining every rule type.
            return Err(SelectorError::EmptySchemaOrRule(format!(
                "schema pattern {schema} or rule can't be empty"
            )));
        }
        let rule = rule.expect("rule is Some after the guard above");

        let mut trie = self.trie.write().expect("selector lock poisoned");
        if table.is_empty() {
            trie.insert_schema(schema, rule, insert_type)
        } else {
            trie.insert_table(schema, table, rule, insert_type)
        }
    }

    fn match_rules(&self, schema: &str, table: &str) -> Option<RuleSet<R>> {
        let cache_key = quote_schema_table(schema, table);
        {
            let trie = self.trie.read().expect("selector lock poisoned");
            if let Some(rules) = trie.cache.get(&cache_key) {
                return rules.clone();
            }
        }

        let mut trie = self.trie.write().expect("selector lock poisoned");
        let mut matched_schema_result = MatchedResult {
            nodes: Vec::with_capacity(4),
            rules: Vec::with_capacity(4),
        };
        let root = trie.root;
        trie.match_node(Some(root), schema.as_bytes(), &mut matched_schema_result);

        // not found matched rules in schema level
        if matched_schema_result.empty() {
            trie.add_to_cache(cache_key, None);
            return None;
        }

        // Go starts from a nil `RuleSet` and only appends, so the result stays
        // nil unless at least one rule is found.
        let mut rules: Vec<R> = Vec::new();
        rules.extend(matched_schema_result.rules.iter().cloned());

        for &si in &matched_schema_result.nodes {
            let mut matched_table_result = MatchedResult {
                nodes: Vec::new(),
                rules: Vec::with_capacity(4),
            };
            trie.match_node(Some(si), table.as_bytes(), &mut matched_table_result);
            rules.extend(matched_table_result.rules.iter().cloned());
        }

        // not found matched rule in table level, return matched rule in schema level
        let rules = if rules.is_empty() { None } else { Some(rules) };
        trie.add_to_cache(cache_key, rules.clone());
        rules
    }

    fn remove(&self, schema: &str, table: &str) -> Result<(), SelectorError> {
        let mut trie = self.trie.write().expect("selector lock poisoned");

        if schema.is_empty() {
            return Err(SelectorError::NotValid(format!(
                "schema/table {schema}/{table}"
            )));
        }

        let root = trie.root;
        let schema_items = trie.track(root, schema).map_err(|err| {
            err.annotate(format!(
                "track schema/table {schema}/{table} in schema level"
            ))
        })?;

        let schema_leaf_item = *schema_items
            .last()
            .expect("schema pattern is not empty, so track yields at least one item");
        if !table.is_empty() {
            let Some(next_level) = trie.items[schema_leaf_item].next_level else {
                return Err(SelectorError::NotFound(format!(
                    "table level while we track chema/table {schema}/{table}"
                )));
            };

            let table_items = trie.track(next_level, table).map_err(|err| {
                err.annotate(format!(
                    "track schema/table {schema}/{table} in table level"
                ))
            })?;

            let table_leaf_item = *table_items
                .last()
                .expect("table pattern is not empty, so track yields at least one item");
            if trie.items[table_leaf_item].rule.is_none() {
                return Err(SelectorError::NotFound(format!(
                    "schema/table {schema}/{table} in table level"
                )));
            }

            // remove table level nodes
            trie.items[table_leaf_item].rule = None;
            trie.clear_cache();
            return Ok(());
        }

        if trie.items[schema_leaf_item].rule.is_none() {
            return Err(SelectorError::NotFound(format!(
                "schema/table {schema}/{table} in schema level"
            )));
        }

        trie.items[schema_leaf_item].rule = None;
        trie.clear_cache();
        Ok(())
    }

    fn all_rules(&self) -> (SchemaRules<R>, TableRules<R>) {
        let mut table_rules: TableRules<R> = HashMap::new();
        let mut schema_nodes: Option<HashMap<String, NodeId>> = Some(HashMap::new());
        let mut schema_rules: SchemaRules<R> = HashMap::new();
        let word: Vec<u8> = Vec::new();

        let trie = self.trie.read().expect("selector lock poisoned");
        let root = trie.root;
        trie.travel(Some(root), &word, &mut schema_rules, &mut schema_nodes);

        let schema_nodes = schema_nodes.expect("schema_nodes stays Some");
        for (schema, &n) in &schema_nodes {
            let mut rules = table_rules.remove(schema).unwrap_or_default();
            let mut no_nodes: Option<HashMap<String, NodeId>> = None;
            trie.travel(Some(n), &[], &mut rules, &mut no_nodes);
            if !rules.is_empty() {
                table_rules.insert(schema.clone(), rules);
            }
        }
        (schema_rules, table_rules)
    }
}

#[cfg(test)]
impl<R: Clone> TrieSelector<R> {
    /// Snapshot of the internal match cache. Go's test reaches into
    /// `trieSelector.cache` directly after type-asserting the interface.
    fn cache_snapshot(&self) -> HashMap<String, Option<RuleSet<R>>> {
        self.trie
            .read()
            .expect("selector lock poisoned")
            .cache
            .clone()
    }
}

/// Transcreated from `pkg/util/table-rule-selector/selector_test.go`, the only
/// upstream coverage of this package (`TestSelector` plus its five helpers).
#[cfg(test)]
mod tests {
    use super::*;

    /// Go: `type dummyRule struct`. The Go test compares rules by pointer
    /// through `reflect.DeepEqual`, which compares the pointed-to values.
    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct DummyRule {
        description: String,
    }

    impl DummyRule {
        fn new(description: impl Into<String>) -> Self {
            DummyRule {
                description: description.into(),
            }
        }
    }

    /// Go: `testSelectorSuite.tables`.
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
            // [] match nothing.
            ("[]*", vec!["*"]),
        ]
    }

    /// Go: `testSelectorSuite.matchCase`. Tuple layout is
    /// `(schema, table, matchedNum, matchedRules)` where `matchedRules` holds
    /// schema/table pattern pairs.
    fn match_cases() -> Vec<(&'static str, &'static str, usize, Vec<&'static str>)> {
        vec![
            // test one level
            ("dbc", "t1_abc", 2, vec!["?bc", "t1_ab?", "?bc", "t1_abc"]),
            ("adc", "t2_abc", 2, vec!["a?c", "t2_ab*", "a?c", "t2_abc"]),
            ("abd", "t3_abc", 2, vec!["ab?", "t3_ab*", "ab?", "t3_ab?"]),
            ("abc", "t4_abc", 2, vec!["ab*", "t4_abc", "ab*", "t4_abc*"]),
            (
                "abc",
                "abc",
                4,
                vec!["?bc", "abc*", "ab*", "ab*", "ab?", "ab?", "abc", "abc"],
            ),
            // test only schema rule
            ("schema1", "xxx", 1, vec!["schema*", ""]),
            ("schema1", "", 1, vec!["schema*", ""]),
            // test table rule
            (
                "schema1",
                "test1",
                2,
                vec!["schema*", "", "schema*", "test*"],
            ),
            ("t1", "test1", 1, vec!["t*", "test*"]),
            ("schema1", "abc1", 2, vec!["schema*", "", "schema*", "abc*"]),
            ("ikj", "ikb", 1, vec!["ik[hjkl]", "ik[!zxc]"]),
            (
                "ikh",
                "iky",
                2,
                vec!["ik[hjkl]", "ik[!zxc]", "ik[f-h]", "ik[!a-ce-g]"],
            ),
            (
                "iz3",
                "ixz",
                2,
                vec!["i[x-z][1-3]", "i?[x-z]", "i[x-z][1-3]", "ix*"],
            ),
            ("!", "-", 2, vec!["[!]", "[a-]", "[!]", "[a-c-f]"]),
            ("!", "c", 1, vec!["[!]", "[a-c-f]"]),
            ("d", "zxcv", 1, vec!["[!a-c!f-g]", "*"]),
        ]
    }

    /// Go: `testSelectorSuite.removeCases`, schema/table pairs.
    const REMOVE_CASES: [&str; 8] = [
        "schema*",
        "",
        "a?c",
        "t2_ab*",
        "i[x-z][1-3]",
        "i?[x-z]",
        "[!]",
        "[a-c-f]",
    ];

    /// Go: `testGenerateExpectedRules`.
    fn generate_expected_rules() -> (SchemaRules<DummyRule>, TableRules<DummyRule>) {
        let mut schema_rules: SchemaRules<DummyRule> = HashMap::new();
        let mut table_rules: TableRules<DummyRule> = HashMap::new();
        for (schema, tbls) in tables() {
            table_rules.entry(schema.to_string()).or_default();
            for table in tbls {
                if table.is_empty() {
                    schema_rules.insert(
                        schema.to_string(),
                        vec![DummyRule::new(quote_schema_table(schema, ""))],
                    );
                } else {
                    table_rules
                        .get_mut(schema)
                        .expect("entry created above")
                        .insert(
                            table.to_string(),
                            vec![DummyRule::new(quote_schema_table(schema, table))],
                        );
                }
            }
        }
        (schema_rules, table_rules)
    }

    /// Go: `testInsert`.
    fn run_insert(
        s: &TrieSelector<DummyRule>,
        schema_rules: &SchemaRules<DummyRule>,
        table_rules: &TableRules<DummyRule>,
    ) {
        for (schema, rules) in schema_rules {
            s.insert(schema, "", Some(rules[0].clone()), InsertType::Insert)
                .expect("first schema insert succeeds");
            // test duplicate error
            assert!(s
                .insert(schema, "", Some(rules[0].clone()), InsertType::Insert)
                .is_err());
            // test simple replace
            s.insert(schema, "", Some(rules[0].clone()), InsertType::Replace)
                .expect("replace succeeds");
        }

        for (schema, tbls) in table_rules {
            for (table, rules) in tbls {
                s.insert(schema, table, Some(rules[0].clone()), InsertType::Insert)
                    .expect("first table insert succeeds");
                // test duplicate error
                assert!(s
                    .insert(schema, table, Some(rules[0].clone()), InsertType::Insert)
                    .is_err());
                // test simple replace
                s.insert(schema, table, Some(rules[0].clone()), InsertType::Replace)
                    .expect("replace succeeds");
            }
        }

        // insert wrong pattern
        // rule can't be nil
        assert!(s.insert("schema", "", None, InsertType::Replace).is_err());
        // asterisk must be the last character of pattern
        assert!(s
            .insert(
                "ab**",
                "",
                Some(DummyRule::new("error")),
                InsertType::Replace
            )
            .is_err());
        assert!(s
            .insert(
                "abcd",
                "ab**",
                Some(DummyRule::new("error")),
                InsertType::Replace
            )
            .is_err());

        let (schemas, tbls) = s.all_rules();
        assert_eq!(&schemas, schema_rules);
        assert_eq!(&tbls, table_rules);
    }

    /// Go: `testMatch`.
    fn run_match(s: &TrieSelector<DummyRule>) {
        let mut cache: HashMap<String, Option<RuleSet<DummyRule>>> = HashMap::new();
        for (schema, table, matched_num, matched_rules) in match_cases() {
            let mut rules = s
                .match_rules(schema, table)
                .unwrap_or_else(|| panic!("{schema}/{table} must match"));
            let mut expected_rules: Vec<DummyRule> = (0..matched_num)
                .map(|i| {
                    DummyRule::new(quote_schema_table(
                        matched_rules[2 * i],
                        matched_rules[2 * i + 1],
                    ))
                })
                .collect();
            expected_rules.sort();
            rules.sort();
            assert_eq!(rules, expected_rules, "match {schema}/{table}");
            cache.insert(quote_schema_table(schema, table), Some(expected_rules));
        }

        // test cache
        let mut trie_cache = s.cache_snapshot();
        for cache_item in trie_cache.values_mut().flatten() {
            cache_item.sort();
        }
        assert_eq!(trie_cache, cache);

        // test not matched
        let rule = s.match_rules("t1", "");
        assert!(rule.is_none());
        cache.insert(quote_schema_table("t1", ""), rule);

        let rule = s.match_rules("t1", "abc");
        assert!(rule.is_none());
        cache.insert(quote_schema_table("t1", "abc"), rule);

        let rule = s.match_rules("xxx", "abc");
        assert!(rule.is_none());
        cache.insert(quote_schema_table("xxx", "abc"), rule);

        let mut trie_cache = s.cache_snapshot();
        for cache_item in trie_cache.values_mut().flatten() {
            cache_item.sort();
        }
        assert_eq!(trie_cache, cache);
    }

    /// Go: `testAppend`.
    fn run_append(
        s: &TrieSelector<DummyRule>,
        schema_rules: &mut SchemaRules<DummyRule>,
        table_rules: &TableRules<DummyRule>,
    ) {
        let appended_rule = DummyRule::new("append");
        for (schema, rules) in schema_rules.iter_mut() {
            rules.push(appended_rule.clone());
            s.insert(schema, "", Some(appended_rule.clone()), InsertType::Append)
                .expect("append succeeds");
        }
        let (schemas, tbls) = s.all_rules();
        assert_eq!(&schemas, &*schema_rules);
        assert_eq!(&tbls, table_rules);
    }

    /// Go: `testReplace`.
    fn run_replace(
        s: &TrieSelector<DummyRule>,
        schema_rules: &mut SchemaRules<DummyRule>,
        table_rules: &TableRules<DummyRule>,
    ) {
        let replaced_rule = DummyRule::new("replace");
        for (schema, rules) in schema_rules.iter_mut() {
            *rules = vec![replaced_rule.clone()];
            // to prevent it doesn't exist
            s.insert(schema, "", Some(replaced_rule.clone()), InsertType::Replace)
                .expect("replace succeeds");
            // test replace
            s.insert(schema, "", Some(replaced_rule.clone()), InsertType::Replace)
                .expect("replace succeeds");
            assert!(s
                .insert(schema, "", Some(replaced_rule.clone()), InsertType::Insert)
                .is_err());
        }

        let (schemas, tbls) = s.all_rules();
        assert_eq!(&schemas, &*schema_rules);
        assert_eq!(&tbls, table_rules);
    }

    /// Go: `testRemove`.
    fn run_remove(
        s: &TrieSelector<DummyRule>,
        schema_rules: &mut SchemaRules<DummyRule>,
        table_rules: &mut TableRules<DummyRule>,
    ) {
        for pair in REMOVE_CASES.chunks(2) {
            let (schema, table) = (pair[0], pair[1]);
            s.remove(schema, table).expect("first remove succeeds");
            assert!(s.remove(schema, table).is_err());

            if table.is_empty() {
                schema_rules.remove(schema);
            } else {
                let rules = table_rules
                    .get_mut(schema)
                    .expect("schema exists in expected table rules");
                rules.remove(table);
            }
        }

        let (schemas, tbls) = s.all_rules();
        assert_eq!(&schemas, &*schema_rules);
        assert_eq!(&tbls, &*table_rules);
    }

    /// Go: `TestSelector`.
    #[test]
    fn test_selector() {
        let s: TrieSelector<DummyRule> = new_trie_selector();
        let (mut expected_schema_rules, mut expected_table_rules) = generate_expected_rules();

        run_insert(&s, &expected_schema_rules, &expected_table_rules);
        run_match(&s);
        run_append(&s, &mut expected_schema_rules, &expected_table_rules);
        run_replace(&s, &mut expected_schema_rules, &expected_table_rules);
        run_remove(&s, &mut expected_schema_rules, &mut expected_table_rules);
    }
}
