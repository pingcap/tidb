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

//! Go `br/pkg/restore/utils/rewrite_rule.go`: the prefix rewrites that map a
//! backup's table and index key space onto the IDs the restore target
//! allocated.

use std::collections::BTreeMap;

use tidb_codec::table_key::{encode_table_index_prefix, encode_table_prefix};
use tidb_codec::{decode_bytes, decode_table_id, encode_bytes, gen_table_record_prefix};
use tidb_model::TableInfo;
use tidb_util::redact;

use super::misc::{get_index_id_map, get_table_id_map, DEFAULT_CF_NAME, WRITE_CF_NAME};
use super::proto::{File, RewriteRule};
use crate::rtree::{Range, RangeFile};

/// boundary: the `br/pkg/errors` sentinels this package raises.
///
/// `errors.Cause(err) == berrors.X` becomes [`RestoreError::kind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreErrorKind {
    /// Go `berrors.ErrInvalidRange`.
    InvalidRange,
    /// Go `berrors.ErrRestoreTableIDMismatch`.
    RestoreTableIdMismatch,
    /// Go `berrors.ErrRestoreInvalidRewrite`.
    RestoreInvalidRewrite,
    /// Go `berrors.ErrRestoreInvalidBackup`.
    RestoreInvalidBackup,
    /// A bare `errors.Errorf` with no normalized cause.
    Plain,
}

impl RestoreErrorKind {
    /// The normalized message `errors.Normalize` registered for the sentinel.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidRange => "invalid restore range",
            Self::RestoreTableIdMismatch => "restore table ID mismatch",
            Self::RestoreInvalidRewrite => "invalid rewrite rule",
            Self::RestoreInvalidBackup => "invalid backup",
            Self::Plain => "",
        }
    }
}

/// An error raised by `br/pkg/restore/utils`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreError {
    kind: RestoreErrorKind,
    annotation: String,
}

impl RestoreError {
    /// Go `errors.Annotate(kind, annotation)` / `errors.Annotatef(...)`.
    #[must_use]
    pub fn annotate(kind: RestoreErrorKind, annotation: impl Into<String>) -> Self {
        Self {
            kind,
            annotation: annotation.into(),
        }
    }

    /// Go `errors.Errorf(...)`: a message with no normalized cause.
    #[must_use]
    pub fn plain(message: impl Into<String>) -> Self {
        Self::annotate(RestoreErrorKind::Plain, message)
    }

    /// Go `errors.Cause(err)`.
    #[must_use]
    pub const fn kind(&self) -> RestoreErrorKind {
        self.kind
    }
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.kind == RestoreErrorKind::Plain {
            return formatter.write_str(&self.annotation);
        }
        if self.annotation.is_empty() {
            return formatter.write_str(self.kind.message());
        }
        write!(formatter, "{}: {}", self.annotation, self.kind.message())
    }
}

impl std::error::Error for RestoreError {}

/// The `(startKey, endKey)` pair Go's two rewrite entry points return, where
/// each side is `nil` when the corresponding input key was empty.
pub type RewrittenKeys = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Go `utils.AppliedFile`, which has two shapes:
/// 1. an SST file used by full backup/restore;
/// 2. a KV file used by PITR restore.
pub trait AppliedFile {
    /// Go `GetStartKey`.
    fn get_start_key(&self) -> &[u8];
    /// Go `GetEndKey`.
    fn get_end_key(&self) -> &[u8];
}

impl AppliedFile for File {
    fn get_start_key(&self) -> &[u8] {
        &self.start_key
    }

    fn get_end_key(&self) -> &[u8] {
        &self.end_key
    }
}

/// Go `utils.TableIDRemap`: a remapping of a table ID during rewriting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableIdRemap {
    /// Go `TableIDRemap.Origin`.
    pub origin: i64,
    /// Go `TableIDRemap.Rewritten`.
    pub rewritten: i64,
}

/// Go `utils.RewriteRules`: the rules for rewriting the keys of tables.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RewriteRules {
    /// Go `RewriteRules.Data`.
    pub data: Vec<RewriteRule>,
    /// Go `RewriteRules.OldKeyspace`.
    pub old_keyspace: Vec<u8>,
    /// Go `RewriteRules.NewKeyspace`.
    pub new_keyspace: Vec<u8>,
    /// Go `RewriteRules.NewTableID`, used to record checkpoint data.
    pub new_table_id: i64,
    /// Go `RewriteRules.ShiftStartTs`.
    pub shift_start_ts: u64,
    /// Go `RewriteRules.StartTs`.
    pub start_ts: u64,
    /// Go `RewriteRules.RestoredTs`.
    pub restored_ts: u64,
    /// Go `RewriteRules.TableIDRemapHint`, used to record backup files to PITR.
    pub table_id_remap_hint: Vec<TableIdRemap>,
}

impl RewriteRules {
    /// Go `(*RewriteRules).HasSetTs`.
    #[must_use]
    pub const fn has_set_ts(&self) -> bool {
        self.start_ts != 0 && self.restored_ts != 0
    }

    /// Go `(*RewriteRules).SetTsRange`.
    pub const fn set_ts_range(&mut self, shift_start_ts: u64, start_ts: u64, restored_ts: u64) {
        self.shift_start_ts = shift_start_ts;
        self.start_ts = start_ts;
        self.restored_ts = restored_ts;
    }

    /// Go `(*RewriteRules).RewriteSourceTableID`.
    pub fn rewrite_source_table_id(&mut self, from: i64, to: i64) -> bool {
        let to_prefix = encode_table_prefix(to);
        let from_prefix = encode_table_prefix(from);
        let mut rewritten = false;
        for rule in &mut self.data {
            if rule.old_key_prefix.starts_with(&from_prefix) {
                let mut next = to_prefix.clone();
                next.extend_from_slice(&rule.old_key_prefix[to_prefix.len()..]);
                rule.old_key_prefix = next;
                rewritten = true;
            }
        }
        rewritten
    }

    /// Go `(*RewriteRules).Clone`.
    ///
    /// Note that Go deliberately drops the three timestamp fields here; the
    /// clone is only ever used where a fresh time range is about to be set.
    #[must_use]
    pub fn go_clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            table_id_remap_hint: self.table_id_remap_hint.clone(),
            old_keyspace: self.old_keyspace.clone(),
            new_keyspace: self.new_keyspace.clone(),
            new_table_id: self.new_table_id,
            shift_start_ts: 0,
            start_ts: 0,
            restored_ts: 0,
        }
    }

    /// Go `(*RewriteRules).Equal`.
    #[must_use]
    pub fn equal(&self, rhs: &Self) -> bool {
        if self.new_keyspace != rhs.new_keyspace
            || self.old_keyspace != rhs.old_keyspace
            || self.new_table_id != rhs.new_table_id
            || self.shift_start_ts != rhs.shift_start_ts
            || self.start_ts != rhs.start_ts
            || self.restored_ts != rhs.restored_ts
        {
            return false;
        }
        self.table_id_remap_hint == rhs.table_id_remap_hint && self.data == rhs.data
    }

    /// Go `(*RewriteRules).Append`.
    pub fn append(&mut self, other: &Self) {
        self.data.extend(other.data.iter().cloned());
    }
}

impl std::fmt::Display for RewriteRules {
    /// Go `(*RewriteRules).String`. Go renders a `nil` receiver as `[]`; that
    /// is [`Option<&RewriteRules>`]'s `None` here, handled by callers.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[")?;
        if !self.old_keyspace.is_empty() {
            write!(
                formatter,
                "{} =[ks]=> {}",
                redact::key(&self.old_keyspace),
                redact::key(&self.new_keyspace)
            )?;
        }
        for (i, d) in self.data.iter().enumerate() {
            if i > 0 {
                formatter.write_str(",")?;
            }
            write!(
                formatter,
                "{} => {}",
                redact::key(&d.old_key_prefix),
                redact::key(&d.new_key_prefix)
            )?;
        }
        formatter.write_str("]")
    }
}

/// Renders `Option<&RewriteRules>` the way Go's `String` method does, including
/// the `[]` it returns for a `nil` receiver.
fn rules_to_string(rules: Option<&RewriteRules>) -> String {
    rules.map_or_else(|| "[]".to_owned(), ToString::to_string)
}

/// Go `SetTimeRangeFilter`.
///
/// # Errors
///
/// Returns a [`RestoreErrorKind::Plain`] error for an unrecognized column
/// family.
pub fn set_time_range_filter(
    table_rules: &RewriteRules,
    file_rule: &mut RewriteRule,
    cf_name: &str,
) -> Result<(), RestoreError> {
    // For some SST files, like DB-restore copy SSTs, no time range filter is
    // needed.
    if !table_rules.has_set_ts() {
        return Ok(());
    }

    let ignore_before_ts = if cf_name.contains(DEFAULT_CF_NAME) {
        // For the default CF the shift start TS can be less than the start TS,
        // which happens when a large-KV txn lands after a small-KV txn. Using
        // the start TS to filter out irrelevant default-CF data is safer.
        table_rules.shift_start_ts.min(table_rules.start_ts)
    } else if cf_name.contains(WRITE_CF_NAME) {
        table_rules.start_ts
    } else {
        return Err(RestoreError::plain(format!(
            "unsupported column family type: {cf_name}"
        )));
    };

    // Set both timestamps since the file's range needs filtering.
    file_rule.ignore_before_timestamp = ignore_before_ts;
    file_rule.ignore_after_timestamp = table_rules.restored_ts;
    Ok(())
}

/// Go `EmptyRewriteRulesMap`.
#[must_use]
pub fn empty_rewrite_rules_map() -> BTreeMap<i64, RewriteRules> {
    BTreeMap::new()
}

/// Go `EmptyRewriteRule`.
#[must_use]
pub fn empty_rewrite_rule() -> RewriteRules {
    RewriteRules::default()
}

/// Builds the per-table-ID rules Go's three rule constructors share.
fn table_rules(
    old_table_id: i64,
    new_table_id: i64,
    index_ids: &BTreeMap<i64, i64>,
    new_time_stamp: u64,
    get_detail_rule: bool,
) -> Vec<RewriteRule> {
    let mut data_rules = Vec::new();
    if get_detail_rule {
        data_rules.push(RewriteRule {
            old_key_prefix: gen_table_record_prefix(old_table_id),
            new_key_prefix: gen_table_record_prefix(new_table_id),
            new_timestamp: new_time_stamp,
            ..RewriteRule::default()
        });
        for (old_index_id, new_index_id) in index_ids {
            data_rules.push(RewriteRule {
                old_key_prefix: encode_table_index_prefix(old_table_id, *old_index_id),
                new_key_prefix: encode_table_index_prefix(new_table_id, *new_index_id),
                new_timestamp: new_time_stamp,
                ..RewriteRule::default()
            });
        }
    } else {
        data_rules.push(RewriteRule {
            old_key_prefix: encode_table_prefix(old_table_id),
            new_key_prefix: encode_table_prefix(new_table_id),
            new_timestamp: new_time_stamp,
            ..RewriteRule::default()
        });
    }
    data_rules
}

/// Go `GetRewriteRules`: the rules mapping the old table onto the new one.
///
/// `get_detail_rule == true` collects the `tXXX_r` / `tYYY_i` rules used by
/// normal backup and restore; `false` collects only the `tXXX` / `tYYY` table
/// prefixes.
#[must_use]
pub fn get_rewrite_rules(
    new_table: &TableInfo,
    old_table: &TableInfo,
    new_time_stamp: u64,
    get_detail_rule: bool,
) -> RewriteRules {
    let table_ids = get_table_id_map(new_table, old_table);
    let index_ids = get_index_id_map(new_table, old_table);
    let mut remaps = Vec::new();
    let mut data_rules = Vec::new();

    for (old_table_id, new_table_id) in &table_ids {
        remaps.push(TableIdRemap {
            origin: *old_table_id,
            rewritten: *new_table_id,
        });
        data_rules.extend(table_rules(
            *old_table_id,
            *new_table_id,
            &index_ids,
            new_time_stamp,
            get_detail_rule,
        ));
    }

    RewriteRules {
        data: data_rules,
        table_id_remap_hint: remaps,
        ..RewriteRules::default()
    }
}

/// Go `GetRewriteRulesMap`.
#[must_use]
pub fn get_rewrite_rules_map(
    new_table: &TableInfo,
    old_table: &TableInfo,
    new_time_stamp: u64,
    get_detail_rule: bool,
) -> BTreeMap<i64, RewriteRules> {
    let mut rules = BTreeMap::new();

    let table_ids = get_table_id_map(new_table, old_table);
    let index_ids = get_index_id_map(new_table, old_table);
    let mut remaps = Vec::new();

    for (old_table_id, new_table_id) in &table_ids {
        remaps.push(TableIdRemap {
            origin: *old_table_id,
            rewritten: *new_table_id,
        });
        rules.insert(
            *old_table_id,
            RewriteRules {
                data: table_rules(
                    *old_table_id,
                    *new_table_id,
                    &index_ids,
                    new_time_stamp,
                    get_detail_rule,
                ),
                // Go hands every entry the `remaps` slice as it stands at this
                // iteration.
                table_id_remap_hint: remaps.clone(),
                ..RewriteRules::default()
            },
        );
    }

    rules
}

/// Go `GetRewriteRuleOfTable`: a rewrite rule from `t_{old_id}` to
/// `t_{new_id}`.
#[must_use]
pub fn get_rewrite_rule_of_table(
    old_table_id: i64,
    new_table_id: i64,
    index_ids: &BTreeMap<i64, i64>,
    get_detail_rule: bool,
) -> RewriteRules {
    RewriteRules {
        data: table_rules(old_table_id, new_table_id, index_ids, 0, get_detail_rule),
        new_table_id,
        table_id_remap_hint: vec![TableIdRemap {
            origin: old_table_id,
            rewritten: new_table_id,
        }],
        ..RewriteRules::default()
    }
}

/// Go `ValidateFileRewriteRule`: validates a file's ranges against the rules.
///
/// # Errors
///
/// Returns [`RestoreErrorKind::RestoreInvalidRewrite`] when either bound has no
/// matching rule, or when the two bounds match different rules.
pub fn validate_file_rewrite_rule(
    file: &File,
    rewrite_rules: Option<&RewriteRules>,
) -> Result<(), RestoreError> {
    // Check if the start key has a matched rewrite key.
    let (_, start_rule) = rewrite_raw_key(file.get_start_key(), rewrite_rules);
    if rewrite_rules.is_some() && start_rule.is_none() {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            "cannot find rewrite rule",
        ));
    }
    // Check if the end key has a matched rewrite key.
    let (_, end_rule) = rewrite_raw_key(file.get_end_key(), rewrite_rules);
    if rewrite_rules.is_some() && end_rule.is_none() {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            "cannot find rewrite rule",
        ));
    }
    // The rewrite rules of the start key and the end key should be equal: there
    // should be only one rewrite rule per file, and a file should be imported
    // into exactly one region.
    let start_new = RewriteRule::get_new_key_prefix(start_rule.as_ref());
    let end_new = RewriteRule::get_new_key_prefix(end_rule.as_ref());
    if start_new != end_new {
        let start = start_rule.unwrap_or_default();
        let end = end_rule.unwrap_or_default();
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            format!(
                "rewrite rule mismatch, the backup data may be dirty or from incompatible \
                 versions of BR, startKey rule: {} => {}, endKey rule: {} => {}",
                hex_upper(&start.old_key_prefix),
                hex_upper(&start.new_key_prefix),
                hex_upper(&end.old_key_prefix),
                hex_upper(&end.new_key_prefix),
            ),
        ));
    }
    Ok(())
}

/// Go's `%X` verb over a byte slice.
fn hex_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

/// Go `rewriteEncodedKey`: rewrites an encoded key and returns an encoded key.
fn rewrite_encoded_key(
    key: &[u8],
    rewrite_rules: Option<&RewriteRules>,
) -> (Option<Vec<u8>>, Option<RewriteRule>) {
    let Some(rules) = rewrite_rules else {
        return (Some(key.to_vec()), None);
    };
    if !key.is_empty() {
        let raw_key = decode_bytes(key).map_or_else(|_| Vec::new(), |(_, raw)| raw);
        return rewrite_raw_key(&raw_key, Some(rules));
    }
    (None, None)
}

/// Go `rewriteRawKey`: rewrites a raw key with a raw-key rewrite rule and
/// returns an encoded key.
fn rewrite_raw_key(
    key: &[u8],
    rewrite_rules: Option<&RewriteRules>,
) -> (Option<Vec<u8>>, Option<RewriteRule>) {
    let Some(rules) = rewrite_rules else {
        let mut encoded = Vec::new();
        encode_bytes(&mut encoded, key);
        return (Some(encoded), None);
    };
    if !key.is_empty() {
        let rule = match_old_prefix(key, rules).cloned();
        return (Some(rewrite_and_encode_raw_key(key, rule.as_ref())), rule);
    }
    (None, None)
}

/// Go `RewriteAndEncodeRawKey`.
#[must_use]
pub fn rewrite_and_encode_raw_key(key: &[u8], rule: Option<&RewriteRule>) -> Vec<u8> {
    let old = RewriteRule::get_old_key_prefix(rule);
    let new = RewriteRule::get_new_key_prefix(rule);
    // Go `bytes.Replace(key, old, new, 1)`: replaces the first occurrence, and
    // an empty `old` inserts `new` at the front.
    let ret = replace_first(key, old, new);
    let mut encoded = Vec::new();
    encode_bytes(&mut encoded, &ret);
    encoded
}

/// Go `bytes.Replace(s, old, new, 1)`.
fn replace_first(s: &[u8], old: &[u8], new: &[u8]) -> Vec<u8> {
    if old.is_empty() {
        let mut out = new.to_vec();
        out.extend_from_slice(s);
        return out;
    }
    if let Some(at) = s.windows(old.len()).position(|window| window == old) {
        let mut out = Vec::with_capacity(s.len() - old.len() + new.len());
        out.extend_from_slice(&s[..at]);
        out.extend_from_slice(new);
        out.extend_from_slice(&s[at + old.len()..]);
        return out;
    }
    s.to_vec()
}

/// Go `matchOldPrefix`.
fn match_old_prefix<'a>(key: &[u8], rewrite_rules: &'a RewriteRules) -> Option<&'a RewriteRule> {
    rewrite_rules
        .data
        .iter()
        .find(|rule| key.starts_with(&rule.old_key_prefix))
}

/// Go `GetRewriteTableID`: the rewritten table ID for an original one.
#[must_use]
pub fn get_rewrite_table_id(table_id: i64, rewrite_rules: &RewriteRules) -> i64 {
    let table_key = gen_table_record_prefix(table_id);
    match_old_prefix(&table_key, rewrite_rules)
        .map_or(0, |rule| decode_table_id(&rule.new_key_prefix))
}

/// Go `FindMatchedRewriteRule`.
#[must_use]
pub fn find_matched_rewrite_rule(
    file: &dyn AppliedFile,
    rules: Option<&RewriteRules>,
) -> Option<RewriteRule> {
    let start_id = decode_table_id(file.get_start_key());
    let end_id = decode_table_id(file.get_end_key());
    if start_id != end_id {
        return None;
    }
    let (_, rule) = rewrite_raw_key(file.get_start_key(), rules);
    if rule.is_none() {
        // Fall back to the encoded key.
        let (_, rule) = rewrite_encoded_key(file.get_start_key(), rules);
        return rule;
    }
    rule
}

/// Go `GetRewriteRawKeys`: rewrites the rules onto the raw key.
///
/// # Errors
///
/// Returns [`RestoreErrorKind::RestoreInvalidRewrite`] when the bounds address
/// different tables or when either bound has no matching rule.
pub fn get_rewrite_raw_keys(
    file: &dyn AppliedFile,
    rewrite_rules: Option<&RewriteRules>,
) -> Result<RewrittenKeys, RestoreError> {
    let start_id = decode_table_id(file.get_start_key());
    let end_id = decode_table_id(file.get_end_key());
    if start_id != end_id {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            "invalid table id",
        ));
    }
    let (start_key, rule) = rewrite_raw_key(file.get_start_key(), rewrite_rules);
    if rewrite_rules.is_some() && rule.is_none() {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            format!(
                "cannot find raw rewrite rule for start key, startKey: {}; self = {}",
                redact::key(file.get_start_key()),
                rules_to_string(rewrite_rules)
            ),
        ));
    }
    let (end_key, rule) = rewrite_raw_key(file.get_end_key(), rewrite_rules);
    if rewrite_rules.is_some() && rule.is_none() {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            format!(
                "cannot find raw rewrite rule for end key, endKey: {}",
                redact::key(file.get_end_key())
            ),
        ));
    }
    Ok((start_key, end_key))
}

/// Go `GetRewriteEncodedKeys`: rewrites the rules onto the encoded key.
///
/// # Errors
///
/// See [`get_rewrite_raw_keys`].
pub fn get_rewrite_encoded_keys(
    file: &dyn AppliedFile,
    rewrite_rules: Option<&RewriteRules>,
) -> Result<RewrittenKeys, RestoreError> {
    let start_id = decode_table_id(file.get_start_key());
    let end_id = decode_table_id(file.get_end_key());
    if start_id != end_id {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            "invalid table id",
        ));
    }
    let (start_key, rule) = rewrite_encoded_key(file.get_start_key(), rewrite_rules);
    if rewrite_rules.is_some() && rule.is_none() {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            format!(
                "cannot find encode rewrite rule for start key, startKey: {}; rewrite rules: {}",
                redact::key(file.get_start_key()),
                rules_to_string(rewrite_rules)
            ),
        ));
    }
    let (end_key, rule) = rewrite_encoded_key(file.get_end_key(), rewrite_rules);
    if rewrite_rules.is_some() && rule.is_none() {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreInvalidRewrite,
            format!(
                "cannot find encode rewrite rule for end key, endKey: {}; rewrite rules: {}",
                redact::key(file.get_end_key()),
                rules_to_string(rewrite_rules)
            ),
        ));
    }
    Ok((start_key, end_key))
}

/// Go `replacePrefix`: the data rules are searched first.
fn replace_prefix(s: &[u8], rewrite_rules: &RewriteRules) -> (Vec<u8>, bool) {
    for rule in &rewrite_rules.data {
        if s.starts_with(&rule.old_key_prefix) {
            let mut out = rule.new_key_prefix.clone();
            out.extend_from_slice(&s[rule.old_key_prefix.len()..]);
            return (out, true);
        }
    }
    (s.to_vec(), false)
}

/// Go `RewriteRange`: rewrites both bounds of a range in place.
///
/// # Errors
///
/// Returns [`RestoreErrorKind::RestoreTableIdMismatch`] when the two bounds
/// address different tables.
pub fn rewrite_range<F: RangeFile>(
    mut rg: Range<F>,
    rewrite_rules: Option<&RewriteRules>,
) -> Result<Range<F>, RestoreError> {
    let Some(rules) = rewrite_rules else {
        return Ok(rg);
    };
    let start_id = decode_table_id(rg.start_key());
    let end_id = decode_table_id(rg.end_key());
    if start_id != end_id {
        return Err(RestoreError::annotate(
            RestoreErrorKind::RestoreTableIdMismatch,
            "table id mismatch",
        ));
    }
    let (start_key, _) = replace_prefix(rg.start_key(), rules);
    rg.key_range.start_key = start_key;
    let (end_key, _) = replace_prefix(rg.end_key(), rules);
    rg.key_range.end_key = end_key;
    Ok(rg)
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_codec::table_key::{encode_index_seek_key, gen_table_index_prefix, gen_table_prefix};
    use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
    use tidb_model::{IndexInfo, PartitionDefinition, PartitionInfo};

    use super::*;

    fn rule(old: Vec<u8>, new: Vec<u8>) -> RewriteRule {
        RewriteRule {
            old_key_prefix: old,
            new_key_prefix: new,
            ..RewriteRule::default()
        }
    }

    /// Go `kv.Key.PrefixNext`, needed by `TestRewriteFileKeys`.
    fn prefix_next(key: &[u8]) -> Vec<u8> {
        let mut buf = key.to_vec();
        for i in (0..buf.len()).rev() {
            buf[i] = buf[i].wrapping_add(1);
            if buf[i] != 0 {
                return buf;
            }
        }
        vec![0xff; key.len().max(1)]
    }

    /// Go `TestValidateFileRewriteRule` (`rewrite_rule_test.go`).
    #[test]
    fn validate_file_rewrite_rule_cases() {
        let mut rules = RewriteRules {
            data: vec![rule(encode_table_prefix(1), encode_table_prefix(2))],
            ..RewriteRules::default()
        };

        // Empty start/end keys are not allowed.
        let err = validate_file_rewrite_rule(
            &File {
                name: "file_write.sst".to_owned(),
                ..File::default()
            },
            Some(&rules),
        )
        .expect_err("empty keys have no rule");
        assert!(err.to_string().contains("cannot find rewrite rule"));

        // The range does not overlap, so no rule is found.
        let err = validate_file_rewrite_rule(
            &File {
                name: "file_write.sst".to_owned(),
                start_key: encode_table_prefix(0),
                end_key: encode_table_prefix(1),
                ..File::default()
            },
            Some(&rules),
        )
        .expect_err("no rule for table 0");
        assert!(err.to_string().contains("cannot find rewrite rule"));

        // No rule for the end key.
        let err = validate_file_rewrite_rule(
            &File {
                name: "file_write.sst".to_owned(),
                start_key: encode_table_prefix(1),
                end_key: encode_table_prefix(2),
                ..File::default()
            },
            Some(&rules),
        )
        .expect_err("no rule for table 2");
        assert!(err.to_string().contains("cannot find rewrite rule"));

        // Add a rule for the end key.
        rules
            .data
            .push(rule(encode_table_prefix(2), encode_table_prefix(3)));
        let err = validate_file_rewrite_rule(
            &File {
                name: "file_write.sst".to_owned(),
                start_key: encode_table_prefix(1),
                end_key: encode_table_prefix(2),
                ..File::default()
            },
            Some(&rules),
        )
        .expect_err("two different rules");
        assert!(err.to_string().contains("rewrite rule mismatch"));

        // Add a bad rule for the end key: after rewriting, start key > end key.
        rules.data.truncate(1);
        rules
            .data
            .push(rule(encode_table_prefix(2), encode_table_prefix(1)));
        let err = validate_file_rewrite_rule(
            &File {
                name: "file_write.sst".to_owned(),
                start_key: encode_table_prefix(1),
                end_key: encode_table_prefix(2),
                ..File::default()
            },
            Some(&rules),
        )
        .expect_err("two different rules");
        assert!(err.to_string().contains("rewrite rule mismatch"));
    }

    /// boundary: `brpb.DataFileInfo`, the PITR log-file descriptor. Only its
    /// two key bounds matter to [`AppliedFile`].
    struct DataFileInfo {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    }

    impl AppliedFile for DataFileInfo {
        fn get_start_key(&self) -> &[u8] {
            &self.start_key
        }
        fn get_end_key(&self) -> &[u8] {
            &self.end_key
        }
    }

    fn encoded(key: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_bytes(&mut out, key);
        out
    }

    /// Go `TestRewriteFileKeys` (`rewrite_rule_test.go`).
    #[test]
    fn rewrite_file_keys() {
        let rewrite_rules = RewriteRules {
            data: vec![
                rule(gen_table_prefix(1), gen_table_prefix(2)),
                rule(gen_table_prefix(767), gen_table_prefix(511)),
            ],
            ..RewriteRules::default()
        };

        let raw_key_file = File {
            name: "backup.sst".to_owned(),
            start_key: gen_table_record_prefix(1),
            end_key: prefix_next(&gen_table_record_prefix(1)),
            ..File::default()
        };
        let (start, end) =
            get_rewrite_raw_keys(&raw_key_file, Some(&rewrite_rules)).expect("rules match");
        let (_, end) = decode_bytes(&end.expect("end key")).expect("decodable");
        let (_, start) = decode_bytes(&start.expect("start key")).expect("decodable");
        assert_eq!(gen_table_record_prefix(2), start);
        assert_eq!(prefix_next(&gen_table_record_prefix(2)), end);

        let encode_key_file = DataFileInfo {
            start_key: encoded(&gen_table_record_prefix(1)),
            end_key: encoded(&prefix_next(&gen_table_record_prefix(1))),
        };
        let (start, end) =
            get_rewrite_encoded_keys(&encode_key_file, Some(&rewrite_rules)).expect("rules match");
        assert_eq!(Some(encoded(&gen_table_record_prefix(2))), start);
        assert_eq!(
            Some(encoded(&prefix_next(&gen_table_record_prefix(2)))),
            end
        );

        // Table ID 767.
        let encode_key_file767 = DataFileInfo {
            start_key: encoded(&gen_table_record_prefix(767)),
            end_key: encoded(&prefix_next(&gen_table_record_prefix(767))),
        };
        // The raw rewrite should not error, but must not match either.
        let (start, end) =
            get_rewrite_raw_keys(&encode_key_file767, Some(&rewrite_rules)).expect("no error");
        assert_ne!(Some(encoded(&gen_table_record_prefix(511))), start);
        assert_ne!(
            Some(encoded(&prefix_next(&gen_table_record_prefix(511)))),
            end
        );
        // The encoded rewrite should match.
        let (start, end) =
            get_rewrite_encoded_keys(&encode_key_file767, Some(&rewrite_rules)).expect("no error");
        assert_eq!(Some(encoded(&gen_table_record_prefix(511))), start);
        assert_eq!(
            Some(encoded(&prefix_next(&gen_table_record_prefix(511)))),
            end
        );
    }

    /// Go `TestRewriteRange` (`rewrite_rule_test.go`).
    #[test]
    fn rewrite_range_cases() {
        let with_suffix = |prefix: Vec<u8>, suffix: &str| {
            let mut key = prefix;
            key.extend_from_slice(suffix.as_bytes());
            key
        };

        // Test case 1: no rewrite rules.
        let rg: Range<File> = Range::new(b"startKey".to_vec(), b"endKey".to_vec());
        assert_eq!(
            Range::<File>::new(b"startKey".to_vec(), b"endKey".to_vec()),
            rewrite_range(rg, None).expect("no rules is a no-op")
        );

        // Test case 2: a rewrite rule is found for both bounds.
        let rg: Range<File> = Range::new(
            with_suffix(gen_table_index_prefix(1), "startKey"),
            with_suffix(gen_table_index_prefix(1), "endKey"),
        );
        let rules = RewriteRules {
            data: vec![rule(gen_table_index_prefix(1), gen_table_index_prefix(2))],
            ..RewriteRules::default()
        };
        assert_eq!(
            Range::<File>::new(
                with_suffix(gen_table_index_prefix(2), "startKey"),
                with_suffix(gen_table_index_prefix(2), "endKey"),
            ),
            rewrite_range(rg, Some(&rules)).expect("rules match")
        );

        // Test case 3: a rewrite rule is found only for the end key.
        let rg: Range<File> = Range::new(
            with_suffix(gen_table_index_prefix(1), "startKey"),
            with_suffix(gen_table_index_prefix(1), "endKey"),
        );
        let rules = RewriteRules {
            data: vec![rule(
                with_suffix(gen_table_index_prefix(1), "endKey"),
                with_suffix(gen_table_index_prefix(2), "newEndKey"),
            )],
            ..RewriteRules::default()
        };
        assert_eq!(
            Range::<File>::new(
                with_suffix(gen_table_index_prefix(1), "startKey"),
                with_suffix(gen_table_index_prefix(2), "newEndKey"),
            ),
            rewrite_range(rg, Some(&rules)).expect("rules match")
        );

        // Test case 4: table ID mismatch.
        let rg: Range<File> = Range::new(b"t1_startKey".to_vec(), b"t2_endKey".to_vec());
        let rules = RewriteRules {
            data: vec![rule(b"t1_startKey".to_vec(), b"t2_newStartKey".to_vec())],
            ..RewriteRules::default()
        };
        let err = rewrite_range(rg, Some(&rules)).expect_err("table id mismatch");
        assert_eq!(
            "table id mismatch: restore table ID mismatch",
            err.to_string()
        );
    }

    /// Go `TestGetRewriteTableID` (`rewrite_rule_test.go`).
    #[test]
    fn get_rewrite_table_id_cases() {
        let table_id: i64 = 76;
        let old_table_id: i64 = 80;
        {
            let rules = RewriteRules {
                data: vec![rule(
                    encode_table_prefix(old_table_id),
                    encode_table_prefix(table_id),
                )],
                ..RewriteRules::default()
            };
            assert_eq!(table_id, get_rewrite_table_id(old_table_id, &rules));
        }
        {
            let rules = RewriteRules {
                data: vec![rule(
                    gen_table_record_prefix(old_table_id),
                    gen_table_record_prefix(table_id),
                )],
                ..RewriteRules::default()
            };
            assert_eq!(table_id, get_rewrite_table_id(old_table_id, &rules));
        }
    }

    fn get_new_key_prefix(key: &[u8], rules: &RewriteRules) -> Option<Vec<u8>> {
        rules
            .data
            .iter()
            .find(|rule| key.starts_with(&rule.old_key_prefix))
            .map(|rule| rule.new_key_prefix.clone())
    }

    fn index(id: i64, name: &str) -> IndexInfo {
        IndexInfo {
            id,
            name: CiString::new(name),
            ..IndexInfo::default()
        }
    }

    fn partition(id: i64, name: &str) -> PartitionDefinition {
        PartitionDefinition {
            id,
            name: CiString::new(name),
            ..PartitionDefinition::default()
        }
    }

    /// Go `generateRewriteTableInfos` (`rewrite_rule_test.go`).
    fn generate_rewrite_table_infos() -> (TableInfo, TableInfo) {
        let new_table = TableInfo {
            id: 1,
            indices: vec![index(1, "i1"), index(2, "i2")].into(),
            partition: Some(tidb_model::GoShared::new(PartitionInfo {
                definitions: vec![partition(100, "p1"), partition(200, "p2")].into(),
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        };
        let old_table = TableInfo {
            id: 2,
            indices: vec![index(1, "i1"), index(2, "i2")].into(),
            partition: Some(tidb_model::GoShared::new(PartitionInfo {
                definitions: vec![partition(101, "p1"), partition(201, "p2")].into(),
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        };
        (new_table, old_table)
    }

    /// Go `TestGetRewriteRules` (`rewrite_rule_test.go`).
    #[test]
    fn get_rewrite_rules_cases() {
        let (new_table_info, old_table_info) = generate_rewrite_table_infos();

        {
            let rules = get_rewrite_rules(&new_table_info, &old_table_info, 0, false);
            for (old, new) in [(2, 1), (101, 100), (201, 200)] {
                assert_eq!(
                    Some(encode_table_prefix(new)),
                    get_new_key_prefix(&encode_table_prefix(old), &rules)
                );
            }
        }

        {
            let rules = get_rewrite_rules(&new_table_info, &old_table_info, 0, true);
            for (old, new) in [(2, 1), (101, 100), (201, 200)] {
                assert_eq!(
                    Some(gen_table_record_prefix(new)),
                    get_new_key_prefix(&gen_table_record_prefix(old), &rules)
                );
                for index_id in [1, 2] {
                    assert_eq!(
                        Some(encode_table_index_prefix(new, index_id)),
                        get_new_key_prefix(&encode_table_index_prefix(old, index_id), &rules)
                    );
                }
            }
        }
    }

    /// Go `TestGetRewriteRulesMap` (`rewrite_rule_test.go`).
    #[test]
    fn get_rewrite_rules_map_cases() {
        let (new_table_info, old_table_info) = generate_rewrite_table_infos();

        {
            let rules = get_rewrite_rules_map(&new_table_info, &old_table_info, 0, false);
            for (old, new) in [(2, 1), (101, 100), (201, 200)] {
                assert_eq!(
                    Some(encode_table_prefix(new)),
                    get_new_key_prefix(&encode_table_prefix(old), &rules[&old])
                );
            }
        }

        {
            let rules = get_rewrite_rules_map(&new_table_info, &old_table_info, 0, true);
            for (old, new) in [(2, 1), (101, 100), (201, 200)] {
                assert_eq!(
                    Some(gen_table_record_prefix(new)),
                    get_new_key_prefix(&gen_table_record_prefix(old), &rules[&old])
                );
                for index_id in [1, 2] {
                    assert_eq!(
                        Some(encode_table_index_prefix(new, index_id)),
                        get_new_key_prefix(&encode_table_index_prefix(old, index_id), &rules[&old])
                    );
                }
            }
        }
    }

    /// Go `TestGetRewriteRuleOfTable` (`rewrite_rule_test.go`).
    #[test]
    fn get_rewrite_rule_of_table_cases() {
        // A basic table-prefix rewrite without detailed rules.
        {
            let rules = get_rewrite_rule_of_table(2, 1, &BTreeMap::from([(1, 1), (2, 2)]), false);
            assert_eq!(
                Some(encode_table_prefix(1)),
                get_new_key_prefix(&encode_table_prefix(2), &rules)
            );
            // Only one rule for the table prefix.
            assert_eq!(1, rules.data.len());
            assert_eq!(1, rules.new_table_id);
            assert_eq!(
                vec![TableIdRemap {
                    origin: 2,
                    rewritten: 1
                }],
                rules.table_id_remap_hint
            );
        }

        // Detailed rules including record and index prefixes.
        {
            let index_ids = BTreeMap::from([(1, 1), (2, 2)]);
            let rules = get_rewrite_rule_of_table(2, 1, &index_ids, true);
            assert_eq!(
                Some(gen_table_record_prefix(1)),
                get_new_key_prefix(&gen_table_record_prefix(2), &rules)
            );
            for index_id in [1, 2] {
                assert_eq!(
                    Some(encode_table_index_prefix(1, index_id)),
                    get_new_key_prefix(&encode_table_index_prefix(2, index_id), &rules)
                );
            }
            // One record rule plus two index rules.
            assert_eq!(3, rules.data.len());
        }

        // Timestamp fields.
        {
            let mut rules = get_rewrite_rule_of_table(2, 1, &BTreeMap::from([(1, 1)]), true);
            rules.set_ts_range(30, 50, 100);
            assert_eq!(100, rules.restored_ts);
            assert_eq!(50, rules.start_ts);
            assert_eq!(30, rules.shift_start_ts);
            assert_eq!(
                vec![TableIdRemap {
                    origin: 2,
                    rewritten: 1
                }],
                rules.table_id_remap_hint
            );
            assert_eq!(1, rules.new_table_id);
        }

        // Empty index IDs.
        {
            let rules = get_rewrite_rule_of_table(2, 1, &BTreeMap::new(), true);
            // Only the record rule, no index rules.
            assert_eq!(1, rules.data.len());
            assert_eq!(
                Some(gen_table_record_prefix(1)),
                get_new_key_prefix(&gen_table_record_prefix(2), &rules)
            );
        }
    }

    /// Go `fakeApplyFile` (`rewrite_rule_test.go`).
    struct FakeApplyFile {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    }

    impl AppliedFile for FakeApplyFile {
        fn get_start_key(&self) -> &[u8] {
            &self.start_key
        }
        fn get_end_key(&self) -> &[u8] {
            &self.end_key
        }
    }

    /// Go `rewriteKey` (`rewrite_rule_test.go`).
    fn rewrite_key(key: &[u8], rule: Option<&RewriteRule>) -> Option<Vec<u8>> {
        let old = RewriteRule::get_old_key_prefix(rule);
        let new = RewriteRule::get_new_key_prefix(rule);
        if key.starts_with(old) {
            let mut out = new.to_vec();
            out.extend_from_slice(&key[new.len()..]);
            return Some(out);
        }
        None
    }

    fn row_key(table_id: i64, handle: i64) -> Vec<u8> {
        encode_row_key_with_handle(table_id, &RecordHandle::Int(handle))
    }

    /// Go `TestFindMatchedRewriteRule` (`rewrite_rule_test.go`).
    #[test]
    fn find_matched_rewrite_rule_cases() {
        let rules = get_rewrite_rule_of_table(2, 1, &BTreeMap::from([(1, 10)]), true);
        {
            let apply_file = FakeApplyFile {
                start_key: row_key(2, 100),
                end_key: row_key(2, 200),
            };
            let rule = find_matched_rewrite_rule(&apply_file, Some(&rules));
            assert_eq!(
                Some(row_key(1, 100)),
                rewrite_key(&row_key(2, 100), rule.as_ref())
            );
        }

        {
            let apply_file = FakeApplyFile {
                start_key: encode_index_seek_key(2, 1, b"test-1"),
                end_key: encode_index_seek_key(2, 1, b"test-2"),
            };
            let rule = find_matched_rewrite_rule(&apply_file, Some(&rules));
            assert_eq!(
                Some(encode_index_seek_key(1, 10, b"test-1")),
                rewrite_key(&encode_index_seek_key(2, 1, b"test-1"), rule.as_ref())
            );
        }

        {
            let apply_file = FakeApplyFile {
                start_key: row_key(1, 100),
                end_key: row_key(2, 200),
            };
            assert!(find_matched_rewrite_rule(&apply_file, Some(&rules)).is_none());
        }

        {
            let apply_file = FakeApplyFile {
                start_key: encode_table_prefix(1),
                end_key: encode_table_prefix(1),
            };
            assert!(find_matched_rewrite_rule(&apply_file, Some(&rules)).is_none());
        }
    }

    /// Go `TestGetRewriteKeyWithDifferentTable` (`rewrite_rule_test.go`).
    #[test]
    fn get_rewrite_key_with_different_table() {
        let apply_file = FakeApplyFile {
            start_key: row_key(1, 100),
            end_key: row_key(2, 200),
        };
        assert!(get_rewrite_raw_keys(&apply_file, None).is_err());
        assert!(get_rewrite_encoded_keys(&apply_file, None).is_err());
    }

    /// Go `TestSetTimeRangeFilter` (`rewrite_rule_test.go`).
    #[test]
    fn set_time_range_filter_cases() {
        struct Case {
            name: &'static str,
            rules: RewriteRules,
            cf_name: &'static str,
            expect_error: bool,
        }

        let with_ts = |shift: u64, start: u64, restored: u64| RewriteRules {
            data: vec![rule(b"old".to_vec(), b"new".to_vec())],
            shift_start_ts: shift,
            start_ts: start,
            restored_ts: restored,
            ..RewriteRules::default()
        };

        let cases = vec![
            Case {
                name: "default cf with valid timestamps",
                rules: with_ts(50, 100, 200),
                cf_name: "default",
                expect_error: false,
            },
            Case {
                name: "write cf with valid timestamps",
                rules: with_ts(50, 100, 200),
                cf_name: "write",
                expect_error: false,
            },
            Case {
                name: "invalid shift start ts (greater than start ts)",
                rules: with_ts(150, 100, 200),
                cf_name: "default",
                expect_error: false,
            },
            Case {
                name: "write cf valid shift start ts (greater than start ts)",
                rules: with_ts(150, 100, 200),
                cf_name: "write",
                expect_error: false,
            },
            Case {
                name: "invalid cf name",
                rules: with_ts(50, 100, 200),
                cf_name: "invalid",
                expect_error: true,
            },
            Case {
                name: "zero timestamps should skip filter",
                rules: with_ts(0, 0, 0),
                cf_name: "default",
                expect_error: false,
            },
        ];

        for case in &cases {
            let mut file_rule = RewriteRule::default();
            let result = set_time_range_filter(&case.rules, &mut file_rule, case.cf_name);
            if case.expect_error {
                assert!(result.is_err(), "{}", case.name);
                continue;
            }
            assert!(result.is_ok(), "{}", case.name);

            if case.rules.start_ts == 0 || case.rules.restored_ts == 0 {
                // Nothing should be modified when the timestamps are zero.
                for rule in &case.rules.data {
                    assert_eq!(0, rule.ignore_before_timestamp, "{}", case.name);
                    assert_eq!(0, rule.ignore_after_timestamp, "{}", case.name);
                }
                continue;
            }

            assert_eq!(
                case.rules.restored_ts, file_rule.ignore_after_timestamp,
                "{}",
                case.name
            );
            if case.cf_name.contains("default") {
                let expect = if case.rules.shift_start_ts < case.rules.start_ts {
                    case.rules.shift_start_ts
                } else {
                    case.rules.start_ts
                };
                assert_eq!(expect, file_rule.ignore_before_timestamp, "{}", case.name);
            } else if case.cf_name.contains("write") {
                assert_eq!(
                    case.rules.start_ts, file_rule.ignore_before_timestamp,
                    "{}",
                    case.name
                );
            }
        }
    }

    /// Go `TestSetTimeRangeFilterRace` (`rewrite_rule_test.go`).
    ///
    /// Go spawns 100 goroutines to prove `SetTimeRangeFilter` only *reads* the
    /// shared `RewriteRules`. Rust's borrow checker proves the same property
    /// statically: the function takes `&RewriteRules` and `&mut RewriteRule`,
    /// so a data race on the shared value cannot be written. The value
    /// assertions are kept.
    #[test]
    fn set_time_range_filter_does_not_mutate_shared_rules() {
        let rules = RewriteRules {
            data: vec![rule(b"old".to_vec(), b"new".to_vec())],
            shift_start_ts: 50,
            start_ts: 100,
            restored_ts: 200,
            ..RewriteRules::default()
        };

        for _ in 0..100 {
            let mut file_rule = RewriteRule::default();
            set_time_range_filter(&rules, &mut file_rule, "default").expect("default cf");
            assert_eq!(50, file_rule.ignore_before_timestamp);
            assert_eq!(200, file_rule.ignore_after_timestamp);
        }

        assert_eq!(50, rules.shift_start_ts);
        assert_eq!(100, rules.start_ts);
        assert_eq!(200, rules.restored_ts);
    }
}
