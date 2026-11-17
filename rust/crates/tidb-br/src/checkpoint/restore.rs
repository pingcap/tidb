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

//! Go `br/pkg/checkpoint/restore.go`: the snapshot restore checkpoint's
//! keys and values. The metadata managers that start its runner come with
//! the restore task.

use tidb_gocontext::Context;

use super::{gojson, CheckpointMessage, CheckpointRunner, RangeGroup};
use crate::errors::{self, Result};

/// Go `RestoreKeyType`: the table ID.
pub type RestoreKeyType = i64;

/// Go `RestoreValueType`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreValueType {
    /// Go `RangeKey`: the file key of a range (`range-key`, omitempty).
    pub range_key: String,
    /// Go `Name`: the file name, for compacted restore (`name`, omitempty).
    pub name: String,
}

/// The restore checkpoint runner.
pub type RestoreRunner = CheckpointRunner<RestoreKeyType, RestoreValueType>;

/// Go `CheckpointItem`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckpointItem {
    table_id: RestoreKeyType,
    range_key: String,
    name: String,
}

/// Go `NewCheckpointRangeKeyItem`.
#[must_use]
pub fn new_checkpoint_range_key_item(table_id: RestoreKeyType, range_key: &str) -> CheckpointItem {
    CheckpointItem {
        table_id,
        range_key: range_key.to_owned(),
        name: String::new(),
    }
}

/// Go `NewCheckpointFileItem`.
#[must_use]
pub fn new_checkpoint_file_item(table_id: RestoreKeyType, file_name: &str) -> CheckpointItem {
    CheckpointItem {
        table_id,
        range_key: String::new(),
        name: file_name.to_owned(),
    }
}

/// Go `valueMarshalerForRestore`: `json.Marshal(group)`.
///
/// # Errors
/// Never; Go's signature has one.
pub fn value_marshaler_for_restore(
    group: &RangeGroup<RestoreKeyType, RestoreValueType>,
) -> Result<Vec<u8>> {
    let values: Vec<String> = group
        .group
        .iter()
        .map(|v| {
            let mut fields = Vec::new();
            if !v.range_key.is_empty() {
                fields.push(format!("\"range-key\":{}", gojson::string(&v.range_key)));
            }
            if !v.name.is_empty() {
                fields.push(format!("\"name\":{}", gojson::string(&v.name)));
            }
            format!("{{{}}}", fields.join(","))
        })
        .collect();
    let mut out = String::from("{");
    if group.group_key != 0 {
        out.push_str(&format!("\"group-key\":{},", group.group_key));
    }
    let groups = if group.group.is_empty() {
        "null".to_owned()
    } else {
        format!("[{}]", values.join(","))
    };
    out.push_str(&format!("\"groups\":{groups}}}"));
    Ok(out.into_bytes())
}

/// Go `AppendRangesForRestore`.
///
/// # Errors
/// An item with neither a range key nor a name, or the runner's error.
pub fn append_ranges_for_restore(
    ctx: &Context,
    r: &RestoreRunner,
    c: &CheckpointItem,
) -> Result<()> {
    let mut group = RestoreValueType::default();
    if !c.range_key.is_empty() {
        group.range_key.clone_from(&c.range_key);
    } else if !c.name.is_empty() {
        group.name.clone_from(&c.name);
    } else {
        return Err(errors::errorf(
            "either rangekey or name should be used in checkpoint append",
        ));
    }
    r.append(
        ctx,
        CheckpointMessage {
            group_key: c.table_id,
            group: vec![group],
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `json.Marshal(RangeGroup[int64, RestoreValueType])`.
    #[test]
    fn restore_value_json_matches_go() {
        let g = RangeGroup {
            group_key: 120,
            group: vec![
                RestoreValueType {
                    range_key: "1/a_b".into(),
                    name: String::new(),
                },
                RestoreValueType {
                    range_key: String::new(),
                    name: "c<d>.sst".into(),
                },
            ],
        };
        assert_eq!(
            String::from_utf8(value_marshaler_for_restore(&g).unwrap()).unwrap(),
            r#"{"group-key":120,"groups":[{"range-key":"1/a_b"},{"name":"c\u003cd\u003e.sst"}]}"#
        );
        let empty = RangeGroup::<RestoreKeyType, RestoreValueType> {
            group_key: 0,
            group: vec![],
        };
        assert_eq!(
            String::from_utf8(value_marshaler_for_restore(&empty).unwrap()).unwrap(),
            r#"{"groups":null}"#
        );
    }
}
