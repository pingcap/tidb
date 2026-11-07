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

//! Go `aggregate.GetGroupKey`'s per-item key type.

/// Go `aggregate.GetGroupKey`'s key type for one group item: an ENUM keys by
/// its value rather than its name (`EnumSetAsIntFlag`, issue #26885 -- the
/// invalid value 0 has the name '' a user-defined member may share), and a
/// DECIMAL by its own precision (`SetFlen(0)`, so `EncodeDecimal` never
/// fails on a value wider than the declared type).
pub(super) fn group_key_type(
    field_type: &tidb_datatype::FieldType,
) -> std::borrow::Cow<'_, tidb_datatype::FieldType> {
    match field_type.code() {
        tidb_datatype::FieldTypeCode::Enum => {
            let mut key_type = field_type.clone();
            key_type.add_flags(tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT);
            std::borrow::Cow::Owned(key_type)
        }
        tidb_datatype::FieldTypeCode::NewDecimal => {
            let mut key_type = field_type.clone();
            key_type.set_flen(0);
            std::borrow::Cow::Owned(key_type)
        }
        _ => std::borrow::Cow::Borrowed(field_type),
    }
}
