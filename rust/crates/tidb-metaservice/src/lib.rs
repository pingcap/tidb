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

//! Go `pkg/metaservice` (`metamanager.go`): the meta service group a keyspace
//! stores its TiDB metadata in -- the global one behind PD, or a dedicated
//! group named in the keyspace's configuration.

use tidb_log::{Field, Value};
use tikv_client::proto::keyspacepb::KeyspaceMeta;

/// Go `GlobalGroupID`: the global meta service group id; it stores some
/// global rules and label, region and other information.
pub const GLOBAL_GROUP_ID: &str = "0";
/// Go `GroupIDKey`: a keyspace meta config key name, whose value is the meta
/// service group id for this keyspace.
pub const GROUP_ID_KEY: &str = "meta_service_group_id";
/// Go `GroupAddrsKey`: a keyspace meta config key name, whose value is the
/// meta service group addrs for this keyspace.
pub const GROUP_ADDRS_KEY: &str = "meta_service_group_addrs";

/// Go's `ErrGroupNotMatch`, `ErrNilKeyspaceMeta`, `ErrInvalidGroupID` and
/// `ErrKeyspaceLevelGCRequired`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaServiceError {
    /// Go `ErrGroupNotMatch`.
    GroupNotMatch,
    /// Go `ErrNilKeyspaceMeta`: the caller passed a nil keyspace meta to
    /// `GetGroup`.
    NilKeyspaceMeta,
    /// Go `ErrInvalidGroupID`.
    InvalidGroupId,
    /// Go `ErrKeyspaceLevelGCRequired`, annotated with the keyspace and the
    /// group it configured.
    KeyspaceLevelGcRequired {
        /// The keyspace's name.
        keyspace: String,
        /// The configured group ID.
        group_id: String,
    },
}

impl std::fmt::Display for MetaServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupNotMatch => {
                f.write_str("it is unexpected for the keyspace to have a group ID but no group addresses")
            }
            Self::NilKeyspaceMeta => f.write_str("GetGroup: keyspace meta is nil"),
            Self::InvalidGroupId => f.write_str(
                "invalid meta service group id: it must contain at least one letter and only contain letters, digits, '-' or '_'",
            ),
            Self::KeyspaceLevelGcRequired { keyspace, group_id } => write!(
                f,
                "keyspace {keyspace:?} configured meta service group {group_id:?}: meta service group requires keyspace-level GC"
            ),
        }
    }
}

impl std::error::Error for MetaServiceError {}

/// Go `validateGroupID` (`groupIDPattern`): only ASCII letters, digits, '-'
/// and '_', with at least one letter, so pure numeric IDs are rejected. The
/// default global group ID "0" is not validated here; it is used when
/// [`GROUP_ID_KEY`] is absent.
fn validate_group_id(group_id: &str) -> Result<(), MetaServiceError> {
    let allowed = group_id.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if allowed && group_id.bytes().any(|byte| byte.is_ascii_alphabetic()) {
        Ok(())
    } else {
        Err(MetaServiceError::InvalidGroupId)
    }
}

/// Go `Info`: the PD addresses and the TiDB meta service group info.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    /// Go `PDAddrs`.
    pub pd_addrs: Vec<String>,
    /// Go `Group`.
    pub group: Group,
}

/// Go `Group`: keyspace meta service group info.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    /// Go `GroupID`.
    pub group_id: String,
    /// Go `Addrs`.
    pub addrs: Vec<String>,
}

/// Go `GetGroup`: the keyspace meta service group.
///
/// # Errors
///
/// The meta is nil, or names an invalid group, one without addresses, or a
/// group on a keyspace without keyspace-level GC.
pub fn get_group(keyspace_meta: Option<&KeyspaceMeta>, pd_addrs: &[String]) -> Result<Group, MetaServiceError> {
    let keyspace_meta = keyspace_meta.ok_or(MetaServiceError::NilKeyspaceMeta)?;
    if let Some(group_id) = keyspace_meta.config.get(GROUP_ID_KEY) {
        validate_group_id(group_id)?;
        if !tikv_client::pd_keyspace::is_keyspace_using_keyspace_level_gc(Some(keyspace_meta)) {
            return Err(MetaServiceError::KeyspaceLevelGcRequired {
                keyspace: keyspace_meta.name.clone(),
                group_id: group_id.clone(),
            });
        }
        let addrs_str = keyspace_meta.config.get(GROUP_ADDRS_KEY).ok_or(MetaServiceError::GroupNotMatch)?;
        let addrs: Vec<String> =
            addrs_str.trim().split(',').map(str::trim).filter(|addr| !addr.is_empty()).map(str::to_owned).collect();
        if addrs.is_empty() {
            return Err(MetaServiceError::GroupNotMatch);
        }
        let group = Group { group_id: group_id.clone(), addrs };
        log_info("get keyspace meta service group info", "group-info", format!("{group:?}"));
        return Ok(group);
    }
    // If keyspace doesn't have GroupIDKey, use the global meta service group.
    let group = Group { group_id: GLOBAL_GROUP_ID.to_owned(), addrs: pd_addrs.to_vec() };
    log_info("get default keyspace meta service group info ", "group-info", format!("{group:?}"));
    Ok(group)
}

/// Go `GetInfo`: the meta service info.
///
/// # Errors
///
/// [`get_group`]'s.
pub fn get_info(keyspace_meta: Option<&KeyspaceMeta>, pd_addrs: &[String]) -> Result<Info, MetaServiceError> {
    // If non-keyspace then return global meta service or not enable meta
    // service group.
    let Some(keyspace_meta) = keyspace_meta else {
        let info = Info {
            pd_addrs: pd_addrs.to_vec(),
            group: Group { group_id: GLOBAL_GROUP_ID.to_owned(), addrs: pd_addrs.to_vec() },
        };
        log_info("return meta service group info", "meta-service-info", format!("{info:?}"));
        return Ok(info);
    };
    let info = Info { pd_addrs: pd_addrs.to_vec(), group: get_group(Some(keyspace_meta), pd_addrs)? };
    log_info("return keyspace meta service group info", "meta-service-info", format!("{info:?}"));
    Ok(info)
}

impl Info {
    /// Go `GroupAddrs`: the meta service group addresses.
    #[must_use]
    pub fn group_addrs(&self) -> &[String] {
        &self.group.addrs
    }
}

/// Go `usesGlobalMetaServiceGroup` (`etcd.go`).
#[must_use]
pub fn uses_global_meta_service_group(keyspace_meta: Option<&KeyspaceMeta>) -> bool {
    keyspace_meta.is_none_or(|meta| !meta.config.contains_key(GROUP_ID_KEY))
}

/// Go `filterEmptyAddrs` (`etcd.go`).
#[must_use]
pub fn filter_empty_addrs(addrs: &[String]) -> Vec<String> {
    addrs.iter().filter(|addr| !addr.is_empty()).cloned().collect()
}

fn log_info(message: &str, key: &'static str, value: String) {
    tidb_logutil::bg_logger().info(message, &[Field::new(key, Value::Str(value))]);
}

#[cfg(test)]
mod tests {
    use super::*;

    const GC_KEY: &str = "gc_management_type";

    fn meta(name: &str, config: &[(&str, &str)]) -> KeyspaceMeta {
        KeyspaceMeta {
            name: name.to_owned(),
            config: config.iter().map(|(key, value)| ((*key).to_owned(), (*value).to_owned())).collect(),
            ..Default::default()
        }
    }

    fn sorted(mut addrs: Vec<String>) -> Vec<String> {
        addrs.sort();
        addrs
    }

    #[test]
    fn test_get_group() {
        let pd_addrs = vec!["127.0.0.1:2379".to_owned()];
        let expected_addrs_str = "127.0.0.1:2388,127.0.0.1:2389";
        let expected_addrs: Vec<String> = expected_addrs_str.split(',').map(str::to_owned).collect();

        // Test case where keyspaceMeta is nil
        assert_eq!(get_group(None, &pd_addrs).unwrap_err(), MetaServiceError::NilKeyspaceMeta);

        // Test case with a valid group ID and addresses
        let keyspace_meta =
            meta("", &[(GROUP_ID_KEY, "group1"), (GROUP_ADDRS_KEY, expected_addrs_str), (GC_KEY, "keyspace_level")]);
        let group = get_group(Some(&keyspace_meta), &pd_addrs).unwrap();
        assert_eq!("group1", group.group_id);
        assert_eq!(expected_addrs, sorted(group.addrs));

        // Test case where the keyspace does not use keyspace-level GC.
        let keyspace_meta = meta(
            "ks-unified-gc",
            &[(GROUP_ID_KEY, "group1"), (GROUP_ADDRS_KEY, expected_addrs_str), (GC_KEY, "unified")],
        );
        let error = get_group(Some(&keyspace_meta), &pd_addrs).unwrap_err();
        assert!(matches!(error, MetaServiceError::KeyspaceLevelGcRequired { .. }));
        assert!(error.to_string().contains("requires keyspace-level GC"), "{error}");

        // Test case with blank entries in addresses
        let mut keyspace_meta = meta(
            "",
            &[
                (GROUP_ID_KEY, "group1"),
                (GROUP_ADDRS_KEY, " 127.0.0.1:2388, ,127.0.0.1:2389,  "),
                (GC_KEY, "keyspace_level"),
            ],
        );
        let group = get_group(Some(&keyspace_meta), &pd_addrs).unwrap();
        assert_eq!("group1", group.group_id);
        assert_eq!(expected_addrs, sorted(group.addrs));

        // Test case where all addresses are blank
        keyspace_meta.config.insert(GROUP_ADDRS_KEY.to_owned(), " , \t,  ".to_owned());
        assert_eq!(get_group(Some(&keyspace_meta), &pd_addrs).unwrap_err(), MetaServiceError::GroupNotMatch);

        // Test case where the group ID exists but addresses do not
        keyspace_meta.config.remove(GROUP_ADDRS_KEY);
        assert_eq!(get_group(Some(&keyspace_meta), &pd_addrs).unwrap_err(), MetaServiceError::GroupNotMatch);

        // Test case where the group ID does not exist
        keyspace_meta.config.remove(GROUP_ID_KEY);
        let group = get_group(Some(&keyspace_meta), &pd_addrs).unwrap();
        assert_eq!(GLOBAL_GROUP_ID, group.group_id);
        assert_eq!(pd_addrs, group.addrs);
    }

    #[test]
    fn test_get_group_rejects_invalid_group_id() {
        let pd_addrs = vec!["127.0.0.1:2379".to_owned()];
        // "numeric only", "contains space", "contains dot", "contains only
        // separators and digits"
        for group_id in ["1", "group 1", "group.1", "1-2_3"] {
            let keyspace_meta = meta(
                "",
                &[
                    (GROUP_ID_KEY, group_id),
                    (GROUP_ADDRS_KEY, "127.0.0.1:2388,127.0.0.1:2389"),
                    (GC_KEY, "keyspace_level"),
                ],
            );
            let error = get_group(Some(&keyspace_meta), &pd_addrs).unwrap_err();
            assert_eq!(error, MetaServiceError::InvalidGroupId, "{group_id}");
            assert!(error.to_string().contains("invalid meta service group id"));
        }
    }

    #[test]
    fn test_get_info() {
        let pd_addrs = vec!["127.0.0.1:2379".to_owned()];

        // Test case where keyspaceMeta is nil
        let meta_info = get_info(None, &pd_addrs).unwrap();
        assert_eq!("0", meta_info.group.group_id);
        assert_eq!(pd_addrs, meta_info.group.addrs);
        assert_eq!(pd_addrs, meta_info.pd_addrs);

        // Test case with a valid keyspaceMeta
        let keyspace_meta = meta(
            "",
            &[
                (GROUP_ID_KEY, "group2"),
                (GROUP_ADDRS_KEY, "127.0.0.1:2388,127.0.0.1:2389"),
                (GC_KEY, "keyspace_level"),
            ],
        );
        let meta_info = get_info(Some(&keyspace_meta), &pd_addrs).unwrap();
        assert_eq!("group2", meta_info.group.group_id);
        assert_eq!(pd_addrs, meta_info.pd_addrs);
        assert_eq!(meta_info.group_addrs(), ["127.0.0.1:2388".to_owned(), "127.0.0.1:2389".to_owned()]);
        assert!(!uses_global_meta_service_group(Some(&keyspace_meta)));
        assert!(uses_global_meta_service_group(None));
        assert_eq!(filter_empty_addrs(&[String::new(), "a".to_owned()]), ["a".to_owned()]);
    }
}
