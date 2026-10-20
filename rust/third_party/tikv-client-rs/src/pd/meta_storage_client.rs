// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The meta storage request shaping of PD's client package
//! (`meta_storage_client.go`).

use std::sync::atomic::Ordering;

use super::opt::{MetaStorageOp, MetaStorageOption, RangeEnd};
use crate::proto::meta_storagepb;

/// Go `getPrefix`: the etcd range end covering every key with `key` as its
/// prefix. See etcd's `clientv3.GetPrefixRangeEnd`.
pub(crate) fn get_prefix(key: &[u8]) -> Vec<u8> {
    let mut end = key.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    vec![0]
}

pub(crate) fn apply_meta_storage_options(opts: &[MetaStorageOption]) -> MetaStorageOp {
    let mut options = MetaStorageOp::default();
    for opt in opts {
        opt(&mut options);
    }
    options
}

fn range_end_bytes(range_end: &Option<RangeEnd>) -> Vec<u8> {
    range_end
        .as_ref()
        .map(|range_end| {
            range_end
                .iter()
                .map(|byte| byte.load(Ordering::SeqCst))
                .collect()
        })
        .unwrap_or_default()
}

/// The range end of a get or watch: the prefix end for a prefix operation.
fn request_range_end(key: &[u8], options: &MetaStorageOp) -> Vec<u8> {
    if options.is_opts_with_prefix {
        return get_prefix(key);
    }
    range_end_bytes(&options.range_end)
}

pub(crate) fn get_request(key: &[u8], opts: &[MetaStorageOption]) -> meta_storagepb::GetRequest {
    let options = apply_meta_storage_options(opts);
    meta_storagepb::GetRequest {
        header: None,
        key: key.to_vec(),
        range_end: request_range_end(key, &options),
        limit: options.limit,
        revision: options.revision,
    }
}

pub(crate) fn put_request(
    key: &[u8],
    value: &[u8],
    opts: &[MetaStorageOption],
) -> meta_storagepb::PutRequest {
    let options = apply_meta_storage_options(opts);
    meta_storagepb::PutRequest {
        header: None,
        key: key.to_vec(),
        value: value.to_vec(),
        lease: options.lease,
        prev_kv: options.prev_kv,
    }
}

pub(crate) fn watch_request(
    key: &[u8],
    opts: &[MetaStorageOption],
) -> meta_storagepb::WatchRequest {
    let options = apply_meta_storage_options(opts);
    meta_storagepb::WatchRequest {
        header: None,
        key: key.to_vec(),
        range_end: request_range_end(key, &options),
        start_revision: options.revision,
        prev_kv: options.prev_kv,
    }
}

/// gogo's text form of a meta storage error, as Go's `header.GetError().String()`.
pub(crate) fn meta_storage_error_text(error: &meta_storagepb::Error) -> String {
    let error_type = meta_storagepb::ErrorType::try_from(error.r#type)
        .map(|error_type| error_type.as_str_name().to_owned())
        .unwrap_or_else(|_| error.r#type.to_string());
    format!("type:{error_type} message:{:?} ", error.message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_prefix_follows_etcd() {
        assert_eq!(get_prefix(b"abc"), b"abd".to_vec());
        assert_eq!(get_prefix(&[b'a', 0xff]), vec![b'b']);
        assert_eq!(get_prefix(&[0xff, 0xff]), vec![0]);
        assert_eq!(get_prefix(b""), vec![0]);
    }
}
