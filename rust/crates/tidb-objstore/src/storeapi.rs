//! Go `pkg/objstore/storeapi`: the storage interface and its options.

use std::time::Duration;

use tidb_gocontext::Context;

use crate::error::Result;
use crate::objectio::{Reader, Writer};

/// Go `Permission`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permission {
    /// Go `AccessBuckets`.
    AccessBuckets,
    /// Go `ListObjects`.
    ListObjects,
    /// Go `GetObject`.
    GetObject,
    /// Go `PutObject`.
    PutObject,
    /// Go `PutAndDeleteObject`.
    PutAndDeleteObject,
}

/// Go `Features`.
impl Permission {
    /// Go's `Permission` string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccessBuckets => "AccessBucket",
            Self::ListObjects => "ListObjects",
            Self::GetObject => "GetObject",
            Self::PutObject => "PutObject",
            Self::PutAndDeleteObject => "PutAndDeleteObject",
        }
    }
}

pub type Features = u64;
/// Go `FeatureStrongConsistency`.
pub const FEATURE_STRONG_CONSISTENCY: Features = 1;
/// Go `FeatureSupportsStartAfter`.
pub const FEATURE_SUPPORTS_START_AFTER: Features = 1 << 1;

/// Go `WalkOption`.
#[derive(Clone, Debug, Default)]
pub struct WalkOption {
    /// Go `SubDir`: walk only this sub-directory.
    pub sub_dir: String,
    /// Go `SkipSubDir`: do not descend into sub-directories (local storage).
    pub skip_sub_dir: bool,
    /// Go `ObjPrefix`: only objects whose name under `sub_dir` starts so.
    pub obj_prefix: String,
    /// Go `ListCount`: the page size of one list request.
    pub list_count: i64,
    /// Go `IncludeTombstone`.
    pub include_tombstone: bool,
    /// Go `StartAfter`: only objects after this key.
    pub start_after: String,
}

/// Go `WriterOption`.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriterOption {
    /// Go `Concurrency`.
    pub concurrency: usize,
    /// Go `PartSize`.
    pub part_size: i64,
}

/// Go `ReaderOption`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReaderOption {
    /// Go `StartOffset`.
    pub start_offset: Option<i64>,
    /// Go `EndOffset`: exclusive.
    pub end_offset: Option<i64>,
    /// Go `PrefetchSize`.
    pub prefetch_size: usize,
}

/// Go `Storage`.
pub trait Storage: Send + Sync {
    /// Go `WriteFile`.
    fn write_file(&self, ctx: &Context, name: &str, data: &[u8]) -> Result<()>;
    /// Go `ReadFile`.
    fn read_file(&self, ctx: &Context, name: &str) -> Result<Vec<u8>>;
    /// Go `FileExists`.
    fn file_exists(&self, ctx: &Context, name: &str) -> Result<bool>;
    /// Go `DeleteFile`.
    fn delete_file(&self, ctx: &Context, name: &str) -> Result<()>;
    /// Go `Open`.
    fn open(&self, ctx: &Context, path: &str, option: Option<&ReaderOption>) -> Result<Box<dyn Reader>>;
    /// Go `DeleteFiles`.
    fn delete_files(&self, ctx: &Context, names: &[String]) -> Result<()>;
    /// Go `WalkDir`: calls `f(path, size)` for every object, in key order.
    fn walk_dir(
        &self,
        ctx: &Context,
        opt: Option<&WalkOption>,
        f: &mut dyn FnMut(&str, i64) -> Result<()>,
    ) -> Result<()>;
    /// Go `URI`.
    fn uri(&self) -> String;
    /// Go `Create`.
    fn create(&self, ctx: &Context, path: &str, option: Option<&WriterOption>) -> Result<Box<dyn Writer>>;
    /// Go `Rename`.
    fn rename(&self, ctx: &Context, old_file_name: &str, new_file_name: &str) -> Result<()>;
    /// Go `PresignFile`.
    fn presign_file(&self, ctx: &Context, file_name: &str, expire: Duration) -> Result<String>;
    /// Go `Close`.
    fn close(&self) {}
    /// Go `FeatureProvider.Features` (0 when the storage has none).
    fn features(&self) -> Features {
        0
    }
    /// Go's `storage.(Copier)` assertion: the storage as a [`Copier`].
    fn as_copier(&self) -> Option<&dyn Copier> {
        None
    }
    /// The concrete storage, for a [`Copier`] reading its source's type.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }
}

/// Go `CopySpec`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopySpec {
    /// Go `From`: the key in the source storage.
    pub from: String,
    /// Go `To`: the key in this storage.
    pub to: String,
}

/// Go `Copier`.
pub trait Copier {
    /// Go `CopyFrom`: copies an object of `e` into this storage.
    fn copy_from(&self, ctx: &Context, e: &dyn Storage, spec: &CopySpec) -> Result<()>;
}

/// Go `Options`.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// Go `SendCredentials`.
    pub send_credentials: bool,
    /// Go `NoCredentials`.
    pub no_credentials: bool,
    /// Go `CheckPermissions`.
    pub check_permissions: Vec<Permission>,
    /// Go `CheckS3ObjectLockOptions`.
    pub check_s3_object_lock_options: bool,
    /// Go `AccessRecording`.
    pub access_recording: Option<std::sync::Arc<tidb_objstore_recording::AccessStats>>,
}

/// Go `FeatureOf`.
pub fn feature_of(storage: &dyn Storage) -> Features {
    storage.features()
}

/// Go `Prefix`: a key prefix that is empty or ends with `/`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Prefix(String);

impl Prefix {
    /// Go `NewPrefix`.
    #[must_use]
    pub fn new(prefix: &str) -> Self {
        let p = prefix.trim_matches('/');
        if p.is_empty() {
            Self(String::new())
        } else {
            Self(format!("{p}/"))
        }
    }

    /// Go `Prefix.JoinStr`.
    #[must_use]
    pub fn join_str(&self, s: &str) -> Self {
        Self(format!("{}{}", self.0, Self::new(s).0))
    }

    /// Go `Prefix.ObjectKey`.
    #[must_use]
    pub fn object_key(&self, name: &str) -> String {
        format!("{}{name}", self.0)
    }

    /// Go `Prefix.ToPath`.
    #[must_use]
    pub fn to_path(&self) -> String {
        format!("/{}", self.0)
    }

    /// Go `Prefix.String`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Go `BucketPrefix`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BucketPrefix {
    /// Go `Bucket`.
    pub bucket: String,
    /// Go `Prefix`.
    pub prefix: Prefix,
}

impl BucketPrefix {
    /// Go `NewBucketPrefix`.
    #[must_use]
    pub fn new(bucket: &str, prefix: &str) -> Self {
        Self { bucket: bucket.to_owned(), prefix: Prefix::new(prefix) }
    }

    /// Go `ObjectKey`.
    #[must_use]
    pub fn object_key(&self, name: &str) -> String {
        self.prefix.object_key(name)
    }

    /// Go `PrefixStr`.
    #[must_use]
    pub fn prefix_str(&self) -> &str {
        self.prefix.as_str()
    }
}

/// Go `GetHTTPRange`: `(full, range)` for `[start, end)`, `end == 0`
/// meaning to the end.
#[must_use]
pub fn get_http_range(start_offset: i64, end_offset: i64) -> (bool, String) {
    if end_offset > start_offset {
        (false, format!("bytes={start_offset}-{}", end_offset - 1))
    } else if start_offset == 0 {
        (true, String::new())
    } else {
        (false, format!("bytes={start_offset}-"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        assert_eq!(Prefix::new("").as_str(), "");
        assert_eq!(Prefix::new("/a/b/").as_str(), "a/b/");
        assert_eq!(Prefix::new("a").join_str("/c/").object_key("x"), "a/c/x");
        assert_eq!(Prefix::new("a").to_path(), "/a/");
    }

    #[test]
    fn test_get_http_range() {
        assert_eq!(get_http_range(0, 0), (true, String::new()));
        assert_eq!(get_http_range(5, 0), (false, "bytes=5-".to_owned()));
        assert_eq!(get_http_range(5, 10), (false, "bytes=5-9".to_owned()));
    }
}
