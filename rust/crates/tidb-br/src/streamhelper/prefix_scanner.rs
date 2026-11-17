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

//! Go `br/pkg/streamhelper/prefix_scanner.go`: a paged scan of every key
//! under a prefix.

use super::client::MetaStore;
use crate::errors::{self, Result};

/// Go `kv.Entry`.
pub type Entry = (Vec<u8>, Vec<u8>);

/// Go `source`: a KV storage that supports range scans.
pub trait Source {
    /// Go `Scan`: at most `limit` pairs of `[from, to)` in key order, and
    /// whether the range holds more.
    ///
    /// # Errors
    /// The storage's error.
    fn scan(&self, from: &[u8], to: &[u8], limit: usize) -> Result<(Vec<Entry>, bool)>;
}

/// Go `etcdSource`.
///
/// WARNING (Go's): pages may come from different revisions. That is
/// acceptable for computing the min backup ts or the ranges to back up.
pub struct EtcdSource<'a>(pub &'a dyn MetaStore);

impl Source for EtcdSource<'_> {
    fn scan(&self, from: &[u8], to: &[u8], limit: usize) -> Result<(Vec<Entry>, bool)> {
        self.0.scan(from, to, limit).map_err(|e| {
            errors::annotate(
                e,
                format!("failed to scan [{}, {})", tidb_redact::key(from), tidb_redact::key(to)),
            )
        })
    }
}

/// Go `prefixScanner`.
pub struct PrefixScanner<S> {
    src: S,
    next: Vec<u8>,
    end: Vec<u8>,
    done: bool,
}

/// Go `scanPrefix`.
pub fn scan_prefix<S: Source>(src: S, prefix: &str) -> PrefixScanner<S> {
    PrefixScanner {
        src,
        next: prefix.as_bytes().to_vec(),
        end: tikv_client::kv::prefix_next_key(prefix.as_bytes()),
        done: false,
    }
}

/// Go `scanEtcdPrefix`.
pub fn scan_etcd_prefix<'a>(cli: &'a dyn MetaStore, prefix: &str) -> PrefixScanner<EtcdSource<'a>> {
    scan_prefix(EtcdSource(cli), prefix)
}

impl<S: Source> PrefixScanner<S> {
    /// Go `Page`: one page of the source.
    ///
    /// # Errors
    /// The source's error.
    pub fn page(&mut self, size: usize) -> Result<Vec<Entry>> {
        let (kvs, more) = self.src.scan(&self.next, &self.end, size)?;
        match kvs.last() {
            // The next key after the last one read.
            Some((last, _)) if more => {
                self.next = last.clone();
                self.next.push(0);
            }
            _ => self.done = true,
        }
        Ok(kvs)
    }

    /// Go `AllPages`: every page, in order.
    ///
    /// # Errors
    /// The source's error.
    pub fn all_pages(mut self, size: usize) -> Result<Vec<Entry>> {
        let mut kvs = Vec::with_capacity(size);
        while !self.done() {
            kvs.extend(self.page(size)?);
        }
        Ok(kvs)
    }

    /// Go `Done`: whether the scan read everything in the range.
    #[must_use]
    pub fn done(&self) -> bool {
        self.done
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use super::*;

    struct Paged {
        kvs: BTreeMap<Vec<u8>, Vec<u8>>,
        calls: RefCell<Vec<(Vec<u8>, Vec<u8>, usize)>>,
    }

    impl Source for &Paged {
        fn scan(&self, from: &[u8], to: &[u8], limit: usize) -> Result<(Vec<Entry>, bool)> {
            self.calls.borrow_mut().push((from.to_vec(), to.to_vec(), limit));
            let mut in_range =
                self.kvs.range(from.to_vec()..to.to_vec()).map(|(k, v)| (k.clone(), v.clone()));
            let page: Vec<Entry> = in_range.by_ref().take(limit).collect();
            Ok((page, in_range.next().is_some()))
        }
    }

    /// Go `prefixScanner.AllPages`: one request per page of `size`, each
    /// resuming after the last key read, until the range has no more;
    /// keys beside the prefix are not read.
    #[test]
    fn all_pages_reads_the_prefix_page_by_page() {
        let kvs: BTreeMap<_, _> = ["/a", "/p/1", "/p/2", "/p/3", "/p/4", "/p/5", "/q"]
            .iter()
            .map(|k| (k.as_bytes().to_vec(), k.as_bytes().to_vec()))
            .collect();
        let src = Paged { kvs, calls: RefCell::default() };
        let read = scan_prefix(&src, "/p/").all_pages(2).unwrap();
        let keys: Vec<_> = read.iter().map(|(k, _)| String::from_utf8_lossy(k).into_owned()).collect();
        assert_eq!(keys, ["/p/1", "/p/2", "/p/3", "/p/4", "/p/5"]);
        let calls = src.calls.borrow();
        let froms: Vec<_> = calls.iter().map(|(from, _, _)| from.clone()).collect();
        assert_eq!(froms, [b"/p/".to_vec(), b"/p/2\0".to_vec(), b"/p/4\0".to_vec()]);
        assert!(calls.iter().all(|(_, to, limit)| to == b"/p0" && *limit == 2));
    }

    /// A page that reads the range's last key ends the scan: etcd answers
    /// `More` false, so no further request is made.
    #[test]
    fn a_full_last_page_ends_the_scan() {
        let kvs: BTreeMap<_, _> =
            ["/p/1", "/p/2"].iter().map(|k| (k.as_bytes().to_vec(), Vec::new())).collect();
        let src = Paged { kvs, calls: RefCell::default() };
        let mut scanner = scan_prefix(&src, "/p/");
        assert_eq!(scanner.page(2).unwrap().len(), 2);
        assert!(scanner.done());
        assert_eq!(src.calls.borrow().len(), 1);
    }
}
