// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `internal/unionstore/art/art_snapshot.go`.

use super::iterator::ArtIterator;
use super::node::leaf;
use super::SharedTree;
use crate::error::StaticError;
use crate::transaction::arena::{ArenaAddress, MemdbCheckpoint, NULL_ADDRESS};

/// Go `Snapshot`: a getter for the stage[0] view of the tree.
#[derive(Clone)]
pub(crate) struct ArtSnapshot {
    tree: SharedTree,
    cp: MemdbCheckpoint,
}

impl ArtSnapshot {
    /// Go `ART.GetSnapshot`.
    pub(crate) fn new(tree: SharedTree) -> Self {
        let cp = super::read(&tree).get_snapshot_checkpoint();
        Self { tree, cp }
    }

    /// Go `Snapshot.Get`.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        let tree = super::read(&self.tree);
        let addr = tree.traverse(key);
        if addr.is_null() {
            return Err(StaticError::NotExist);
        }
        let vlog_addr = leaf::vlog_addr(tree.allocator.node_data(addr));
        if vlog_addr.is_null() {
            // A flags only key, act as value not exists
            return Err(StaticError::NotExist);
        }
        tree.allocator
            .vlog_allocator
            .snapshot_value(vlog_addr, &self.cp)
            .map(<[u8]>::to_vec)
            .ok_or(StaticError::NotExist)
    }

    /// Go `Snapshot.NewSnapshotIterator`.
    pub(crate) fn iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> SnapshotIterator {
        let mut inner = if reverse {
            ArtIterator::new(self.tree.clone(), lower, upper, true, false)
        } else {
            ArtIterator::new(self.tree.clone(), lower, upper, false, false)
        }
        .unwrap_or_else(|error| panic!("{error}"));
        inner.ignore_seq_no = true;
        super::read(&self.tree).allocator.snapshot_inc();
        let mut it = SnapshotIterator {
            inner: Some(inner),
            value_addr: NULL_ADDRESS,
            value: Vec::new(),
            cp: self.cp,
        };
        while !it.set_value() && it.valid() {
            let _ = it.inner().next();
        }
        it
    }

    /// Go `Snapshot.Close`.
    pub(crate) fn close(self) {}
}

/// Go `SnapIter`: an iterator over the stage[0] values, which keeps the
/// nodes it may still visit from reuse until it closes.
///
/// Go keeps the value as a slice into the value log, which a later in-place
/// swap of a same-length value writes through; this iterator keeps the
/// value's address and reads the log when asked.
pub(crate) struct SnapshotIterator {
    inner: Option<ArtIterator>,
    value_addr: ArenaAddress,
    value: Vec<u8>,
    cp: MemdbCheckpoint,
}

impl SnapshotIterator {
    fn inner(&mut self) -> &mut ArtIterator {
        self.inner.as_mut().expect("snapshot iterator is open")
    }

    pub(crate) fn valid(&self) -> bool {
        self.inner.as_ref().expect("snapshot iterator is open").valid()
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.inner.as_ref().expect("snapshot iterator is open").key()
    }

    /// Go `SnapIter.Value`.
    pub(crate) fn value(&mut self) -> &[u8] {
        self.value.clear();
        if !self.value_addr.is_null() {
            let inner = self.inner.as_ref().expect("snapshot iterator is open");
            let tree = super::read(inner.tree());
            self.value
                .extend_from_slice(tree.allocator.vlog_allocator.value(self.value_addr));
        }
        &self.value
    }

    /// Go `SnapIter.Next`.
    pub(crate) fn next(&mut self) -> Result<(), &'static str> {
        self.value_addr = NULL_ADDRESS;
        while self.valid() {
            self.inner().next()?;
            if self.set_value() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Go `SnapIter.Close`: releases the resources of the iterator and
    /// related version.
    pub(crate) fn close(self) {}

    /// Go `SnapIter.setValue`.
    fn set_value(&mut self) -> bool {
        if !self.valid() {
            return false;
        }
        let inner = self.inner.as_ref().expect("snapshot iterator is open");
        let tree = super::read(inner.tree());
        // Go `GetSnapshotValue`: the newest value written before the
        // checkpoint.
        let vlog = &tree.allocator.vlog_allocator;
        let addr = vlog.select_value_history(inner.curr_vlog_addr(), |addr| {
            !vlog.can_modify(Some(&self.cp), addr)
        });
        if addr.is_null() {
            return false;
        }
        self.value_addr = addr;
        true
    }
}

impl Drop for SnapshotIterator {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let tree = inner.tree().clone();
            inner.close();
            super::write(&tree).allocator.snapshot_dec();
        }
    }
}
