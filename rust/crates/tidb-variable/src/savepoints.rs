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

//! A transaction's savepoint stack: Go `TxnCtx.Savepoints` with its
//! `AddSavepoint`, `RollbackToSavepoint` and `ReleaseSavepoint`
//! (`pkg/sessionctx/variable/session.go`), once.
//!
//! The stack owns the RULES: names match case-insensitively
//! (`strings.ToLower`), redefining a name deletes the old entry and appends
//! the new one at the end, `ROLLBACK TO` truncates to `[:idx+1]` so the named
//! savepoint survives its own rollback, and `RELEASE` truncates to `[:idx]`.
//! What a savepoint RECORDS differs by storage tier -- the in-process tier's
//! working catalog image, the cluster node's mutation-buffer checkpoint --
//! and is the type parameter `C`, which Go has as `tikv.MemDBCheckpoint`.

use tidb_hack::go_to_lower;

/// A transaction's savepoints, oldest first.
#[derive(Debug)]
pub struct SavepointStack<C> {
    entries: Vec<(String, C)>,
}

impl<C> Default for SavepointStack<C> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<C> SavepointStack<C> {
    /// Go `TxnCtx.AddSavepoint`: delete any savepoint of the same name, then
    /// append this one. The redefinition moves the name to the END, so the
    /// savepoints taken after the original are no longer after it.
    pub fn add(&mut self, name: &str, checkpoint: C) {
        let name = go_to_lower(name);
        self.entries.retain(|(existing, _)| *existing != name);
        self.entries.push((name, checkpoint));
    }

    /// Go `TxnCtx.RollbackToSavepoint`: the named checkpoint, with every
    /// savepoint after it dropped and the named one kept. `None` for an
    /// unknown name, which the caller reports as 1305.
    pub fn rollback_to(&mut self, name: &str) -> Option<&C> {
        let index = self.position(name)?;
        self.entries.truncate(index + 1);
        Some(&self.entries[index].1)
    }

    /// Go `TxnCtx.ReleaseSavepoint`: drops the named savepoint and every one
    /// taken after it. Whether the name existed.
    pub fn release(&mut self, name: &str) -> bool {
        match self.position(name) {
            Some(index) => {
                self.entries.truncate(index);
                true
            }
            None => false,
        }
    }

    /// Whether a savepoint of this name exists.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.position(name).is_some()
    }

    /// Whether the transaction has declared no savepoint.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The transaction ended: Go builds a fresh `TxnCtx`.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    fn position(&self, name: &str) -> Option<usize> {
        let name = go_to_lower(name);
        self.entries
            .iter()
            .position(|(existing, _)| *existing == name)
    }
}

#[cfg(test)]
mod tests {
    use super::SavepointStack;

    #[test]
    fn names_match_case_insensitively_and_redefinition_moves_to_the_end() {
        let mut stack = SavepointStack::default();
        stack.add("A", 1);
        stack.add("b", 2);
        stack.add("a", 3);
        assert_eq!(stack.rollback_to("B"), Some(&2));
        // `a` was redefined after `b`, so it is gone with the truncation.
        assert!(!stack.contains("a"));
        assert!(stack.contains("b"));
    }

    #[test]
    fn rollback_keeps_the_named_savepoint_and_release_drops_it() {
        let mut stack = SavepointStack::default();
        stack.add("a", 1);
        stack.add("b", 2);
        stack.add("c", 3);
        assert_eq!(stack.rollback_to("b"), Some(&2));
        assert!(stack.contains("b") && !stack.contains("c"));
        assert_eq!(stack.rollback_to("b"), Some(&2));
        assert!(stack.release("b"));
        assert!(!stack.contains("b") && stack.contains("a"));
        assert!(!stack.release("b"));
        assert_eq!(stack.rollback_to("zz"), None);
    }
}
