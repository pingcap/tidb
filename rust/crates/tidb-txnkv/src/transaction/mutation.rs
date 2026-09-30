// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::AssertionOp;

/// The operation a statement applies to the transaction's MemDB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BufferMutationOp {
    /// Store a value, with flags and assertions carried independently.
    Set,
    /// Store a deletion tombstone.
    Delete,
    /// Keep a locked key without changing its value.
    Lock,
}

/// An owned statement write. Commit derives TiKV mutations from MemDB flags.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferMutation {
    op: BufferMutationOp,
    key: Vec<u8>,
    value: Vec<u8>,
    presume_not_exists: bool,
    assertion: AssertionOp,
}

impl BufferMutation {
    /// Sets an encoded value without an existence assertion.
    pub fn set(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        Self::new(BufferMutationOp::Set, key.into(), value.into())
    }
    /// Sets a value with a lazy absence check and a not-exists assertion.
    pub fn insert(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        let mut mutation = Self::set(key, value)?;
        mutation.presume_not_exists = true;
        mutation.assertion = AssertionOp::AssertNotExist;
        Ok(mutation)
    }
    /// Sets a value with the table layer's exists assertion.
    pub fn put_existing(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        let mut mutation = Self::set(key, value)?;
        mutation.assertion = AssertionOp::AssertExist;
        Ok(mutation)
    }
    /// Deletes a key without an existence assertion.
    pub fn delete(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        Self::new(BufferMutationOp::Delete, key.into(), Vec::new())
    }
    /// Deletes a key with the table layer's exists assertion.
    pub fn delete_existing(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        let mut mutation = Self::delete(key)?;
        mutation.assertion = AssertionOp::AssertExist;
        Ok(mutation)
    }
    /// Keeps a locked key whose value is unchanged.
    pub fn lock_only(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        Self::new(BufferMutationOp::Lock, key.into(), Vec::new())
    }
    fn new(op: BufferMutationOp, key: Vec<u8>, value: Vec<u8>) -> Result<Self, MutationSetError> {
        if key.is_empty() {
            return Err(MutationSetError::EmptyKey);
        }
        Ok(Self {
            op,
            key,
            value,
            presume_not_exists: false,
            assertion: AssertionOp::AssertNone,
        })
    }
    /// MemDB operation, independent of SQL table or index categories.
    pub const fn kind(&self) -> BufferMutationOp {
        self.op
    }
    /// Lazy absence-check flag requested by this write.
    pub const fn presume_not_exists(&self) -> bool {
        self.presume_not_exists
    }
    /// Existence assertion requested by this write.
    pub const fn assertion(&self) -> AssertionOp {
        self.assertion
    }
    /// Encoded TiKV key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }
    /// Encoded value; empty for tombstones and locks.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
    /// Moves the write and its independent metadata into the authoritative buffer.
    pub fn into_parts(self) -> (BufferMutationOp, Vec<u8>, Vec<u8>, bool, AssertionOp) {
        (
            self.op,
            self.key,
            self.value,
            self.presume_not_exists,
            self.assertion,
        )
    }
}

/// Invalid statement mutation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationSetError {
    /// The statement requires a mutation plan.
    Empty,
    /// TiKV user keys must not be empty.
    EmptyKey,
}
impl std::fmt::Display for MutationSetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("statement requires mutations"),
            Self::EmptyKey => f.write_str("mutation key is empty"),
        }
    }
}
impl std::error::Error for MutationSetError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_does_not_impose_storage_size_limits() {
        // Only the configured MemDB owns entry-size validation.
        assert!(BufferMutation::insert(vec![1; 4097], vec![2]).is_ok());
        assert!(BufferMutation::insert(vec![1], vec![2; 6 * 1024 * 1024 + 1]).is_ok());
    }

    #[test]
    fn empty_keys_fail_before_storage() {
        assert_eq!(
            BufferMutation::insert(Vec::new(), b"v".to_vec()),
            Err(MutationSetError::EmptyKey)
        );
    }
}
