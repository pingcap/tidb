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

//! Table-owned duplicate-check and assertion selection, following Go AddRecord.
use tidb_txnkv::{
    transaction::{BufferMutation, MutationSetError},
    AssertionOp,
};
/// Absence established by the table caller, not inferred by the KV buffer.
#[derive(Clone, Copy)]
pub(crate) enum AbsenceCheck {
    /// A transaction-view lookup or exclusively allocated handle proves absence.
    Checked,
    /// Absence is deferred to commit validation.
    Lazy,
}
/// Go AddRecord/index.Create assertion selection. The retained configured
/// writer is optimistic; the pessimistic branch alone does not supply Go
/// NeedConstraintCheckInPrewrite or compose a pessimistic table lifecycle.
pub(crate) fn insert_record(
    key: Vec<u8>,
    value: Vec<u8>,
    check: AbsenceCheck,
    pessimistic: bool,
) -> Result<BufferMutation, MutationSetError> {
    let lazy = matches!(check, AbsenceCheck::Lazy);
    let assertion = if lazy && !pessimistic {
        AssertionOp::AssertUnknown
    } else {
        AssertionOp::AssertNotExist
    };
    BufferMutation::set_with_flags(key, value, lazy, assertion)
}
/// Go index.Create for optimistic lazy insertion. Non-distinct keys contain
/// the row handle and need an assertion but no unique-key constraint check.
/// Non-public indexes suppress assertions while retaining duplicate flags.
pub(crate) fn insert_index(
    key: Vec<u8>,
    value: Vec<u8>,
    distinct: bool,
    public: bool,
) -> Result<BufferMutation, MutationSetError> {
    BufferMutation::set_with_flags(
        key,
        value,
        distinct,
        if public {
            AssertionOp::AssertUnknown
        } else {
            AssertionOp::AssertNone
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn optimistic_lazy_keeps_constraint_check_without_claiming_snapshot_absence() {
        let m = insert_record(b"r".to_vec(), b"v".to_vec(), AbsenceCheck::Lazy, false).unwrap();
        assert!(m.presume_not_exists());
        assert_eq!(m.assertion(), AssertionOp::AssertUnknown);
    }
    #[test]
    fn checked_absence_does_not_request_a_second_lazy_check() {
        let m = insert_record(b"r".to_vec(), b"v".to_vec(), AbsenceCheck::Checked, false).unwrap();
        assert!(!m.presume_not_exists());
        assert_eq!(m.assertion(), AssertionOp::AssertNotExist);
    }
    #[test]
    fn pessimistic_lazy_keeps_not_exist_assertion() {
        let m = insert_record(b"r".to_vec(), b"v".to_vec(), AbsenceCheck::Lazy, true).unwrap();
        assert!(m.presume_not_exists());
        assert_eq!(m.assertion(), AssertionOp::AssertNotExist);
    }
}
