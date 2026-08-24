// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

/// Byte width of [`KeyFlags`].
pub const FLAG_BYTES: usize = 2;

const FLAG_PRESUME_KEY_NOT_EXISTS: u16 = 1 << 0;
const FLAG_KEY_LOCKED: u16 = 1 << 1;
const FLAG_NEED_LOCKED: u16 = 1 << 2;
const FLAG_KEY_LOCKED_VALUE_EXISTS: u16 = 1 << 3;
const FLAG_NEED_CHECK_EXISTS: u16 = 1 << 4;
const FLAG_PREWRITE_ONLY: u16 = 1 << 5;
const FLAG_IGNORED_IN_2PC: u16 = 1 << 6;
const FLAG_READABLE: u16 = 1 << 7;
const FLAG_NEWLY_INSERTED: u16 = 1 << 8;
const FLAG_ASSERT_EXIST: u16 = 1 << 9;
const FLAG_ASSERT_NOT_EXIST: u16 = 1 << 10;
const FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE: u16 = 1 << 11;
const FLAG_PREVIOUS_PRESUME_KEY_NOT_EXISTS: u16 = 1 << 12;
const FLAG_KEY_LOCKED_IN_SHARE_MODE: u16 = 1 << 13;
const PERSISTENT_FLAGS: u16 = FLAG_KEY_LOCKED
    | FLAG_KEY_LOCKED_VALUE_EXISTS
    | FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE
    | FLAG_KEY_LOCKED_IN_SHARE_MODE;

/// Metadata associated with a transaction-buffer key.
///
/// The high bit remains unused because client-go reserves it for its red-black
/// tree implementation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct KeyFlags(u16);

impl KeyFlags {
    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    pub const fn has_assert_exist(self) -> bool {
        self.0 & FLAG_ASSERT_EXIST != 0 && self.0 & FLAG_ASSERT_NOT_EXIST == 0
    }

    pub const fn has_assert_not_exist(self) -> bool {
        self.0 & FLAG_ASSERT_NOT_EXIST != 0 && self.0 & FLAG_ASSERT_EXIST == 0
    }

    pub const fn has_assert_unknown(self) -> bool {
        self.0 & FLAG_ASSERT_EXIST != 0 && self.0 & FLAG_ASSERT_NOT_EXIST != 0
    }

    pub const fn has_assertion_flags(self) -> bool {
        self.0 & (FLAG_ASSERT_EXIST | FLAG_ASSERT_NOT_EXIST) != 0
    }

    pub const fn has_presume_key_not_exists(self) -> bool {
        self.0 & (FLAG_PRESUME_KEY_NOT_EXISTS | FLAG_PREVIOUS_PRESUME_KEY_NOT_EXISTS) != 0
    }

    pub const fn has_locked(self) -> bool {
        self.0 & FLAG_KEY_LOCKED != 0
    }

    pub const fn has_locked_in_share_mode(self) -> bool {
        self.0 & FLAG_KEY_LOCKED_IN_SHARE_MODE != 0
    }

    pub const fn has_need_locked(self) -> bool {
        self.0 & FLAG_NEED_LOCKED != 0
    }

    pub const fn has_locked_value_exists(self) -> bool {
        self.0 & FLAG_KEY_LOCKED_VALUE_EXISTS != 0
    }

    pub const fn has_need_check_exists(self) -> bool {
        self.0 & FLAG_NEED_CHECK_EXISTS != 0
    }

    pub const fn has_prewrite_only(self) -> bool {
        self.0 & FLAG_PREWRITE_ONLY != 0
    }

    pub const fn has_ignored_in_2pc(self) -> bool {
        self.0 & FLAG_IGNORED_IN_2PC != 0
    }

    pub const fn has_readable(self) -> bool {
        self.0 & FLAG_READABLE != 0
    }

    pub const fn has_need_constraint_check_in_prewrite(self) -> bool {
        self.0 & FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE != 0
    }

    pub const fn and_persistent(self) -> Self {
        Self(self.0 & PERSISTENT_FLAGS)
    }

    pub const fn has_newly_inserted(self) -> bool {
        self.0 & FLAG_NEWLY_INSERTED != 0
    }
}

/// A single mutation of [`KeyFlags`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum FlagsOp {
    SetPresumeKeyNotExists = 1 << 0,
    DelPresumeKeyNotExists = 1 << 1,
    SetKeyLocked = 1 << 2,
    DelKeyLocked = 1 << 3,
    SetNeedLocked = 1 << 4,
    DelNeedLocked = 1 << 5,
    SetKeyLockedValueExists = 1 << 6,
    SetKeyLockedValueNotExists = 1 << 7,
    DelNeedCheckExists = 1 << 8,
    SetPrewriteOnly = 1 << 9,
    SetIgnoredIn2Pc = 1 << 10,
    SetReadable = 1 << 11,
    SetNewlyInserted = 1 << 12,
    SetAssertExist = 1 << 13,
    SetAssertNotExist = 1 << 14,
    SetAssertUnknown = 1 << 15,
    SetAssertNone = 1 << 16,
    SetNeedConstraintCheckInPrewrite = 1 << 17,
    DelNeedConstraintCheckInPrewrite = 1 << 18,
    SetPreviousPresumeKeyNotExists = 1 << 19,
    SetKeyLockedInShareMode = 1 << 20,
    SetKeyLockedInExclusiveMode = 1 << 21,
}

/// Apply flag operations in order.
pub fn apply_flags_ops(mut origin: KeyFlags, operations: &[FlagsOp]) -> KeyFlags {
    for operation in operations {
        match operation {
            FlagsOp::SetPresumeKeyNotExists => {
                origin.0 |= FLAG_PRESUME_KEY_NOT_EXISTS | FLAG_NEED_CHECK_EXISTS
            }
            FlagsOp::DelPresumeKeyNotExists => {
                origin.0 &= !(FLAG_PRESUME_KEY_NOT_EXISTS | FLAG_NEED_CHECK_EXISTS)
            }
            FlagsOp::SetKeyLocked => origin.0 |= FLAG_KEY_LOCKED,
            FlagsOp::DelKeyLocked => origin.0 &= !FLAG_KEY_LOCKED,
            FlagsOp::SetNeedLocked => origin.0 |= FLAG_NEED_LOCKED,
            FlagsOp::DelNeedLocked => origin.0 &= !FLAG_NEED_LOCKED,
            FlagsOp::SetKeyLockedValueExists => {
                origin.0 |= FLAG_KEY_LOCKED_VALUE_EXISTS;
                origin.0 &= !FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE;
            }
            FlagsOp::DelNeedCheckExists => origin.0 &= !FLAG_NEED_CHECK_EXISTS,
            FlagsOp::SetKeyLockedValueNotExists => {
                origin.0 &= !FLAG_KEY_LOCKED_VALUE_EXISTS;
                origin.0 &= !FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE;
            }
            FlagsOp::SetPrewriteOnly => origin.0 |= FLAG_PREWRITE_ONLY,
            FlagsOp::SetIgnoredIn2Pc => origin.0 |= FLAG_IGNORED_IN_2PC,
            FlagsOp::SetReadable => origin.0 |= FLAG_READABLE,
            FlagsOp::SetNewlyInserted => origin.0 |= FLAG_NEWLY_INSERTED,
            FlagsOp::SetAssertExist => {
                origin.0 &= !FLAG_ASSERT_NOT_EXIST;
                origin.0 |= FLAG_ASSERT_EXIST;
            }
            FlagsOp::SetAssertNotExist => {
                origin.0 &= !FLAG_ASSERT_EXIST;
                origin.0 |= FLAG_ASSERT_NOT_EXIST;
            }
            FlagsOp::SetAssertUnknown => {
                origin.0 |= FLAG_ASSERT_NOT_EXIST | FLAG_ASSERT_EXIST;
            }
            FlagsOp::SetAssertNone => {
                origin.0 &= !(FLAG_ASSERT_EXIST | FLAG_ASSERT_NOT_EXIST);
            }
            FlagsOp::SetNeedConstraintCheckInPrewrite => {
                origin.0 |= FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE
            }
            FlagsOp::DelNeedConstraintCheckInPrewrite => {
                origin.0 &= !FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE
            }
            FlagsOp::SetPreviousPresumeKeyNotExists => {
                origin.0 |= FLAG_PREVIOUS_PRESUME_KEY_NOT_EXISTS
            }
            FlagsOp::SetKeyLockedInShareMode => origin.0 |= FLAG_KEY_LOCKED_IN_SHARE_MODE,
            FlagsOp::SetKeyLockedInExclusiveMode => origin.0 &= !FLAG_KEY_LOCKED_IN_SHARE_MODE,
        }
    }
    origin
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_operations_preserve_source_implications_and_assertions() {
        let flags = apply_flags_ops(
            KeyFlags::default(),
            &[
                FlagsOp::SetPresumeKeyNotExists,
                FlagsOp::SetNeedConstraintCheckInPrewrite,
                FlagsOp::SetKeyLockedValueExists,
                FlagsOp::SetKeyLocked,
                FlagsOp::SetKeyLockedInShareMode,
                FlagsOp::SetAssertNotExist,
            ],
        );
        assert!(flags.has_presume_key_not_exists());
        assert!(flags.has_need_check_exists());
        assert!(flags.has_locked());
        assert!(flags.has_locked_in_share_mode());
        assert!(flags.has_locked_value_exists());
        assert!(!flags.has_need_constraint_check_in_prewrite());
        assert!(flags.has_assert_not_exist());
        assert_eq!(
            flags.and_persistent().bits(),
            FLAG_KEY_LOCKED | FLAG_KEY_LOCKED_VALUE_EXISTS | FLAG_KEY_LOCKED_IN_SHARE_MODE
        );

        let unknown = apply_flags_ops(flags, &[FlagsOp::SetAssertUnknown]);
        assert!(unknown.has_assert_unknown());
        let none = apply_flags_ops(unknown, &[FlagsOp::SetAssertNone]);
        assert!(!none.has_assertion_flags());
    }

    #[test]
    fn every_flag_operation_matches_its_inverse_or_single_effect() {
        assert_eq!(FlagsOp::SetPresumeKeyNotExists as u32, 1);
        assert_eq!(FlagsOp::SetKeyLockedInExclusiveMode as u32, 1 << 21);
        let flags = apply_flags_ops(
            KeyFlags::default(),
            &[
                FlagsOp::SetNeedLocked,
                FlagsOp::SetPrewriteOnly,
                FlagsOp::SetIgnoredIn2Pc,
                FlagsOp::SetReadable,
                FlagsOp::SetNewlyInserted,
                FlagsOp::SetPreviousPresumeKeyNotExists,
                FlagsOp::SetAssertExist,
            ],
        );
        assert!(flags.has_need_locked());
        assert!(flags.has_prewrite_only());
        assert!(flags.has_ignored_in_2pc());
        assert!(flags.has_readable());
        assert!(flags.has_newly_inserted());
        assert!(flags.has_presume_key_not_exists());
        assert!(flags.has_assert_exist());

        let flags = apply_flags_ops(
            flags,
            &[
                FlagsOp::DelNeedLocked,
                FlagsOp::SetKeyLocked,
                FlagsOp::DelKeyLocked,
                FlagsOp::DelPresumeKeyNotExists,
                FlagsOp::SetKeyLockedInExclusiveMode,
            ],
        );
        assert!(!flags.has_need_locked());
        assert!(!flags.has_locked());
        assert!(!flags.has_locked_in_share_mode());
        assert!(flags.has_presume_key_not_exists());
    }
}
