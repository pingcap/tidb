// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use crate::proto::kvrpcpb;

/// Priority for TiKV to execute a transactional command.
///
/// Client-go defines this as an integer-backed protobuf enum, so values that
/// are newer than this client's generated protobuf definitions must still be
/// retained on the wire. The default is [`Priority::Normal`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct Priority(i32);

#[allow(non_upper_case_globals)]
impl Priority {
    /// Normal command priority.
    pub const Normal: Self = Self(kvrpcpb::CommandPri::Normal as i32);
    /// Low command priority.
    pub const Low: Self = Self(kvrpcpb::CommandPri::Low as i32);
    /// High command priority.
    pub const High: Self = Self(kvrpcpb::CommandPri::High as i32);

    /// Constructs a priority from its protobuf numeric value.
    pub const fn from_i32(value: i32) -> Self {
        Self(value)
    }

    /// Returns the protobuf numeric value written to request contexts.
    pub const fn to_pb(self) -> i32 {
        self.0
    }
}

impl Default for Priority {
    fn default() -> Self {
        Self::Normal
    }
}

impl From<kvrpcpb::CommandPri> for Priority {
    fn from(priority: kvrpcpb::CommandPri) -> Self {
        Self(priority as i32)
    }
}

impl From<i32> for Priority {
    fn from(priority: i32) -> Self {
        Self(priority)
    }
}

impl From<Priority> for i32 {
    fn from(priority: Priority) -> Self {
        priority.to_pb()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_preserves_known_and_future_wire_values() {
        assert_eq!(Priority::Normal.to_pb(), kvrpcpb::CommandPri::Normal as i32);
        assert_eq!(Priority::Low.to_pb(), kvrpcpb::CommandPri::Low as i32);
        assert_eq!(Priority::High.to_pb(), kvrpcpb::CommandPri::High as i32);

        let future = Priority::from_i32(99);
        assert_eq!(future.to_pb(), 99);
        assert_eq!(Priority::from(99), future);
    }
}
