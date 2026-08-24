// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use crate::proto::kvrpcpb;

/// Priority for TiKV to execute a transactional command.
///
/// The default is [`Priority::Normal`]. Priority is carried in the request
/// context and applies to both reads and writes issued by a transaction or
/// snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum Priority {
    /// Normal command priority.
    #[default]
    Normal,
    /// Low command priority.
    Low,
    /// High command priority.
    High,
}

impl From<Priority> for kvrpcpb::CommandPri {
    fn from(priority: Priority) -> Self {
        match priority {
            Priority::Normal => kvrpcpb::CommandPri::Normal,
            Priority::Low => kvrpcpb::CommandPri::Low,
            Priority::High => kvrpcpb::CommandPri::High,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_matches_the_wire_values() {
        assert_eq!(
            kvrpcpb::CommandPri::from(Priority::Normal),
            kvrpcpb::CommandPri::Normal
        );
        assert_eq!(
            kvrpcpb::CommandPri::from(Priority::Low),
            kvrpcpb::CommandPri::Low
        );
        assert_eq!(
            kvrpcpb::CommandPri::from(Priority::High),
            kvrpcpb::CommandPri::High
        );
    }
}
