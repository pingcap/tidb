// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;

use crate::proto::metapb;

/// Process-wide store request limit.
pub static STORE_LIMIT: AtomicI64 = AtomicI64::new(0);

/// Default transaction commit batch size.
pub const DEF_TXN_COMMIT_BATCH_SIZE: u64 = 16 * 1024;

/// Process-wide transaction commit batch size.
pub static TXN_COMMIT_BATCH_SIZE: AtomicU64 = AtomicU64::new(DEF_TXN_COMMIT_BATCH_SIZE);

/// Replica selection requested for reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaReadType {
    Leader,
    Follower,
    Mixed,
    Learner,
    PreferLeader,
    Unknown(u8),
}

impl ReplicaReadType {
    pub const fn is_follower_read(self) -> bool {
        !matches!(self, Self::Leader)
    }
}

/// Source selector settings retained by a sharded read plan. The selector
/// owns retry attempts separately; these are stable request-level inputs.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaReadConfig {
    pub read_type: ReplicaReadType,
    pub leader_only: bool,
    pub prefer_leader: bool,
    /// Source stale reads begin as mixed-replica reads but carry distinct
    /// TiKV context flags and retry transitions.
    pub stale_read: bool,
    pub labels: Vec<metapb::StoreLabel>,
    /// Source `WithMatchStores` input. Like client-go, this participates in
    /// the label-match score rather than making other replicas ineligible.
    pub stores: Vec<u64>,
    /// Source `Context.busy_threshold_ms`. A nonzero threshold lets TiKV
    /// reject overloaded reads and enables idle-replica routing.
    pub busy_threshold_ms: u32,
}

/// One source `locate.StoreSelectorOption` returned by a snapshot
/// replica-read adjuster. Client-go permits one optional selector option per
/// request adjustment; this enum preserves those observable option shapes.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplicaReadSelectorOption {
    MatchLabels(Vec<metapb::StoreLabel>),
    MatchStores(Vec<u64>),
    LeaderOnly,
    PreferLeader,
}

/// Per-read output from client-go's `ReplicaReadAdjuster` callback.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaReadAdjustment {
    pub selector: Option<ReplicaReadSelectorOption>,
    pub read_type: ReplicaReadType,
}

impl ReplicaReadAdjustment {
    pub const fn new(
        selector: Option<ReplicaReadSelectorOption>,
        read_type: ReplicaReadType,
    ) -> Self {
        Self {
            selector,
            read_type,
        }
    }
}

/// Native callback counterpart to client-go's `ReplicaReadAdjuster`.
///
/// The argument is the number of keys in the current get/batch-get request.
/// The callback is invoked only while the stable snapshot setting is a
/// follower-mode read, exactly as the source guards it.
pub type ReplicaReadAdjuster = Arc<dyn Fn(usize) -> ReplicaReadAdjustment + Send + Sync>;

impl ReplicaReadConfig {
    pub const fn leader() -> Self {
        Self {
            read_type: ReplicaReadType::Leader,
            leader_only: false,
            prefer_leader: false,
            stale_read: false,
            labels: Vec::new(),
            stores: Vec::new(),
            busy_threshold_ms: 0,
        }
    }

    pub const fn effective_prefer_leader(&self) -> bool {
        self.prefer_leader || matches!(self.read_type, ReplicaReadType::PreferLeader)
    }

    pub(crate) fn apply_adjustment(&mut self, adjustment: ReplicaReadAdjustment) {
        self.read_type = adjustment.read_type;
        match adjustment.selector {
            None => {}
            Some(ReplicaReadSelectorOption::MatchLabels(labels)) => self.labels.extend(labels),
            Some(ReplicaReadSelectorOption::MatchStores(stores)) => self.stores = stores,
            Some(ReplicaReadSelectorOption::LeaderOnly) => self.leader_only = true,
            Some(ReplicaReadSelectorOption::PreferLeader) => self.prefer_leader = true,
        }
    }
}

impl Default for ReplicaReadConfig {
    fn default() -> Self {
        Self::leader()
    }
}

impl From<u8> for ReplicaReadType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Leader,
            1 => Self::Follower,
            2 => Self::Mixed,
            3 => Self::Learner,
            4 => Self::PreferLeader,
            value => Self::Unknown(value),
        }
    }
}

impl From<ReplicaReadType> for u8 {
    fn from(value: ReplicaReadType) -> Self {
        match value {
            ReplicaReadType::Leader => 0,
            ReplicaReadType::Follower => 1,
            ReplicaReadType::Mixed => 2,
            ReplicaReadType::Learner => 3,
            ReplicaReadType::PreferLeader => 4,
            ReplicaReadType::Unknown(value) => value,
        }
    }
}

impl fmt::Display for ReplicaReadType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leader => formatter.write_str("leader"),
            Self::Follower => formatter.write_str("follower"),
            Self::Mixed => formatter.write_str("mixed"),
            Self::Learner => formatter.write_str("learner"),
            Self::PreferLeader => formatter.write_str("prefer-leader"),
            Self::Unknown(value) => write!(formatter, "unknown-{value}"),
        }
    }
}

/// Physical relationship between the caller and selected store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AccessLocationType {
    #[default]
    Unknown,
    LocalZone,
    CrossZone,
    Other(u8),
}

impl From<u8> for AccessLocationType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::LocalZone,
            2 => Self::CrossZone,
            value => Self::Other(value),
        }
    }
}

impl From<AccessLocationType> for u8 {
    fn from(value: AccessLocationType) -> Self {
        match value {
            AccessLocationType::Unknown => 0,
            AccessLocationType::LocalZone => 1,
            AccessLocationType::CrossZone => 2,
            AccessLocationType::Other(value) => value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn replica_read_names_and_follower_classification_match_client_go() {
        let cases = [
            (ReplicaReadType::Leader, "leader", false),
            (ReplicaReadType::Follower, "follower", true),
            (ReplicaReadType::Mixed, "mixed", true),
            (ReplicaReadType::Learner, "learner", true),
            (ReplicaReadType::PreferLeader, "prefer-leader", true),
            (ReplicaReadType::Unknown(9), "unknown-9", true),
        ];
        for (read_type, name, follower) in cases {
            assert_eq!(read_type.to_string(), name);
            assert_eq!(read_type.is_follower_read(), follower);
            let encoded: u8 = read_type.into();
            assert_eq!(ReplicaReadType::from(encoded), read_type);
        }
        assert_eq!(u8::from(AccessLocationType::Unknown), 0);
        assert_eq!(u8::from(AccessLocationType::LocalZone), 1);
        assert_eq!(u8::from(AccessLocationType::CrossZone), 2);
        assert_eq!(AccessLocationType::from(9), AccessLocationType::Other(9));
        assert_eq!(STORE_LIMIT.load(Ordering::SeqCst), 0);
        assert_eq!(
            TXN_COMMIT_BATCH_SIZE.load(Ordering::SeqCst),
            DEF_TXN_COMMIT_BATCH_SIZE
        );
    }

    #[test]
    fn replica_read_config_defaults_to_leader_and_prefer_leader_is_effective() {
        let config = ReplicaReadConfig::default();
        assert_eq!(config.read_type, ReplicaReadType::Leader);
        assert!(!config.effective_prefer_leader());
        assert_eq!(config.busy_threshold_ms, 0);

        let prefer = ReplicaReadConfig {
            read_type: ReplicaReadType::PreferLeader,
            ..Default::default()
        };
        assert!(prefer.effective_prefer_leader());
    }
}
