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

use std::time::Duration;

use tidb_proto::{errorpb, metapb};

use crate::retry::{RegionBackoffBudget, RegionBackoffKind};

use super::{
    BucketMetadata, PeerRole, RegionCache, RegionLoadError, RegionMetadata, RegionMetadataPeer,
    RegionRecoveryLoader, RegionRouteError, RegionVerId, SelectorRecovery,
};

/// Exact route observation attached to one failed TiKV request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionAttempt {
    /// Exact versioned region used by the request.
    pub region: RegionVerId,
    /// Peer selected by the request.
    pub peer_id: u64,
    /// Store selected by the request.
    pub store_id: u64,
    /// Address used by the request.
    pub address: String,
    /// Store resolve epoch captured by the request.
    pub store_epoch: u64,
}

/// Cache-issued dispatch observation binding an attempt to its peer vector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionAttemptObservation {
    attempt: RegionAttempt,
    selectable_peer_count: usize,
}

impl RegionAttemptObservation {
    pub(crate) const fn new(attempt: RegionAttempt, selectable_peer_count: usize) -> Self {
        Self {
            attempt,
            selectable_peer_count,
        }
    }

    /// Exact route generation captured for dispatch.
    #[must_use]
    pub const fn attempt(&self) -> &RegionAttempt {
        &self.attempt
    }

    pub(crate) const fn selectable_peer_count(&self) -> usize {
        self.selectable_peer_count
    }
}

/// Owned leader route safe to carry across cache borrows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedLeaderRoute {
    /// Exact versioned region.
    pub region: RegionVerId,
    /// Selected leader peer.
    pub peer_id: u64,
    /// Selected leader store.
    pub store_id: u64,
    /// Resolved TiKV address.
    pub address: String,
    /// Store resolve epoch.
    pub store_epoch: u64,
}

/// Source-shaped action returned to the DistSQL retry owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegionErrorDisposition {
    /// Retry immediately or after the reserved delay on this exact route.
    RetryRoute {
        /// Updated or unchanged leader route.
        route: OwnedLeaderRoute,
        /// Sleep reserved from the shared response budget.
        delay: Duration,
    },
    /// Continue the same request-scoped selector after an exact typed state
    /// transition. The cache has already completed any topology mutation.
    RetrySelector {
        /// Exact completed attempt to which the transition applies.
        attempt: RegionAttempt,
        /// Sole request-local recovery transition.
        transition: SelectorRecovery,
        /// Sleep reserved from the shared response budget.
        delay: Duration,
    },
    /// Re-locate the failed task's remaining ranges after the reserved delay.
    RebuildRanges {
        /// Sleep reserved from the shared response budget.
        delay: Duration,
        /// Cache work complete now or intentionally deferred until after sleep.
        action: RegionRebuildAction,
    },
    /// Return a caller-owned or deliberately unsupported error fail-closed.
    ///
    /// Stale topology never uses this path: it invalidates/replaces cache and
    /// rebuilds ranges through the TiDB outer `BoRegionMiss` loop.
    ReturnRegionError,
    /// Return a typed terminal source condition.
    Terminal(RegionTerminalError),
}

/// Cache transition associated with a DistSQL range rebuild.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegionRebuildAction {
    /// The cache was invalidated or replaced before returning the disposition.
    CacheReady,
}

/// Region errors which pinned client-go does not retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegionTerminalError {
    /// Flashback is active for the region.
    FlashbackInProgress {
        /// Region identifier.
        region_id: u64,
        /// Flashback start timestamp.
        flashback_start_ts: u64,
    },
    /// A second-phase flashback request reached an unprepared region.
    FlashbackNotPrepared {
        /// Region identifier.
        region_id: u64,
    },
    /// The request cannot fit in one raft entry.
    RaftEntryTooLarge {
        /// Region identifier.
        region_id: u64,
        /// Encoded raft entry size.
        entry_size: u64,
    },
    /// TiKV rejected a decreasing max timestamp update.
    InvalidMaxTimestampUpdate {
        /// Original region-error message.
        message: String,
    },
    /// The failed region's shared effective recovery budget was exhausted.
    BackoffExhausted {
        /// Category that attempted the next reservation.
        kind: RegionBackoffKind,
        /// Configured effective maximum.
        max_sleep: Duration,
    },
}

/// Fail-closed cache recovery failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegionRecoveryError {
    /// A delayed response no longer describes the exact cached route.
    StaleObservation(RegionAttempt),
    /// Returned current-region metadata omitted its required epoch.
    MissingRegionEpoch {
        /// Region identifier from the malformed payload.
        region_id: u64,
    },
    /// Returned current-region metadata has no peers.
    MissingRegionPeers {
        /// Region identifier from the malformed payload.
        region_id: u64,
    },
    /// A loader returned a different identity than the metadata it hydrated.
    HydratedRegionMismatch {
        /// Identity supplied to the loader.
        expected: RegionVerId,
        /// Identity returned by the loader.
        actual: RegionVerId,
    },
    /// Store hydration failed before the atomic cache replacement.
    Loader(RegionLoadError),
    /// A complete hydrated replacement violated cache topology invariants.
    Route(RegionRouteError),
}

impl std::fmt::Display for RegionRecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleObservation(attempt) => write!(
                formatter,
                "region response no longer matches observed route {:?}/peer {}/store {}@{}#{}",
                attempt.region,
                attempt.peer_id,
                attempt.store_id,
                attempt.address,
                attempt.store_epoch
            ),
            Self::MissingRegionEpoch { region_id } => {
                write!(formatter, "current region {region_id} omitted its epoch")
            }
            Self::MissingRegionPeers { region_id } => {
                write!(formatter, "current region {region_id} omitted its peers")
            }
            Self::HydratedRegionMismatch { expected, actual } => write!(
                formatter,
                "region hydration returned {actual:?}, expected {expected:?}"
            ),
            Self::Loader(error) => write!(formatter, "region hydration failed: {error}"),
            Self::Route(error) => write!(formatter, "replacement region is invalid: {error}"),
        }
    }
}

impl std::error::Error for RegionRecoveryError {}

impl From<RegionLoadError> for RegionRecoveryError {
    fn from(error: RegionLoadError) -> Self {
        Self::Loader(error)
    }
}

impl From<RegionRouteError> for RegionRecoveryError {
    fn from(error: RegionRouteError) -> Self {
        Self::Route(error)
    }
}

impl<L: RegionRecoveryLoader> RegionCache<L> {
    /// Applies one pinned region error to the sole topology authority.
    ///
    /// Handler ordering intentionally follows client-go's
    /// `RegionRequestSender.onRegionError`, including its leading
    /// `UndeterminedResult` check, rather than sorting branches by protobuf
    /// field number.
    pub fn on_region_error(
        &mut self,
        error: &errorpb::Error,
        attempt: RegionAttempt,
        backoff: &mut RegionBackoffBudget,
    ) -> Result<RegionErrorDisposition, RegionRecoveryError> {
        self.validate_attempt(&attempt)?;

        if error.undetermined_result.is_some() {
            return Ok(RegionErrorDisposition::ReturnRegionError);
        }
        if let Some(not_leader) = &error.not_leader {
            return self.on_not_leader(not_leader, attempt, backoff);
        }
        if error.disk_full.is_some() {
            let route = self.owned_leader_route(attempt.region)?;
            return Ok(match backoff.next_delay(RegionBackoffKind::TikvDiskFull) {
                Ok(delay) => RegionErrorDisposition::RetryRoute { route, delay },
                // Pinned client-go discards the exhausted inner backoff error
                // and returns the region error to the cop owner. That owner
                // then attempts its ordinary outer RegionMiss backoff.
                Err(_) => RegionErrorDisposition::RebuildRanges {
                    delay: Duration::ZERO,
                    action: RegionRebuildAction::CacheReady,
                },
            });
        }
        if error.recovery_in_progress.is_some() {
            self.invalidate(attempt.region);
            return Ok(rebuild_with_backoff(
                backoff,
                RegionBackoffKind::RegionRecoveryInProgress,
            ));
        }
        if error.is_witness.is_some() {
            self.invalidate(attempt.region);
            return Ok(rebuild_with_backoff(backoff, RegionBackoffKind::IsWitness));
        }
        if let Some(flashback) = &error.flashback_in_progress {
            return Ok(RegionErrorDisposition::Terminal(
                RegionTerminalError::FlashbackInProgress {
                    region_id: flashback.region_id,
                    flashback_start_ts: flashback.flashback_start_ts,
                },
            ));
        }
        if let Some(flashback) = &error.flashback_not_prepared {
            return Ok(RegionErrorDisposition::Terminal(
                RegionTerminalError::FlashbackNotPrepared {
                    region_id: flashback.region_id,
                },
            ));
        }
        if error.region_not_found.is_some() || error.key_not_in_region.is_some() {
            self.invalidate(attempt.region);
            return Ok(RegionErrorDisposition::RebuildRanges {
                delay: Duration::ZERO,
                action: RegionRebuildAction::CacheReady,
            });
        }
        if let Some(epoch) = &error.epoch_not_match {
            return self.on_epoch_not_match(epoch, attempt, backoff);
        }
        if let Some(mismatch) = &error.bucket_version_not_match {
            self.publish_bucket_version_not_match(attempt.region, mismatch);
            return Ok(RegionErrorDisposition::ReturnRegionError);
        }
        if error.server_is_busy.is_some() {
            return self.retry_same_route(attempt, backoff, RegionBackoffKind::TikvServerBusy);
        }
        if error.stale_command.is_some() {
            return self.retry_same_route(attempt, backoff, RegionBackoffKind::StaleCommand);
        }
        if error.store_not_match.is_some() {
            self.invalidate(attempt.region);
            return Ok(RegionErrorDisposition::RebuildRanges {
                delay: Duration::ZERO,
                action: RegionRebuildAction::CacheReady,
            });
        }
        if let Some(too_large) = &error.raft_entry_too_large {
            return Ok(RegionErrorDisposition::Terminal(
                RegionTerminalError::RaftEntryTooLarge {
                    region_id: too_large.region_id,
                    entry_size: too_large.entry_size,
                },
            ));
        }
        if error.max_timestamp_not_synced.is_some() {
            return self.retry_same_route(
                attempt,
                backoff,
                RegionBackoffKind::MaxTimestampNotSynced,
            );
        }
        if error.region_not_initialized.is_some() {
            return self.retry_same_route(
                attempt,
                backoff,
                RegionBackoffKind::RegionNotInitialized,
            );
        }
        if error.read_index_not_ready.is_some() || error.proposal_in_merging_mode.is_some() {
            return self.retry_same_route(attempt, backoff, RegionBackoffKind::RegionScheduling);
        }
        if error.data_is_not_ready.is_some() {
            return Ok(RegionErrorDisposition::RetrySelector {
                attempt,
                transition: SelectorRecovery::DataIsNotReady,
                delay: Duration::ZERO,
            });
        }
        if error.mismatch_peer_id.is_some() {
            self.invalidate(attempt.region);
            return Ok(RegionErrorDisposition::RebuildRanges {
                delay: Duration::ZERO,
                action: RegionRebuildAction::CacheReady,
            });
        }
        if error.message.contains("invalid max_ts update") {
            return Ok(RegionErrorDisposition::Terminal(
                RegionTerminalError::InvalidMaxTimestampUpdate {
                    message: error.message.clone(),
                },
            ));
        }

        // The pinned legacy fallback drops a nonzero region. Unknown leader
        // errors then cross TiDB's outer RegionMiss rebuild.
        self.invalidate(attempt.region);
        Ok(RegionErrorDisposition::RebuildRanges {
            delay: Duration::ZERO,
            action: RegionRebuildAction::CacheReady,
        })
    }

    fn publish_bucket_version_not_match(
        &mut self,
        region: RegionVerId,
        mismatch: &errorpb::BucketVersionNotMatch,
    ) {
        let Some(location) = self
            .regions
            .iter_mut()
            .find(|location| location.region == region)
        else {
            return;
        };
        if location
            .buckets
            .as_ref()
            .is_some_and(|buckets| buckets.version >= mismatch.version)
        {
            return;
        }
        location.buckets = Some(BucketMetadata {
            region_id: location.region.id,
            version: mismatch.version,
            keys: mismatch.keys.clone(),
            stats: None,
            period_in_ms: 0,
        });
    }

    fn on_not_leader(
        &mut self,
        not_leader: &errorpb::NotLeader,
        attempt: RegionAttempt,
        backoff: &mut RegionBackoffBudget,
    ) -> Result<RegionErrorDisposition, RegionRecoveryError> {
        let Some(leader) = &not_leader.leader else {
            let delay = match reserve_delay(backoff, RegionBackoffKind::RegionScheduling) {
                Ok(delay) => delay,
                Err(terminal) => return Ok(RegionErrorDisposition::Terminal(terminal)),
            };
            return Ok(RegionErrorDisposition::RetrySelector {
                attempt,
                transition: SelectorRecovery::RejectPeer,
                delay,
            });
        };

        if self.update_leader(attempt.region, leader.id, leader.store_id) {
            Ok(RegionErrorDisposition::RetrySelector {
                attempt,
                transition: SelectorRecovery::FollowKnownLeader {
                    leader_peer_id: leader.id,
                },
                delay: Duration::ZERO,
            })
        } else {
            self.invalidate(attempt.region);
            Ok(RegionErrorDisposition::RebuildRanges {
                delay: Duration::ZERO,
                action: RegionRebuildAction::CacheReady,
            })
        }
    }

    fn on_epoch_not_match(
        &mut self,
        mismatch: &errorpb::EpochNotMatch,
        attempt: RegionAttempt,
        backoff: &mut RegionBackoffBudget,
    ) -> Result<RegionErrorDisposition, RegionRecoveryError> {
        if mismatch.current_regions.is_empty() {
            self.invalidate(attempt.region);
            return Ok(RegionErrorDisposition::RebuildRanges {
                delay: Duration::ZERO,
                action: RegionRebuildAction::CacheReady,
            });
        }

        if mismatch.current_regions.iter().any(|current| {
            current.id == attempt.region.id
                && current.region_epoch.as_ref().is_some_and(|epoch| {
                    epoch.conf_ver < attempt.region.epoch.conf_ver
                        || epoch.version < attempt.region.epoch.version
                })
        }) {
            return self.retry_same_route(attempt, backoff, RegionBackoffKind::RegionMiss);
        }

        let mut replacements = Vec::with_capacity(mismatch.current_regions.len());
        for current in &mismatch.current_regions {
            let metadata = region_metadata(current)?;
            let hydrated = self
                .with_loader(|loader| loader.hydrate_region(&metadata, attempt.store_id))
                .map_err(RegionRecoveryError::Loader)?;
            if hydrated.region != metadata.region {
                return Err(RegionRecoveryError::HydratedRegionMismatch {
                    expected: metadata.region,
                    actual: hydrated.region,
                });
            }
            replacements.push(hydrated);
        }
        self.replace_regions_atomically(attempt.region, replacements)?;
        Ok(RegionErrorDisposition::RebuildRanges {
            delay: Duration::ZERO,
            action: RegionRebuildAction::CacheReady,
        })
    }

    fn retry_same_route(
        &mut self,
        attempt: RegionAttempt,
        backoff: &mut RegionBackoffBudget,
        kind: RegionBackoffKind,
    ) -> Result<RegionErrorDisposition, RegionRecoveryError> {
        let route = self.owned_leader_route(attempt.region)?;
        let delay = match backoff.next_delay(kind) {
            Ok(delay) => delay,
            Err(exhausted) => {
                return Ok(RegionErrorDisposition::Terminal(
                    RegionTerminalError::BackoffExhausted {
                        kind: exhausted.kind,
                        max_sleep: exhausted.max_sleep,
                    },
                ));
            }
        };
        Ok(RegionErrorDisposition::RetryRoute { route, delay })
    }
}

fn rebuild_with_backoff(
    backoff: &mut RegionBackoffBudget,
    kind: RegionBackoffKind,
) -> RegionErrorDisposition {
    match backoff.next_delay(kind) {
        Ok(delay) => RegionErrorDisposition::RebuildRanges {
            delay,
            action: RegionRebuildAction::CacheReady,
        },
        Err(exhausted) => RegionErrorDisposition::Terminal(RegionTerminalError::BackoffExhausted {
            kind: exhausted.kind,
            max_sleep: exhausted.max_sleep,
        }),
    }
}

fn reserve_delay(
    backoff: &mut RegionBackoffBudget,
    kind: RegionBackoffKind,
) -> Result<Duration, RegionTerminalError> {
    backoff
        .next_delay(kind)
        .map_err(|exhausted| RegionTerminalError::BackoffExhausted {
            kind: exhausted.kind,
            max_sleep: exhausted.max_sleep,
        })
}

fn region_metadata(region: &metapb::Region) -> Result<RegionMetadata, RegionRecoveryError> {
    let epoch = region
        .region_epoch
        .as_ref()
        .ok_or(RegionRecoveryError::MissingRegionEpoch {
            region_id: region.id,
        })?;
    if region.peers.is_empty() {
        return Err(RegionRecoveryError::MissingRegionPeers {
            region_id: region.id,
        });
    }
    Ok(RegionMetadata {
        region: RegionVerId::new(region.id, epoch.conf_ver, epoch.version),
        encoded_start_key: region.start_key.clone(),
        encoded_end_key: region.end_key.clone(),
        peers: region
            .peers
            .iter()
            .map(|peer| RegionMetadataPeer {
                id: peer.id,
                store_id: peer.store_id,
                role: map_peer_role(peer.role),
                is_witness: peer.is_witness,
            })
            .collect(),
    })
}

const fn map_peer_role(role: i32) -> PeerRole {
    match role {
        0 => PeerRole::Voter,
        1 => PeerRole::Learner,
        2 => PeerRole::IncomingVoter,
        3 => PeerRole::DemotingVoter,
        role => PeerRole::Unknown(role),
    }
}
