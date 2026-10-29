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

//! Go `ToTiDBErr`'s backoff arm (`pkg/store/driver/error/error.go`).

/// Go `ToTiDBErr`'s backoff arm: the backoff class whose sentinel client-go
/// returns when that class's budget ran out (`tikverr.ErrRegionUnavailable`
/// for `BoRegionMiss`, and so on), from client-rust's sentinel.
pub fn client_backoff_kind(error: &tikv_client::Error) -> Option<tidb_txnkv::retry::RegionBackoffKind> {
    use tidb_txnkv::retry::RegionBackoffKind as Kind;
    use tikv_client::error::StaticError;
    let terminal = match error {
        tikv_client::Error::Static(error) => *error,
        tikv_client::Error::PdServerTimeout(_) => return Some(Kind::PdRpc),
        _ => return None,
    };
    Some(match terminal {
        StaticError::TiKvServerTimeout => Kind::TikvRpc,
        StaticError::RegionUnavailable => Kind::RegionMiss,
        StaticError::TiKvStaleCommand => Kind::StaleCommand,
        StaticError::TiKvMaxTimestampNotSynced => Kind::MaxTimestampNotSynced,
        StaticError::ResolveLockTimeout => Kind::TxnLock,
        StaticError::TiKvServerBusy => Kind::TikvServerBusy,
        StaticError::TiKvDiskFull => Kind::TikvDiskFull,
        StaticError::RegionRecoveryInProgress => Kind::RegionRecoveryInProgress,
        StaticError::RegionNotInitialized => Kind::RegionNotInitialized,
        StaticError::IsWitness => Kind::IsWitness,
        _ => return None,
    })
}
