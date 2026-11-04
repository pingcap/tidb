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
pub fn client_backoff_terminal(error: &tikv_client::Error) -> Option<tikv_client::retry::RetryTerminal> {
    use tikv_client::error::StaticError;
    use tikv_client::retry::RetryTerminal;
    match error {
        tikv_client::Error::PdServerTimeout(_) => Some(RetryTerminal::PdServerTimeout),
        tikv_client::Error::Static(
            error @ (StaticError::TiKvServerTimeout
            | StaticError::RegionUnavailable
            | StaticError::TiKvStaleCommand
            | StaticError::TiKvMaxTimestampNotSynced
            | StaticError::ResolveLockTimeout
            | StaticError::TiKvServerBusy
            | StaticError::TiKvDiskFull
            | StaticError::RegionRecoveryInProgress
            | StaticError::RegionNotInitialized
            | StaticError::IsWitness),
        ) => Some(RetryTerminal::Static(*error)),
        _ => None,
    }
}
