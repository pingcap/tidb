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

//! `pkg/util/channel`: channel cleanup without a second channel abstraction.

/// Discards every value received from `channel` and returns after the channel
/// disconnects.
///
/// Native blocking receivers implement [`IntoIterator`], so iteration carries
/// the source contract directly: buffered and later values are drained, and an
/// open sender keeps this call blocked.
pub fn clear<T>(channel: impl IntoIterator<Item = T>) {
    for _ in channel {}
}
