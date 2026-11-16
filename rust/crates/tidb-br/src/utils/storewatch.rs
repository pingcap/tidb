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

//! Go `br/pkg/utils/storewatch`: notices stores joining, going offline
//! and restarting between two looks at PD's store list.

use std::collections::{HashMap, HashSet};

use tidb_gocontext::Context;
use tidb_proto::metapb;

use crate::conn::{get_all_tikv_stores_with_retry, StoreBehavior};
use crate::errors::{self, Result};
use crate::pdclient::PdClient;

/// A store event callback.
pub type StoreCallback<'a> = Option<Box<dyn FnMut(&metapb::Store) + 'a>>;

/// Go `Callback` / `DynCallback`.
#[derive(Default)]
pub struct Callback<'a> {
    /// Go `OnNewStoreRegistered`.
    pub on_new_store_registered: StoreCallback<'a>,
    /// Go `OnDisconnect`.
    pub on_disconnect: StoreCallback<'a>,
    /// Go `OnReboot`.
    pub on_reboot: StoreCallback<'a>,
}

/// Go `Watcher`.
pub struct Watcher<'a> {
    cli: &'a dyn PdClient,
    cb: Callback<'a>,
    last_stores: HashMap<u64, metapb::Store>,
}

impl<'a> Watcher<'a> {
    /// Go `New`.
    #[must_use]
    pub fn new(cli: &'a dyn PdClient, cb: Callback<'a>) -> Self {
        Self {
            cli,
            cb,
            last_stores: HashMap::new(),
        }
    }

    /// Go `Step`: compares PD's store list with the last one.
    ///
    /// # Errors
    /// PD's error.
    pub fn step(&mut self, ctx: &Context) -> Result<()> {
        let live = get_all_tikv_stores_with_retry(ctx, self.cli, StoreBehavior::SkipTiFlash)
            .map_err(|e| errors::annotate(e, "failed to update store list"))?;
        let mut recorded = HashSet::new();
        for store in live {
            recorded.insert(store.id);
            self.update_store(store);
        }
        self.last_stores.retain(|id, _| recorded.contains(id));
        Ok(())
    }

    /// Go `updateStore`.
    fn update_store(&mut self, new_store: metapb::Store) {
        let last = self.last_stores.insert(new_store.id, new_store.clone());
        let Some(last) = last else {
            if let Some(f) = &mut self.cb.on_new_store_registered {
                f(&new_store);
            }
            return;
        };
        if last.state == metapb::StoreState::Up as i32
            && new_store.state == metapb::StoreState::Offline as i32
        {
            if let Some(f) = &mut self.cb.on_disconnect {
                f(&new_store);
            }
        }
        if last.start_timestamp != new_store.start_timestamp {
            if let Some(f) = &mut self.cb.on_reboot {
                f(&new_store);
            }
        }
    }
}

#[cfg(test)]
mod tests;
