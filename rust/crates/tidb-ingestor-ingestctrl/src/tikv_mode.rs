// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/tikv_mode.go`.
use crate::Result;
use std::sync::Arc;
use tidb_gocontext::Context;
use tidb_proto::{import_sstpb as sst, metapb};
/// Go `pdhttp.Client.GetStores` and `tikv.SwitchMode` dependencies.
pub trait ModeClient: Send + Sync {
    /// Go `pdhttp.Client.GetStores` metadata.
    fn stores(&self, ctx: &Context) -> Result<Vec<metapb::Store>>;
    /// Go `tikv.SwitchMode`.
    fn switch_mode(
        &self,
        ctx: &Context,
        store_id: u64,
        mode: sst::SwitchMode,
        ranges: &[sst::Range],
    ) -> Result<()>;
}
/// Go `TiKVModeSwitcher` / `switcher`.
pub struct TiKVModeSwitcher {
    client: Arc<dyn ModeClient>,
}
impl TiKVModeSwitcher {
    /// Go `NewTiKVModeSwitcher`.
    pub fn new(client: Arc<dyn ModeClient>) -> Self {
        Self { client }
    }
    /// Go `switcher.ToImportMode`.
    pub fn to_import_mode(&self, ctx: &Context, ranges: &[sst::Range]) {
        self.switch_tikv_mode(ctx, sst::SwitchMode::Import, ranges);
    }
    /// Go `switcher.ToNormalMode`.
    pub fn to_normal_mode(&self, ctx: &Context, ranges: &[sst::Range]) {
        self.switch_tikv_mode(ctx, sst::SwitchMode::Normal, ranges);
    }
    /// Go `switcher.switchTiKVMode`: best effort, including offline stores and
    /// excluding tombstones; errors never abort the ingest job.
    fn switch_tikv_mode(&self, ctx: &Context, mode: sst::SwitchMode, ranges: &[sst::Range]) {
        let Ok(stores) = self.client.stores(ctx) else {
            return;
        };
        std::thread::scope(|scope| {
            for store in stores {
                if store.state <= metapb::StoreState::Offline as i32 {
                    scope.spawn(move || {
                        let _ = self.client.switch_mode(ctx, store.id, mode, ranges);
                    });
                }
            }
        });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Client {
        called: Mutex<Vec<u64>>,
    }
    impl ModeClient for Client {
        /// Go `pdhttp.Client.GetStores`.
        fn stores(&self, _: &Context) -> Result<Vec<metapb::Store>> {
            Ok([
                (1, metapb::StoreState::Up),
                (5, metapb::StoreState::Offline),
                (4, metapb::StoreState::Tombstone),
            ]
            .into_iter()
            .map(|(id, state)| metapb::Store {
                id,
                state: state as i32,
                ..Default::default()
            })
            .collect())
        }
        /// Go `tikv.SwitchMode`.
        fn switch_mode(
            &self,
            _: &Context,
            id: u64,
            _: sst::SwitchMode,
            _: &[sst::Range],
        ) -> Result<()> {
            self.called.lock().unwrap().push(id);
            Ok(())
        }
    }
    /// Go `lightning/tikv.TestForAllStores`, the filter used by switchTiKVMode.
    #[test]
    fn test_for_all_stores() {
        let client = Arc::new(Client {
            called: Mutex::new(Vec::new()),
        });
        let switcher = TiKVModeSwitcher::new(client.clone());
        for mode in [sst::SwitchMode::Import, sst::SwitchMode::Normal] {
            switcher.switch_tikv_mode(&Context::background(), mode, &[]);
            let mut calls = client.called.lock().unwrap();
            calls.sort();
            assert_eq!(*calls, vec![1, 5]);
            calls.clear();
        }
    }
}
