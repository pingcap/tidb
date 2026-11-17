// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/tikv_mode.go` (`TiKVModeSwitcher`), with
//! `pkg/lightning/tikv`'s `ForAllStores` / `SwitchMode` and `local.go`'s
//! `switchModeBySplitKeys`.

use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_proto::import_sstpb as sst;

use crate::region_job::ImportClient;

/// Go `config.DefaultSwitchTiKVModeInterval`.
pub const DEFAULT_SWITCH_TIKV_MODE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Go `metapb.StoreState_Offline`.
const STORE_STATE_OFFLINE: i64 = 1;

/// Go `TiKVModeSwitcher`: switches TiKV nodes between Import and Normal mode.
pub trait TikvModeSwitcher: Send + Sync {
    /// Go `ToImportMode`.
    fn to_import_mode(&self, ctx: &Context, ranges: &[sst::Range]);
    /// Go `ToNormalMode`.
    fn to_normal_mode(&self, ctx: &Context, ranges: &[sst::Range]);
}

/// Go `switcher`, over the import client's PD and ImportSST connections.
pub struct Switcher {
    client: Arc<dyn ImportClient>,
}

/// Go `NewTiKVModeSwitcher`.
#[must_use]
pub fn new_tikv_mode_switcher(client: Arc<dyn ImportClient>) -> Switcher {
    Switcher { client }
}

impl Switcher {
    /// Go `switchTiKVMode`. A store that misses the switch is fine, since it
    /// is run periodically, so failures are only logged.
    fn switch_tikv_mode(&self, ctx: &Context, mode: sst::SwitchMode, ranges: &[sst::Range]) {
        let mode_field = || tidb_log::Field::new("mode", tidb_log::Value::Str(mode.as_str_name().to_owned()));
        tidb_log::info("switch tikv mode", &[mode_field()]);
        let _ = for_all_stores(ctx, self.client.as_ref(), STORE_STATE_OFFLINE, &|address| {
            let request = sst::SwitchModeRequest { mode: mode as i32, ranges: ranges.to_vec() };
            let result = self.client.switch_mode(address, request);
            let tikv = tidb_log::Field::new("tikv", tidb_log::Value::Str(address.to_owned()));
            match &result {
                Ok(()) => tidb_log::info("switch mode completed", &[mode_field(), tikv]),
                Err(e) => tidb_log::warn(
                    "switch mode failed",
                    &[mode_field(), tikv, tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))],
                ),
            }
            result
        });
    }
}

impl TikvModeSwitcher for Switcher {
    fn to_import_mode(&self, ctx: &Context, ranges: &[sst::Range]) {
        self.switch_tikv_mode(ctx, sst::SwitchMode::Import, ranges);
    }

    fn to_normal_mode(&self, ctx: &Context, ranges: &[sst::Range]) {
        self.switch_tikv_mode(ctx, sst::SwitchMode::Normal, ranges);
    }
}

/// Go `tikv.ForAllStores`: runs `action` on every store whose state is at
/// most `max_state`, concurrently, and returns the first error. Go's
/// errgroup also cancels the other actions' context on the first error;
/// here they run to completion.
///
/// # Errors
/// The stores could not be listed, or an action failed.
pub fn for_all_stores(
    ctx: &Context,
    client: &dyn ImportClient,
    max_state: i64,
    action: &(dyn Fn(&str) -> crate::Result<()> + Sync),
) -> crate::Result<()> {
    if let Some(e) = ctx.err() {
        return Err(crate::Error::Other(e.to_string()));
    }
    let stores = client.get_stores()?;
    std::thread::scope(|scope| {
        let handles: Vec<_> = stores
            .iter()
            .filter(|(_, state)| *state <= max_state)
            .map(|(address, _)| scope.spawn(move || action(address)))
            .collect();
        let mut first = Ok(());
        for handle in handles {
            let result = handle.join().unwrap_or_else(|_| Err(crate::Error::Other("switch mode panicked".into())));
            if first.is_ok() {
                first = result;
            }
        }
        first
    })
}

/// Go `Backend.switchModeBySplitKeys`'s goroutine: switches `range` to
/// import mode now and every `interval` until `ctx` is done, then back to
/// normal mode with a fresh 5-second context.
pub fn switch_mode_periodically(
    ctx: &Context,
    client: Arc<dyn ImportClient>,
    range: sst::Range,
    interval: Duration,
) -> std::thread::JoinHandle<()> {
    let ctx = ctx.clone();
    std::thread::spawn(move || {
        let switcher = new_tikv_mode_switcher(client);
        let ranges = [range];
        switcher.to_import_mode(&ctx, &ranges);
        while !ctx.sleep(interval) {
            switcher.to_import_mode(&ctx, &ranges);
        }
        let (recover_ctx, _cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(5));
        switcher.to_normal_mode(&recover_ctx, &ranges);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Stores {
        switched: Mutex<Vec<(String, i32, usize)>>,
    }

    impl ImportClient for Stores {
        fn get_region(&self, _: &[u8]) -> crate::Result<crate::region_job::RegionInfo> {
            unreachable!()
        }
        fn invalidate_region(&self, _: &crate::region_job::RegionInfo) {}
        fn write(&self, _: u64, _: Vec<sst::WriteRequest>) -> crate::Result<sst::WriteResponse> {
            unreachable!()
        }
        fn multi_ingest(&self, _: u64, _: sst::MultiIngestRequest) -> crate::Result<sst::IngestResponse> {
            unreachable!()
        }
        fn ingest(&self, _: u64, _: sst::IngestRequest) -> crate::Result<sst::IngestResponse> {
            unreachable!()
        }
        fn duplicate_detect(
            &self,
            _: u64,
            _: sst::DuplicateDetectRequest,
            _: &mut (dyn FnMut(sst::DuplicateDetectResponse) -> crate::Result<()> + Send),
        ) -> crate::Result<()> {
            unreachable!()
        }
        fn api_version(&self) -> i32 {
            0
        }
        fn encode_key(&self, key: &[u8]) -> Vec<u8> {
            key.to_vec()
        }
        fn get_stores(&self) -> crate::Result<Vec<(String, i64)>> {
            Ok(vec![("up:20160".into(), 0), ("offline:20160".into(), 1), ("tombstone:20160".into(), 2)])
        }
        fn switch_mode(&self, address: &str, request: sst::SwitchModeRequest) -> crate::Result<()> {
            self.switched.lock().unwrap().push((address.to_owned(), request.mode, request.ranges.len()));
            if address.starts_with("offline") {
                return Err(crate::Error::Other("unreachable store".into()));
            }
            Ok(())
        }
    }

    impl crate::localhelper::SplitClient for Stores {
        fn split_keys_and_scatter(&self, _: &Context, _: &[Vec<u8>]) -> crate::Result<Vec<u64>> {
            unreachable!()
        }
        fn wait_regions_scattered(&self, _: &Context, _: &[u64]) -> crate::Result<usize> {
            unreachable!()
        }
    }

    /// Go `ForAllStores(ctx, cli, metapb.StoreState_Offline, ...)`: Up and
    /// Offline stores are switched, a Tombstone one is not, and a failed
    /// store does not stop the others.
    #[test]
    fn switch_reaches_up_and_offline_stores() {
        let client = Arc::new(Stores::default());
        let switcher = new_tikv_mode_switcher(client.clone());
        switcher.to_import_mode(&Context::background(), &[]);
        switcher.to_normal_mode(&Context::background(), &[sst::Range { start: b"a".to_vec(), end: b"b".to_vec() }]);
        let mut got = client.switched.lock().unwrap().clone();
        got.sort();
        assert_eq!(
            got,
            [
                ("offline:20160".to_owned(), 0, 1),
                ("offline:20160".to_owned(), 1, 0),
                ("up:20160".to_owned(), 0, 1),
                ("up:20160".to_owned(), 1, 0),
            ]
        );
    }
}
