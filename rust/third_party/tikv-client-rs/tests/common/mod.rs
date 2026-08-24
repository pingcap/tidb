#![allow(dead_code)]

mod ctl;

use log::info;
use log::warn;
use rand::Rng;
use std::collections::HashSet;
use std::convert::TryInto;
use std::env;
use std::time::Duration;
use tikv_client::transaction::ResolveLocksOptions;
use tikv_client::Config;
use tikv_client::Key;
use tikv_client::RawClient;
use tikv_client::Result;
use tikv_client::Transaction;
use tikv_client::TransactionClient;
use tikv_client::{Snapshot, TransactionOptions};
use tokio::time::sleep;

const ENV_PD_ADDRS: &str = "PD_ADDRS";
const ENV_ENABLE_MULIT_REGION: &str = "MULTI_REGION";
const REGION_SPLIT_TIME_LIMIT: Duration = Duration::from_secs(15);

// Delete all entries in TiKV to leave a clean space for following tests.
pub async fn clear_tikv() {
    // DEFAULT_REGION_BACKOFF is not long enough for CI environment. So set a longer backoff.
    let backoff = tikv_client::Backoff::no_jitter_backoff(100, 30000, 20);
    let raw_client =
        RawClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await
            .unwrap();
    raw_client
        .with_backoff(backoff)
        .delete_range(..)
        .await
        .unwrap();

    let txn_client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await
            .unwrap();
    txn_client.unsafe_destroy_range(..).await.unwrap();
    // Cleanup any late-arriving locks from prior test case that can appear after destroy.
    cleanup_all_locks(&txn_client).await;
}

// To test with multiple regions, prewrite some data. Tests that hope to test
// with multiple regions should use keys in the corresponding ranges.
pub async fn init() -> Result<()> {
    let _ = env_logger::try_init();

    if env::var(ENV_ENABLE_MULIT_REGION).is_ok() {
        // 1000 keys: 0..1000
        let keys_1 = std::iter::successors(Some(0u32), |x| Some(x + 1))
            .take(1000)
            .map(|x| x.to_be_bytes().to_vec());
        // 1024 keys: 0..u32::MAX
        let count = 1024;
        let step = u32::MAX / count;
        let keys_2 = std::iter::successors(Some(0u32), |x| Some(x + step))
            .take(count as usize - 1)
            .map(|x| x.to_be_bytes().to_vec());

        // about 43 regions with above keys.
        ensure_region_split(keys_1.chain(keys_2), 40).await?;
    }

    clear_tikv().await;
    let region_cnt = ctl::get_region_count().await?;
    // print log for debug convenience
    println!("init finish with {region_cnt} regions");
    Ok(())
}

/// Initialize the test environment synchronously.
///
/// This function creates its own tokio runtime and must be called from
/// synchronous code only (e.g., at the start of a `#[test]` function).
///
/// **Do not call this from inside an async function or tokio runtime** -
/// it will panic. If you're already in an async context, use `init()` directly.
pub fn init_sync() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(init())
}

async fn ensure_region_split(
    keys: impl IntoIterator<Item = impl Into<Key>>,
    region_count: u32,
) -> Result<()> {
    if ctl::get_region_count().await? as u32 >= region_count {
        return Ok(());
    }

    // 1. write plenty transactional keys
    // 2. wait until regions split

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let mut txn = client.begin_optimistic().await?;
    for key in keys.into_iter() {
        txn.put(key.into(), vec![0, 0, 0, 0]).await?;
    }
    txn.commit().await?;
    let mut txn = client.begin_optimistic().await?;
    let _ = txn.scan(.., 2048).await?;
    txn.commit().await?;

    info!("splitting regions...");
    let start_time = std::time::Instant::now();
    let mut observed_regions;
    loop {
        observed_regions = ctl::get_region_count().await? as u32;
        if observed_regions >= region_count {
            break;
        }
        if start_time.elapsed() > REGION_SPLIT_TIME_LIMIT {
            warn!("Stop splitting regions: time limit exceeded");
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    // Setup cost is charged to whichever test runs first, so record it: when that
    // test is the one CI kills, this says whether the time went here or in the body.
    // Reuses the count the loop already fetched — diagnostics must not add a
    // fallible request that could itself fail or stall the setup.
    info!(
        "region split finished in {:?} ({} regions)",
        start_time.elapsed(),
        observed_regions
    );

    Ok(())
}

async fn cleanup_all_locks(client: &TransactionClient) {
    let options = ResolveLocksOptions {
        async_commit_only: false,
        ..Default::default()
    };
    for _ in 0..30 {
        let safepoint = client.current_timestamp().await.unwrap();
        client.cleanup_locks(.., &safepoint, options).await.unwrap();
        let ts = client.current_timestamp().await.unwrap();
        let locks = client.scan_locks(&ts, .., 1024).await.unwrap();
        if locks.is_empty() {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    let ts = client.current_timestamp().await.unwrap();
    let locks = client.scan_locks(&ts, .., 1024).await.unwrap();
    assert!(
        locks.is_empty(),
        "clear_tikv left {} locks behind",
        locks.len()
    );
}

pub fn pd_addrs() -> Vec<String> {
    env::var(ENV_PD_ADDRS)
        .unwrap_or_else(|_| {
            info!(
                "Environment variable {} is not found. Using {:?} as default.",
                ENV_PD_ADDRS, "127.0.0.1:2379"
            );
            "127.0.0.1:2379".to_owned()
        })
        .split(',')
        .map(From::from)
        .collect()
}

// helper function
pub async fn get_u32(client: &RawClient, key: Vec<u8>) -> Result<u32> {
    let x = client.get(key).await?.unwrap();
    let boxed_slice = x.into_boxed_slice();
    let array: Box<[u8; 4]> = boxed_slice
        .try_into()
        .expect("Value should not exceed u32 (4 * u8)");
    Ok(u32::from_be_bytes(*array))
}

// helper function
pub async fn get_txn_u32(txn: &mut Transaction, key: Vec<u8>) -> Result<u32> {
    let x = txn.get(key).await?.unwrap();
    let boxed_slice = x.into_boxed_slice();
    let array: Box<[u8; 4]> = boxed_slice
        .try_into()
        .expect("Value should not exceed u32 (4 * u8)");
    Ok(u32::from_be_bytes(*array))
}

// helper function
pub fn gen_u32_keys(num: u32, rng: &mut impl Rng) -> HashSet<Vec<u8>> {
    let mut set = HashSet::new();
    for _ in 0..num {
        set.insert(rng.gen::<u32>().to_be_bytes().to_vec());
    }
    set
}

pub async fn snapshot(client: &TransactionClient, is_pessimistic: bool) -> Result<Snapshot> {
    let options = if is_pessimistic {
        TransactionOptions::new_pessimistic()
    } else {
        TransactionOptions::new_optimistic()
    };
    Ok(client.snapshot(client.current_timestamp().await?, options))
}

/// Copied from https://github.com/tikv/tikv/blob/d86a449d7f5b656cef28576f166e73291f501d77/components/tikv_util/src/macros.rs#L55
/// Simulates Go's defer.
///
/// Please note that, different from go, this defer is bound to scope.
/// When exiting the scope, its deferred calls are executed in last-in-first-out
/// order.
#[doc(hidden)]
#[macro_export]
macro_rules! defer {
    ($t:expr) => {
        let __ctx = $crate::DeferContext::new(|| $t);
    };
}

/// Invokes the wrapped closure when dropped.
pub struct DeferContext<T: FnOnce()> {
    t: Option<T>,
}

impl<T: FnOnce()> DeferContext<T> {
    pub fn new(t: T) -> DeferContext<T> {
        DeferContext { t: Some(t) }
    }
}

impl<T: FnOnce()> Drop for DeferContext<T> {
    fn drop(&mut self) {
        self.t.take().unwrap()()
    }
}
