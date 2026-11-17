// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `br/pkg/utils/register.go`: a task registered in PD's etcd under a
//! lease, so BR and changefeeds can see a running import or restore.
//! `RegisterTaskOnce` leaves the refresh to the caller; `RegisterTask`
//! keeps the lease alive on a thread.

use std::sync::Arc;
use std::time::Duration;

/// Go `RegisterImportTaskPrefix`.
pub const REGISTER_IMPORT_TASK_PREFIX: &str = "/tidb/brie/import";

/// Go `clientv3.NoLease`.
const NO_LEASE: i64 = 0;
/// Go `defaultTaskRegisterTTL`.
pub const DEFAULT_TASK_REGISTER_TTL: Duration = Duration::from_secs(3 * 60);
/// Go `minTimeLeftThreshold` of `keepaliveLoop`.
const MIN_TIME_LEFT_THRESHOLD: Duration = Duration::from_secs(20);

/// Go `RegisterTaskType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisterTaskType {
    /// Go `RegisterRestore`.
    Restore,
    /// Go `RegisterLightning`.
    Lightning,
    /// Go `RegisterImportInto`.
    ImportInto,
}

impl std::fmt::Display for RegisterTaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Restore => "restore",
            Self::Lightning => "lightning",
            Self::ImportInto => "import-into",
        })
    }
}

/// The `clientv3` calls a task register makes.
pub trait RegisterEtcd: Send + Sync {
    /// `KV.Get(key)`: the key's lease when it exists.
    ///
    /// # Errors
    /// The read failed.
    fn get_key_lease(&self, key: &str) -> Result<Option<i64>, String>;
    /// `Lease.Grant`: the new lease's ID.
    ///
    /// # Errors
    /// The grant failed.
    fn lease_grant(&self, ttl_seconds: i64) -> Result<i64, String>;
    /// `KV.Put(key, value, clientv3.WithLease(lease))`.
    ///
    /// # Errors
    /// The write failed.
    fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String>;
    /// `Lease.KeepAliveOnce`.
    ///
    /// # Errors
    /// The keepalive failed.
    fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String>;
    /// `Lease.Revoke`.
    ///
    /// # Errors
    /// The revoke failed.
    fn lease_revoke(&self, lease: i64) -> Result<(), String>;
}

/// Go `taskRegister`.
pub struct TaskRegister {
    client: Arc<dyn RegisterEtcd>,
    second_ttl: i64,
    key: String,
    cur_lease_id: i64,
    keepalive: Option<(crossbeam_channel::Sender<()>, std::thread::JoinHandle<i64>)>,
}

impl TaskRegister {
    /// Go `NewTaskRegisterWithTTL`: the key is
    /// `{RegisterImportTaskPrefix}/{type}/{task_name}`.
    #[must_use]
    pub fn new_with_ttl(client: Arc<dyn RegisterEtcd>, ttl: Duration, tp: RegisterTaskType, task_name: &str) -> Self {
        Self {
            client,
            second_ttl: i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX),
            key: format!("{REGISTER_IMPORT_TASK_PREFIX}/{tp}/{task_name}"),
            cur_lease_id: NO_LEASE,
            keepalive: None,
        }
    }

    /// Go `NewTaskRegister`: the default 3-minute TTL.
    #[must_use]
    pub fn new(client: Arc<dyn RegisterEtcd>, tp: RegisterTaskType, task_name: &str) -> Self {
        Self::new_with_ttl(client, DEFAULT_TASK_REGISTER_TTL, tp, task_name)
    }

    /// Go `RegisterTask`: the key under a new lease, kept alive every third
    /// of the TTL until [`Self::close`]. A lease that cannot be kept is
    /// granted again (and the key put again) before it would expire.
    ///
    /// # Errors
    /// The first grant or put failed.
    pub fn register_task(&mut self) -> Result<(), String> {
        let lease = self
            .client
            .lease_grant(self.second_ttl)
            .map_err(|e| format!("failed grant a lease: {e}"))?;
        self.cur_lease_id = lease;
        self.client.put_with_lease(&self.key, b"", lease)?;
        let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(0);
        let (client, key, second_ttl) =
            (Arc::clone(&self.client), self.key.clone(), self.second_ttl);
        let ttl = Duration::from_secs(u64::try_from(second_ttl).unwrap_or(0));
        let threshold = (ttl / 4).max(MIN_TIME_LEFT_THRESHOLD);
        let interval = (ttl / 3).max(Duration::from_secs(1));
        let handle = std::thread::Builder::new()
            .name("br-task-register".into())
            .spawn(move || {
                keepalive_loop(
                    client.as_ref(),
                    &key,
                    second_ttl,
                    lease,
                    ttl,
                    threshold,
                    interval,
                    &stop_rx,
                )
            })
            .map_err(|e| e.to_string())?;
        self.keepalive = Some((stop_tx, handle));
        Ok(())
    }

    /// The registered key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Go `RegisterTaskOnce`: puts the key under a new lease when it is
    /// absent; otherwise adopts the key's lease (a distributed task's owner
    /// may have changed) and refreshes it once.
    ///
    /// # Errors
    /// An etcd call failed.
    pub fn register_task_once(&mut self) -> Result<(), String> {
        match self.client.get_key_lease(&self.key)? {
            None => {
                let lease = self.client.lease_grant(self.second_ttl).map_err(|e| format!("failed grant a lease: {e}"))?;
                self.cur_lease_id = lease;
                self.client.put_with_lease(&self.key, b"", lease)
            }
            Some(lease) => {
                self.cur_lease_id = lease;
                self.client.lease_keep_alive_once(lease)
            }
        }
    }

    /// Go `Close`: revokes the lease; a lease etcd no longer has is fine.
    ///
    /// # Errors
    /// The revoke failed.
    pub fn close(&mut self) -> Result<(), String> {
        if let Some((stop, handle)) = self.keepalive.take() {
            drop(stop);
            if let Ok(lease) = handle.join() {
                self.cur_lease_id = lease;
            }
        }
        if self.cur_lease_id == NO_LEASE {
            return Ok(());
        }
        match self.client.lease_revoke(self.cur_lease_id) {
            Err(e) if e.contains("requested lease not found") => Ok(()),
            other => other,
        }
    }
}

/// Go `keepaliveLoop`: refreshes the lease; once refreshes fail and the
/// lease is near its end, grants a new one and puts the key again. Returns
/// the lease in use when stopped.
#[allow(clippy::too_many_arguments)]
fn keepalive_loop(
    client: &dyn RegisterEtcd,
    key: &str,
    second_ttl: i64,
    mut lease: i64,
    ttl: Duration,
    threshold: Duration,
    interval: Duration,
    stop: &crossbeam_channel::Receiver<()>,
) -> i64 {
    let mut last_update = std::time::Instant::now();
    let mut need_reput = false;
    loop {
        match stop.recv_timeout(interval) {
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            _ => return lease,
        }
        if !need_reput && client.lease_keep_alive_once(lease).is_ok() {
            last_update = std::time::Instant::now();
            continue;
        }
        if !need_reput {
            tidb_log::warn("the keepalive channel is closed, try to recreate it", &[]);
        }
        if ttl.saturating_sub(last_update.elapsed()) <= threshold {
            match client.lease_grant(second_ttl) {
                Ok(l) => {
                    lease = l;
                    last_update = std::time::Instant::now();
                    need_reput = true;
                }
                Err(e) => {
                    tidb_log::warn(
                        "failed to grant lease",
                        &[tidb_log::Field::new("error", tidb_log::Value::Str(e))],
                    );
                    continue;
                }
            }
        }
        if need_reput {
            match client.put_with_lease(key, b"", lease) {
                Ok(()) => need_reput = false,
                Err(e) => {
                    tidb_log::warn(
                        "failed to put new kv",
                        &[tidb_log::Field::new("error", tidb_log::Value::Str(e))],
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Etcd {
        keys: Mutex<std::collections::BTreeMap<String, i64>>,
        calls: Mutex<Vec<String>>,
        next_lease: Mutex<i64>,
    }

    impl RegisterEtcd for Etcd {
        fn get_key_lease(&self, key: &str) -> Result<Option<i64>, String> {
            Ok(self.keys.lock().unwrap().get(key).copied())
        }
        fn lease_grant(&self, ttl: i64) -> Result<i64, String> {
            let mut next = self.next_lease.lock().unwrap();
            *next += 1;
            self.calls.lock().unwrap().push(format!("grant {ttl}"));
            Ok(*next)
        }
        fn put_with_lease(&self, key: &str, _: &[u8], lease: i64) -> Result<(), String> {
            self.calls.lock().unwrap().push(format!("put {key} {lease}"));
            self.keys.lock().unwrap().insert(key.to_owned(), lease);
            Ok(())
        }
        fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String> {
            self.calls.lock().unwrap().push(format!("keepalive {lease}"));
            Ok(())
        }
        fn lease_revoke(&self, lease: i64) -> Result<(), String> {
            self.calls.lock().unwrap().push(format!("revoke {lease}"));
            Err("etcdserver: requested lease not found".to_owned())
        }
    }

    /// Go `TestTaskRegisterOnce`'s flow: the first call puts the key under a
    /// new lease, a later one (or another owner's register) refreshes the
    /// key's lease, and close revokes it.
    #[test]
    fn register_once_puts_then_refreshes_and_close_revokes() {
        let etcd = Arc::new(Etcd::default());
        let mut r = TaskRegister::new_with_ttl(etcd.clone(), Duration::from_secs(600), RegisterTaskType::ImportInto, "7");
        assert_eq!(r.key(), "/tidb/brie/import/import-into/7");
        r.register_task_once().unwrap();
        r.register_task_once().unwrap();
        let mut other = TaskRegister::new_with_ttl(etcd.clone(), Duration::from_secs(600), RegisterTaskType::ImportInto, "7");
        other.register_task_once().unwrap();
        r.close().unwrap();
        assert_eq!(
            *etcd.calls.lock().unwrap(),
            ["grant 600", "put /tidb/brie/import/import-into/7 1", "keepalive 1", "keepalive 1", "revoke 1"]
        );
        let mut never = TaskRegister::new_with_ttl(etcd, Duration::from_secs(1), RegisterTaskType::Restore, "x");
        never.close().unwrap();
    }

    /// Go `RegisterTask`: grant and put, refresh on a thread, then close
    /// stops the thread and revokes the lease. A failed refresh near the end
    /// of the lease grants a new one and puts the key again.
    #[test]
    fn register_task_keeps_the_lease_alive() {
        struct Flaky(Etcd, std::sync::atomic::AtomicUsize);
        impl RegisterEtcd for Flaky {
            fn get_key_lease(&self, key: &str) -> Result<Option<i64>, String> {
                self.0.get_key_lease(key)
            }
            fn lease_grant(&self, ttl: i64) -> Result<i64, String> {
                self.0.lease_grant(ttl)
            }
            fn put_with_lease(&self, key: &str, v: &[u8], lease: i64) -> Result<(), String> {
                self.0.put_with_lease(key, v, lease)
            }
            fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String> {
                self.0.lease_keep_alive_once(lease)?;
                if self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    return Err("lease expired".into());
                }
                Ok(())
            }
            fn lease_revoke(&self, lease: i64) -> Result<(), String> {
                self.0.lease_revoke(lease)
            }
        }
        let etcd = Arc::new(Flaky(
            Etcd::default(),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        // With a 3s TTL the 20s threshold always applies: a failed refresh
        // grants at once.
        let mut r = TaskRegister::new_with_ttl(
            etcd.clone(),
            Duration::from_secs(3),
            RegisterTaskType::Restore,
            "restore-u",
        );
        r.register_task().unwrap();
        std::thread::sleep(Duration::from_millis(3500));
        r.close().unwrap();
        let calls = etcd.0.calls.lock().unwrap().clone();
        assert_eq!(
            &calls[..5],
            [
                "grant 3",
                "put /tidb/brie/import/restore/restore-u 1",
                "keepalive 1",
                "grant 3",
                "put /tidb/brie/import/restore/restore-u 2"
            ]
        );
        assert_eq!(calls.last().unwrap(), "revoke 2");
    }
}
