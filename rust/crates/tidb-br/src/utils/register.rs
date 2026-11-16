// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `br/pkg/utils/register.go`: a task registered in PD's etcd under a
//! lease, so BR and changefeeds can see a running import. Only the
//! `RegisterTaskOnce` form (the caller refreshes the lease) is ported.

use std::sync::Arc;
use std::time::Duration;

/// Go `RegisterImportTaskPrefix`.
pub const REGISTER_IMPORT_TASK_PREFIX: &str = "/tidb/brie/import";

/// Go `clientv3.NoLease`.
const NO_LEASE: i64 = 0;

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
        }
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
        if self.cur_lease_id == NO_LEASE {
            return Ok(());
        }
        match self.client.lease_revoke(self.cur_lease_id) {
            Err(e) if e.contains("requested lease not found") => Ok(()),
            other => other,
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
}
