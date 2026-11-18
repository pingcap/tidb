//! Go `pkg/ddl/owner_mgr.go`: the process's DDL owner manager, one per
//! keyspace, started before the bootstrap so that the node's bootstrap and
//! its DDL campaign the DDL owner through the same etcd session ("we only
//! want it to init session once, to avoid DDL owner change after upgrade").

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use tidb_ddl_util::DDL_OWNER_KEY;
use tidb_pd_client::EtcdClient;

/// Go `ddl.Prompt`.
const PROMPT: &str = "ddl";

/// Go `globalOwnerManagers`.
static GLOBAL_OWNER_MANAGERS: LazyLock<Mutex<HashMap<String, Arc<OwnerManager>>>> =
    LazyLock::new(Mutex::default);

/// Go `ddl.ownerManager`: the DDL owner manager over its own etcd client, and
/// its ID, which is the DDL's (and the server info's) ID.
pub struct OwnerManager {
    etcd_cli: Arc<EtcdClient>,
    id: String,
    owner_mgr: Arc<dyn tidb_owner::Manager>,
}

impl OwnerManager {
    /// Go `ownerManager.ID`.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Go `ownerManager.OwnerManager`.
    #[must_use]
    pub fn owner_manager(&self) -> Arc<dyn tidb_owner::Manager> {
        Arc::clone(&self.owner_mgr)
    }
}

fn managers() -> std::sync::MutexGuard<'static, HashMap<String, Arc<OwnerManager>>> {
    GLOBAL_OWNER_MANAGERS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go `StartOwnerManager` / `ownerManager.Start`: creates the keyspace's DDL
/// owner manager, once, over the etcd client `new_etcd_cli` opens for it (Go
/// `storepkg.NewEtcdCli(store)`: the store's PD endpoints, namespaced by its
/// keyspace). `new_etcd_cli` runs only when the keyspace has no manager yet,
/// and answers `None` for a store without etcd endpoints.
///
/// # Errors
///
/// `new_etcd_cli` failed, or the store has no etcd client.
pub fn start_owner_manager(
    keyspace: &str,
    new_etcd_cli: impl FnOnce() -> Result<Option<EtcdClient>, String>,
) -> Result<(), String> {
    let mut managers = managers();
    if managers.contains_key(keyspace) {
        return Ok(());
    }
    let Some(etcd_cli) = new_etcd_cli()? else {
        return Err("etcd client is nil, maybe the server is not started with PD".to_owned());
    };
    let etcd_cli = Arc::new(etcd_cli);
    let id = tidb_domain_serverinfo::serverinfo_syncer::new_node_id();
    let owner_mgr = Arc::new(tidb_owner::OwnerManager::new(
        tidb_owner::Context::background(),
        Arc::clone(&etcd_cli) as Arc<dyn tidb_owner::OwnerStore>,
        PROMPT,
        id.clone(),
        DDL_OWNER_KEY,
    ));
    managers.insert(
        keyspace.to_owned(),
        Arc::new(OwnerManager {
            etcd_cli,
            id,
            owner_mgr,
        }),
    );
    Ok(())
}

/// Go `CloseOwnerManager` / `ownerManager.Close`: closes the keyspace's DDL
/// owner manager, revoking its etcd session, then its etcd client. A later
/// [`start_owner_manager`] creates a new one, as Go's `started = false` lets
/// it.
pub fn close_owner_manager(keyspace: &str) {
    let manager = managers().remove(keyspace);
    if let Some(manager) = manager {
        manager.owner_mgr.close();
        manager.etcd_cli.close();
    }
}

/// Go `getOwnerManager`.
#[must_use]
pub fn get_owner_manager(keyspace: &str) -> Option<Arc<OwnerManager>> {
    managers().get(keyspace).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `ownerManager.Start`: no etcd client is an error, and nothing is
    /// registered for the keyspace.
    #[test]
    fn start_without_etcd_fails_like_go() {
        assert_eq!(
            start_owner_manager("owner-mgr-no-etcd", || Ok(None)).err().as_deref(),
            Some("etcd client is nil, maybe the server is not started with PD")
        );
        assert!(get_owner_manager("owner-mgr-no-etcd").is_none());
        assert_eq!(
            start_owner_manager("owner-mgr-no-etcd", || Err("dial failed".to_owned()))
                .err()
                .as_deref(),
            Some("dial failed")
        );
        assert!(get_owner_manager("owner-mgr-no-etcd").is_none());
        close_owner_manager("owner-mgr-no-etcd");
    }

    /// Go `ownerManager.Start` opens the manager's own etcd client only when
    /// the keyspace has no manager (`started`); `Close` closes that client,
    /// and the next `Start` opens another.
    #[test]
    fn owner_manager_owns_its_etcd_client() {
        let keyspace = "owner-mgr-own-client";
        let opened = std::cell::Cell::new(0);
        let open = || {
            opened.set(opened.get() + 1);
            EtcdClient::connect(["127.0.0.1:1"], std::time::Duration::from_millis(10))
                .map(Some)
                .map_err(|error| error.to_string())
        };
        start_owner_manager(keyspace, open).unwrap();
        start_owner_manager(keyspace, open).unwrap();
        assert_eq!(opened.get(), 1);
        let manager = get_owner_manager(keyspace).unwrap();
        assert!(!manager.id().is_empty());
        close_owner_manager(keyspace);
        assert!(get_owner_manager(keyspace).is_none());
        start_owner_manager(keyspace, open).unwrap();
        assert_eq!(opened.get(), 2);
        assert_ne!(get_owner_manager(keyspace).unwrap().id(), manager.id());
        close_owner_manager(keyspace);
    }
}
