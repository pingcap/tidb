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

/// Go `ddl.ownerManager`: the DDL owner manager and its ID, which is the DDL's
/// (and the server info's) ID.
pub struct OwnerManager {
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
/// owner manager over the node's etcd client, once.
///
/// # Errors
///
/// The node has no etcd client.
pub fn start_owner_manager(keyspace: &str, etcd_cli: Option<Arc<EtcdClient>>) -> Result<(), String> {
    let mut managers = managers();
    if managers.contains_key(keyspace) {
        return Ok(());
    }
    let Some(etcd_cli) = etcd_cli else {
        return Err("etcd client is nil, maybe the server is not started with PD".to_owned());
    };
    let id = tidb_domain_serverinfo::serverinfo_syncer::new_node_id();
    let owner_mgr = Arc::new(tidb_owner::OwnerManager::new(
        tidb_owner::Context::background(),
        etcd_cli as Arc<dyn tidb_owner::OwnerStore>,
        PROMPT,
        id.clone(),
        DDL_OWNER_KEY,
    ));
    managers.insert(keyspace.to_owned(), Arc::new(OwnerManager { id, owner_mgr }));
    Ok(())
}

/// Go `CloseOwnerManager`: closes the keyspace's DDL owner manager, revoking
/// its etcd session. A later [`start_owner_manager`] creates a new one, as
/// Go's `started = false` lets it.
pub fn close_owner_manager(keyspace: &str) {
    let manager = managers().remove(keyspace);
    if let Some(manager) = manager {
        manager.owner_mgr.close();
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
            start_owner_manager("owner-mgr-no-etcd", None).err().as_deref(),
            Some("etcd client is nil, maybe the server is not started with PD")
        );
        assert!(get_owner_manager("owner-mgr-no-etcd").is_none());
        close_owner_manager("owner-mgr-no-etcd");
    }
}
