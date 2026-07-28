// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An account write is only real if this node's OWN loader reads it back.
//!
//! The keyspace here is a real bootstrap -- the same
//! [`tidb_exec::mysql_bootstrap`] plan whose rows a live Go TiDB reads -- so
//! `mysql.user` and friends have their true stored schema, their true indexes
//! and their true `ENUM` columns. Every assertion goes through
//! [`load_cluster_privileges`], because the invariant this writer must keep is
//! exactly `load(apply(plan(desired))) == desired`: anything the loader cannot
//! read back is a row a Go TiDB cannot read back either.

use std::collections::BTreeMap;

use tidb_datatype::{Time, TimeType};
use tidb_exec::cluster_account_write::plan_account_write;
use tidb_exec::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaPairs, MetaSnapshot,
};
use tidb_exec::cluster_privilege_load::{
    load_cluster_privileges, ClusterPrivileges, LoadedDbGrant, LoadedDefaultRole,
    LoadedDynamicGrant, LoadedRoleEdge, LoadedTableGrant, LoadedUser,
};
use tidb_exec::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapEnvironment};
use tidb_txnkv::transaction::{OptimisticMutation, OptimisticMutationKind};

fn timestamp() -> Time {
    Time::from_date_checked(2026, 7, 29, 6, 12, 55, 0, TimeType::Timestamp, 0)
        .expect("a fixed calendar date is a valid timestamp")
}

fn environment() -> BootstrapEnvironment {
    BootstrapEnvironment {
        system_tz: "Asia/Shanghai".to_owned(),
        new_collation_enabled: true,
        cluster_id: 7_667_705_271_188_879_689,
        current_timestamp: timestamp(),
        ddl_table_version: 0,
    }
}

#[derive(Default)]
struct MetaStore {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MetaSnapshot for MetaStore {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        Ok(self
            .pairs
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl MetaStore {
    /// Applies one planned change, the way a committed transaction would.
    ///
    /// An `Insert` whose key already exists is what TiKV rejects as an
    /// assertion failure, and a `Delete`/`PutExisting` of a key that does not
    /// is the mirror of it. Both are the writer's own bugs, so this stand-in
    /// for the 2PC checks them rather than quietly doing the write.
    fn apply(&mut self, mutations: &[OptimisticMutation]) {
        for mutation in mutations {
            let present = self.pairs.contains_key(mutation.key());
            match mutation.kind() {
                OptimisticMutationKind::Insert => {
                    assert!(!present, "an Insert would have overwritten a stored row");
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
                OptimisticMutationKind::PutExisting => {
                    assert!(present, "a PutExisting named a row that is not stored");
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
                OptimisticMutationKind::Delete => {
                    assert!(present, "a Delete named a row that is not stored");
                    self.pairs.remove(mutation.key());
                }
                OptimisticMutationKind::IndexDelete | OptimisticMutationKind::MetaDelete => {
                    self.pairs.remove(mutation.key());
                }
                OptimisticMutationKind::IndexPut | OptimisticMutationKind::MetaPut => {
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
            }
        }
    }

    fn catalog(&mut self) -> ClusterCatalog {
        load_cluster_catalog(self).expect("the bootstrapped catalog loads")
    }

    fn accounts(&mut self) -> ClusterPrivileges {
        let catalog = self.catalog();
        load_cluster_privileges(self, &catalog).expect("the accounts load")
    }

    /// Plans `desired`, applies it, and answers what the loader reads back.
    fn write(&mut self, desired: &ClusterPrivileges) -> ClusterPrivileges {
        let catalog = self.catalog();
        let plan = plan_account_write(self, &catalog, desired, timestamp())
            .expect("the account image is writable");
        self.apply(&plan.mutations);
        self.accounts()
    }
}

fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    let write = plan_mysql_bootstrap(&mut store, 467_996_279_696_261_139, &environment())
        .expect("a fresh keyspace bootstraps");
    let mutations = write.mutations;
    store.apply(&mutations);
    store
}

fn user(name: &str, host: &str, privileges: &[&'static str]) -> LoadedUser {
    LoadedUser {
        host: host.to_owned(),
        user: name.to_owned(),
        authentication_string: String::new(),
        plugin: "mysql_native_password".to_owned(),
        account_locked: false,
        password_expired: false,
        privileges: privileges.to_vec(),
    }
}

/// The bootstrap's own `root` row, as the loader reads it -- every image below
/// keeps it, because dropping it from the desired image would drop the account
/// from the cluster.
fn root(store: &mut MetaStore) -> LoadedUser {
    store
        .accounts()
        .users
        .into_iter()
        .find(|stored| stored.user == "root")
        .expect("the bootstrap seeded root")
}

#[test]
fn a_new_account_is_written_as_a_row_this_nodes_own_loader_reads_back() {
    // The whole bridge in one assertion: what a `CREATE USER` did to the
    // account table becomes a `mysql.user` row, indexes included, that reads
    // back as the same account.
    let mut store = bootstrapped();
    let root = root(&mut store);
    let desired = ClusterPrivileges {
        users: vec![root.clone(), user("bob", "%", &["SELECT"])],
        ..ClusterPrivileges::default()
    };
    let read_back = store.write(&desired);
    assert_eq!(read_back.users.len(), 2);
    let bob = read_back
        .users
        .iter()
        .find(|stored| stored.user == "bob")
        .expect("the new account is stored");
    assert_eq!(bob.privileges, vec!["SELECT"]);
    assert_eq!(bob.plugin, "mysql_native_password");
    assert!(!bob.account_locked);
    // Nothing about root moved: a whole-image write must not disturb the rows
    // the change did not name.
    assert_eq!(
        read_back.users.iter().find(|stored| stored.user == "root"),
        Some(&root)
    );
}

#[test]
fn a_grant_and_a_revoke_rewrite_the_row_in_place() {
    let mut store = bootstrapped();
    let root = root(&mut store);
    let mut desired = ClusterPrivileges {
        users: vec![root.clone(), user("bob", "%", &[])],
        ..ClusterPrivileges::default()
    };
    store.write(&desired);

    desired.users[1] = user("bob", "%", &["SELECT", "INSERT", "GRANT OPTION"]);
    let granted = store.write(&desired);
    let bob = granted
        .users
        .iter()
        .find(|stored| stored.user == "bob")
        .expect("bob is still stored");
    assert_eq!(bob.privileges, vec!["SELECT", "INSERT", "GRANT OPTION"]);

    desired.users[1] = user("bob", "%", &["INSERT"]);
    let revoked = store.write(&desired);
    assert_eq!(
        revoked
            .users
            .iter()
            .find(|stored| stored.user == "bob")
            .expect("bob survives a revoke")
            .privileges,
        vec!["INSERT"]
    );
    // One account, one row: a rewrite must not have left a second one behind.
    assert_eq!(revoked.users.len(), 2);
}

#[test]
fn a_dropped_account_takes_its_row_and_every_grant_row_with_it() {
    // `DROP USER` is where a writer that forgot the dependent tables leaves an
    // orphaned grant that a later account of the same name inherits.
    let mut store = bootstrapped();
    let root = root(&mut store);
    let full = ClusterPrivileges {
        users: vec![root.clone(), user("bob", "%", &["SELECT"])],
        db_grants: vec![LoadedDbGrant {
            host: "%".to_owned(),
            user: "bob".to_owned(),
            database: "app".to_owned(),
            privileges: vec!["SELECT", "INSERT"],
        }],
        dynamic_grants: vec![LoadedDynamicGrant {
            host: "%".to_owned(),
            user: "bob".to_owned(),
            privilege: "BACKUP_ADMIN".to_owned(),
            with_grant_option: false,
        }],
        ..ClusterPrivileges::default()
    };
    let stored = store.write(&full);
    assert_eq!(stored.db_grants.len(), 1);
    assert_eq!(stored.dynamic_grants.len(), 1);

    let dropped = store.write(&ClusterPrivileges {
        users: vec![root],
        ..ClusterPrivileges::default()
    });
    assert_eq!(dropped.users.len(), 1);
    assert!(dropped.db_grants.is_empty());
    assert!(dropped.dynamic_grants.is_empty());
}

#[test]
fn roles_and_default_roles_survive_the_round_trip() {
    let mut store = bootstrapped();
    let root = root(&mut store);
    let mut app_role = user("approle", "%", &[]);
    // Go stores a role as a locked, passwordless account; the round trip has
    // to keep that distinction or a role becomes a login.
    app_role.account_locked = true;
    let desired = ClusterPrivileges {
        users: vec![root, app_role, user("bob", "%", &[])],
        role_edges: vec![LoadedRoleEdge {
            role_host: "%".to_owned(),
            role_user: "approle".to_owned(),
            grantee_host: "%".to_owned(),
            grantee_user: "bob".to_owned(),
        }],
        default_roles: vec![LoadedDefaultRole {
            host: "%".to_owned(),
            user: "bob".to_owned(),
            role_host: "%".to_owned(),
            role_user: "approle".to_owned(),
        }],
        ..ClusterPrivileges::default()
    };
    let read_back = store.write(&desired);
    assert_eq!(read_back.role_edges, desired.role_edges);
    assert_eq!(read_back.default_roles, desired.default_roles);
    assert!(
        read_back
            .users
            .iter()
            .find(|stored| stored.user == "approle")
            .expect("the role is stored")
            .account_locked
    );
}

#[test]
fn an_image_the_cluster_already_holds_plans_nothing() {
    // The no-op is what makes `CREATE USER IF NOT EXISTS` on an existing
    // account spend no commit, and it is also the strongest statement of the
    // round-trip invariant: what the loader read is exactly what the writer
    // would write.
    let mut store = bootstrapped();
    let stored = store.accounts();
    let catalog = store.catalog();
    let plan = plan_account_write(&mut store, &catalog, &stored, timestamp())
        .expect("the stored image is writable");
    assert!(
        plan.is_empty(),
        "writing back what was read planned {} mutations",
        plan.mutations.len()
    );
    assert!(plan.changed_users.is_empty());
}

#[test]
fn a_table_scoped_grant_is_refused_by_name_rather_than_silently_dropped() {
    // `mysql.tables_priv` stores its privileges in a `SET` column this writer
    // does not encode. Dropping such a grant on the floor would be a silent
    // privilege change, so the plan refuses and says which table stopped it.
    let mut store = bootstrapped();
    let root = root(&mut store);
    let catalog = store.catalog();
    let desired = ClusterPrivileges {
        users: vec![root, user("bob", "%", &[])],
        table_grants: vec![LoadedTableGrant {
            host: "%".to_owned(),
            user: "bob".to_owned(),
            database: "app".to_owned(),
            table: "t".to_owned(),
            privileges: vec!["Select".to_owned()],
        }],
        ..ClusterPrivileges::default()
    };
    let refusal = plan_account_write(&mut store, &catalog, &desired, timestamp())
        .expect_err("a table-scoped grant must be refused");
    let message = refusal.to_string();
    assert!(
        message.contains("mysql.tables_priv"),
        "the refusal does not name what stopped it: {message}"
    );
}

#[test]
fn the_changed_accounts_are_named_so_a_notification_can_carry_them() {
    let mut store = bootstrapped();
    let root = root(&mut store);
    let catalog = store.catalog();
    let desired = ClusterPrivileges {
        users: vec![root, user("bob", "%", &["SELECT"])],
        ..ClusterPrivileges::default()
    };
    let plan = plan_account_write(&mut store, &catalog, &desired, timestamp())
        .expect("the account image is writable");
    assert_eq!(plan.changed_users, vec!["'bob'@'%'".to_owned()]);
}

#[test]
fn two_new_accounts_in_one_statement_take_distinct_handles() {
    // One `CREATE USER` may name several accounts, and two rows sharing a
    // `_tidb_rowid` would be one row: the second silently replaces the first.
    let mut store = bootstrapped();
    let root = root(&mut store);
    let desired = ClusterPrivileges {
        users: vec![
            root,
            user("bob", "%", &[]),
            user("carol", "%", &[]),
            user("dave", "localhost", &[]),
        ],
        ..ClusterPrivileges::default()
    };
    let read_back = store.write(&desired);
    assert_eq!(read_back.users.len(), 4);
}

#[test]
fn a_row_written_now_is_still_writable_after_a_reload() {
    // The allocator watermark is what stops a second statement from handing
    // out a handle the first already used; this is that, end to end.
    let mut store = bootstrapped();
    let root = root(&mut store);
    let mut desired = ClusterPrivileges {
        users: vec![root, user("bob", "%", &[])],
        ..ClusterPrivileges::default()
    };
    store.write(&desired);
    desired.users.push(user("carol", "%", &[]));
    let read_back = store.write(&desired);
    assert_eq!(read_back.users.len(), 3);
    desired.users.push(user("dave", "%", &[]));
    assert_eq!(store.write(&desired).users.len(), 4);
}
