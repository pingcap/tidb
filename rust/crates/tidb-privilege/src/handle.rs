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


//! Go `pkg/privilege/privileges/cache.go`'s `Handle`: the loaded rows behind
//! the live [`PrivilegeRegistry`], whether they are every user's
//! (`fullData`), and the users loaded so far (`activeUsers`). Every clone of
//! a registry shares its handle, as every Go session shares the domain's.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tidb_syssession::mysql_system_tables::SystemTableError;

use crate::cluster_privilege_load::{
    find_user_and_all_roles, load_all, load_role_graph, load_some_users, PrivilegeTables,
};
use crate::loaded::ClusterPrivileges;
use crate::PrivilegeRegistry;

/// Go `Handle.sctx`, the domain's system session pool: runs `load` over the
/// `mysql.*` tables one session reads.
pub trait PrivilegeLoader: Send + Sync {
    /// Go `h.sctx.Get()` .. `h.sctx.Put(res)` around `load`.
    ///
    /// # Errors
    ///
    /// No session could be taken, or `load` failed.
    fn with_tables(
        &self,
        load: &mut dyn FnMut(&mut dyn PrivilegeTables) -> Result<(), SystemTableError>,
    ) -> Result<(), String>;
}

/// Go `Handle`'s fields beside the published `priv`.
#[derive(Default)]
pub(crate) struct HandleState {
    /// Go `sctx`, set where Go's `NewHandle` runs (`LoadPrivilegeLoop`).
    loader: RwLock<Option<Arc<dyn PrivilegeLoader>>>,
    /// The rows Go's published `MySQLPrivilege` holds.
    data: Mutex<ClusterPrivileges>,
    /// Go `activeUsers`.
    active_users: Mutex<BTreeSet<String>>,
    /// Go `fullData`.
    full_data: AtomicBool,
}

impl PrivilegeRegistry {
    /// Go `NewHandle(sctx, ...)`: the session pool this registry's handle
    /// loads through.
    pub fn set_privilege_loader(&self, loader: Arc<dyn PrivilegeLoader>) {
        *self
            .handle
            .loader
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(loader);
    }

    fn privilege_loader(&self) -> Result<Arc<dyn PrivilegeLoader>, String> {
        self.handle
            .loader
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| "the privilege handle has no session pool".to_owned())
    }

    /// Go `Handle.fullData`: whether every user's rows are loaded.
    #[must_use]
    pub fn full_data(&self) -> bool {
        self.handle.full_data.load(Ordering::Acquire)
    }

    /// Publishes `data` as this registry's tables.
    fn publish(&self, data: ClusterPrivileges) {
        self.replace_from(&crate::load::registry_from_cluster(&data).registry);
        *self
            .handle
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = data;
    }

    /// Go `Handle.UpdateAll`: every user's rows.
    ///
    /// # Errors
    ///
    /// No session pool, or the load failed (a missing `mysql.user` included,
    /// Go `errLoadPrivilege`).
    pub fn update_all(&self) -> Result<(), String> {
        let mut loaded = None;
        self.privilege_loader()?.with_tables(&mut |tables| {
            loaded = Some(load_all(tables)?);
            Ok(())
        })?;
        self.publish(loaded.expect("a successful load"));
        self.handle.full_data.store(true, Ordering::Release);
        Ok(())
    }

    /// Go `Handle.UpdateAllActive`: the rows of every user loaded so far.
    ///
    /// # Errors
    ///
    /// See [`Self::update_all`].
    pub fn update_all_active(&self) -> Result<(), String> {
        self.handle.full_data.store(false, Ordering::Release);
        let user_list: Vec<String> = self
            .handle
            .active_users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        self.update_users(&user_list)
    }

    /// Go `Handle.Update`: the rows of `user_list`, when one of them is
    /// loaded already.
    ///
    /// # Errors
    ///
    /// See [`Self::update_all`].
    pub fn update(&self, user_list: &[String]) -> Result<(), String> {
        self.handle.full_data.store(false, Ordering::Release);
        let need_reload = {
            let active = self
                .handle
                .active_users
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            user_list.iter().any(|user| active.contains(user))
        };
        if !need_reload {
            return Ok(());
        }
        self.update_users(user_list)
    }

    /// Go `Handle.ensureActiveUser`: loads `user`'s rows unless every user's
    /// are, or `user`'s already are. A registry with no session pool to load
    /// through (an in-process session's) holds every row it has.
    ///
    /// # Errors
    ///
    /// See [`Self::update_all`].
    pub fn ensure_active_user(&self, user: &str) -> Result<(), String> {
        if self.full_data() || self.privilege_loader().is_err() {
            return Ok(());
        }
        if self
            .handle
            .active_users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(user)
        {
            return Ok(());
        }
        self.update_users(&[user.to_owned()])
    }

    /// Go `terror.Log(h.ensureActiveUser(ctx, user))`, as Go's checks of an
    /// account other than the session's own and its login make it.
    pub fn log_ensure_active_user(&self, user: &str) {
        if let Err(error) = self.ensure_active_user(user) {
            let error: Box<dyn std::error::Error> = error.into();
            tidb_terror::log(Some(error.as_ref()));
        }
    }

    /// Go `Handle.updateUsers`: the whole role graph, then the rows of
    /// `user_list` and every role they hold, merged in.
    fn update_users(&self, user_list: &[String]) -> Result<(), String> {
        let mut loaded = None;
        self.privilege_loader()?.with_tables(&mut |tables| {
            let role_edges = load_role_graph(tables)?;
            let users = find_user_and_all_roles(user_list, &role_edges);
            let mut diff = load_some_users(tables, &users)?;
            diff.role_edges = role_edges;
            loaded = Some((diff, users));
            Ok(())
        })?;
        let (diff, users) = loaded.expect("a successful load");
        self.merge(&diff, &users);
        Ok(())
    }

    /// Go `Handle.merge`.
    fn merge(&self, diff: &ClusterPrivileges, users: &BTreeSet<String>) {
        let merged = self
            .handle
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .merge(diff, users);
        self.publish(merged);
        self.handle
            .active_users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(users.iter().cloned());
    }
}
