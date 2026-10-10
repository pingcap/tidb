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

//! The cluster's BDR role in TiKV meta (Go's meta key `BDRRole`), for
//! `ADMIN SET / UNSET / SHOW BDR ROLE` on this node.
//!
//! Go writes the key through the statement's own transaction
//! (`executeAdminSetBDRRole`) and reads it in a new one
//! (`AdminShowBDRRoleExec`). This node runs the write in its own
//! transaction too, so inside an explicit transaction the role is set at
//! once rather than at that transaction's COMMIT.

use std::sync::Arc;
use std::time::Duration;

use tidb_exec::meta_txn::{MetaTxnError, MetaTxnStorage};
use tidb_meta::transaction::Mutator;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};
use tidb_txnkv::{run_in_new_txn, NewTxnStorage, NewTxnTransaction, RunInNewTxnContext};

pub(super) struct ClusterBdrRoleStore<C, L, P: StorePdCapability> {
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
}

impl<C, L, P: StorePdCapability> ClusterBdrRoleStore<C, L, P> {
    pub(super) fn new(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
    ) -> Self {
        Self { opener, timeout }
    }
}

impl<C, L, P> tidb_session::BdrRoleStore for ClusterBdrRoleStore<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn get(&self) -> Result<String, String> {
        let mut storage = MetaTxnStorage::new(&self.opener, self.timeout);
        let mut transaction = storage.begin().map_err(|error| error.to_string())?;
        let role = Mutator::new(&mut transaction)
            .bdr_role()
            .map_err(|error| error.to_string());
        let _ = NewTxnTransaction::rollback(&mut transaction);
        Ok(String::from_utf8_lossy(&role?).into_owned())
    }

    fn set(&self, role: &str) -> Result<(), String> {
        let mut storage = MetaTxnStorage::new(&self.opener, self.timeout);
        run_in_new_txn(
            &RunInNewTxnContext::default(),
            &mut storage,
            true,
            |transaction| {
                let mutator = Mutator::new(&mut *transaction);
                if role.is_empty() {
                    mutator.clear_bdr_role()
                } else {
                    mutator.set_bdr_role(role.as_bytes())
                }
                .map_err(MetaTxnError::from)
            },
        )
        .map_err(|error| error.to_string())
    }
}
