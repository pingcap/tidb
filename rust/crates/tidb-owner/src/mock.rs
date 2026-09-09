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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::{lock, Context, Listener, Manager, OpType};

/// Go `MockGlobalStateEntry`.
pub static MOCK_GLOBAL_STATE_ENTRY: OnceLock<Arc<MockGlobalState>> = OnceLock::new();
static MOCK_OWNER_OP_VALUE: AtomicU8 = AtomicU8::new(OpType::NONE.as_byte());

/// Process-wide mock owner state, keyed by `(store ID, owner key)`.
#[derive(Debug, Default)]
pub struct MockGlobalState {
    current_owner: Mutex<HashMap<(String, String), String>>,
}

impl MockGlobalState {
    /// Returns a selector for one store and owner category.
    pub fn owner_key(self: &Arc<Self>, store_id: &str, owner_key: &str) -> MockGlobalStateSelector {
        MockGlobalStateSelector {
            state: Arc::clone(self),
            store_id: store_id.to_owned(),
            owner_key: owner_key.to_owned(),
        }
    }
}

/// One `(store ID, owner key)` view of mock global state.
#[derive(Clone, Debug)]
pub struct MockGlobalStateSelector {
    state: Arc<MockGlobalState>,
    store_id: String,
    owner_key: String,
}

impl MockGlobalStateSelector {
    fn map_key(&self) -> (String, String) {
        (self.store_id.clone(), self.owner_key.clone())
    }

    /// Returns the current owner.
    pub fn get_owner(&self) -> String {
        lock(&self.state.current_owner)
            .get(&self.map_key())
            .cloned()
            .unwrap_or_default()
    }

    /// Sets the owner iff no owner exists.
    pub fn set_owner(&self, owner: &str) -> bool {
        let mut owners = lock(&self.state.current_owner);
        let current = owners.entry(self.map_key()).or_default();
        if current.is_empty() {
            owner.clone_into(current);
            true
        } else {
            false
        }
    }

    /// Clears the owner iff it equals `owner`.
    pub fn unset_owner(&self, owner: &str) -> bool {
        let mut owners = lock(&self.state.current_owner);
        let current = owners.entry(self.map_key()).or_default();
        if current == owner {
            current.clear();
            true
        } else {
            false
        }
    }

    /// Whether `owner` is current.
    pub fn is_owner(&self, owner: &str) -> bool {
        self.get_owner() == owner
    }
}

struct MockCampaign {
    stop: Arc<AtomicBool>,
    resign: Arc<AtomicBool>,
    worker: JoinHandle<()>,
}

struct MockInner {
    id: String,
    selector: MockGlobalStateSelector,
    context: Context,
    listener: Mutex<Option<Arc<dyn Listener>>>,
    campaign: Mutex<Option<MockCampaign>>,
    closed: AtomicBool,
}

/// Local-store manager used by Go for unistore and tests.
#[derive(Clone)]
pub struct MockManager {
    inner: Arc<MockInner>,
}

impl MockManager {
    /// Creates a mock manager. A missing store ID uses Go's
    /// `"mock_store_id"` identity.
    pub fn new(
        context: Context,
        id: impl Into<String>,
        store_id: Option<&str>,
        owner_key: &str,
    ) -> Self {
        MOCK_OWNER_OP_VALUE.store(OpType::NONE.as_byte(), Ordering::Release);
        let state = Arc::clone(
            MOCK_GLOBAL_STATE_ENTRY.get_or_init(|| Arc::new(MockGlobalState::default())),
        );
        Self {
            inner: Arc::new(MockInner {
                id: id.into(),
                selector: state.owner_key(store_id.unwrap_or("mock_store_id"), owner_key),
                context,
                listener: Mutex::new(None),
                campaign: Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the process-wide mock state.
    #[must_use]
    pub fn global_state() -> Arc<MockGlobalState> {
        Arc::clone(MOCK_GLOBAL_STATE_ENTRY.get_or_init(|| Arc::new(MockGlobalState::default())))
    }
}

impl Manager for MockManager {
    fn id(&self) -> &str {
        &self.inner.id
    }

    fn is_owner(&self) -> bool {
        self.inner.selector.is_owner(&self.inner.id)
    }

    fn retire_owner(&self) {
        if self.inner.selector.unset_owner(&self.inner.id) {
            if let Some(listener) = lock(&self.inner.listener).as_ref() {
                listener.on_retire_owner();
            }
        }
    }

    fn get_owner_id(&self, _context: &Context) -> Result<String, String> {
        self.is_owner()
            .then(|| self.inner.id.clone())
            .ok_or_else(|| "no owner".to_owned())
    }

    fn set_owner_op_value(&self, _context: &Context, op: OpType) -> Result<(), String> {
        MOCK_OWNER_OP_VALUE.store(op.as_byte(), Ordering::Release);
        Ok(())
    }

    fn campaign_owner(&self, _with_ttl: &[i64]) -> Result<(), String> {
        if lock(&self.inner.campaign).is_some() {
            return Ok(());
        }
        let stop = Arc::new(AtomicBool::new(false));
        let resign = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_resign = Arc::clone(&resign);
        let manager = self.clone();
        let worker = std::thread::Builder::new()
            .name("mock-owner".to_owned())
            .spawn(move || {
                while !worker_stop.load(Ordering::Acquire) && !manager.inner.context.is_done() {
                    if worker_resign.swap(false, Ordering::AcqRel) {
                        manager.retire_owner();
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    if manager.inner.selector.set_owner(&manager.inner.id) {
                        if let Some(listener) = lock(&manager.inner.listener).as_ref() {
                            listener.on_become_owner();
                        }
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
                manager.retire_owner();
            })
            .map_err(|error| error.to_string())?;
        *lock(&self.inner.campaign) = Some(MockCampaign {
            stop,
            resign,
            worker,
        });
        Ok(())
    }

    fn campaign_cancel(&self) {
        let campaign = lock(&self.inner.campaign).take();
        if let Some(campaign) = campaign {
            campaign.stop.store(true, Ordering::Release);
            let _ = campaign.worker.join();
        }
    }

    fn break_campaign_loop(&self) {
        self.close();
    }

    fn resign_owner(&self, _context: &Context) -> Result<(), String> {
        if let Some(campaign) = lock(&self.inner.campaign).as_ref() {
            campaign.resign.store(true, Ordering::Release);
        }
        Ok(())
    }

    fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.campaign_cancel();
    }

    fn set_listener(&self, listener: Arc<dyn Listener>) {
        *lock(&self.inner.listener) = Some(listener);
    }

    fn force_to_be_owner(&self, _context: &Context) -> Result<(), String> {
        Ok(())
    }
}

impl Drop for MockManager {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.close();
        }
    }
}

/// Reads Go's global mock owner operation value.
#[must_use]
pub(crate) fn mock_owner_op_value() -> OpType {
    match MOCK_OWNER_OP_VALUE.load(Ordering::Acquire) {
        1 => OpType::SYNC_UPGRADING_STATE,
        value => OpType::from_byte(value),
    }
}
