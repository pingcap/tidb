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

//! The instance's published `mysql.expr_pushdown_blacklist` and
//! `mysql.opt_rule_blacklist`, which every session of one server plans
//! against.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Go `expression.DefaultExprPushDownBlacklist`'s map: a lowercased name to
/// its store-type mask (`tidb_expr::infer_pushdown::ExprPushDownBlacklist`).
type ExprPushDownBlacklist = HashMap<String, u32>;

/// Go's two process-wide atomics -- `expression.DefaultExprPushDownBlacklist`
/// and `plannercore.DefaultDisabledLogicalRulesList` -- held at this tier's
/// INSTANCE boundary instead.
///
/// Go's are package-level because its optimizer runs far from any session,
/// and the scope that gives them is one SERVER: `ADMIN RELOAD` on one
/// connection changes what every other connection plans. A per-session copy
/// would not, so this is a shared handle a front end installs on each session
/// it opens, the same way it installs `tidb_privilege::PrivilegeRegistry`.
///
/// A reload REPLACES the published map rather than mutating one, which is
/// what Go's atomic `Store` does: a statement that already snapshotted the
/// old map keeps planning against it, and no reader ever sees a half-built
/// one.
#[derive(Clone, Debug, Default)]
pub struct PushdownBlacklists {
    expressions: Arc<Mutex<Arc<ExprPushDownBlacklist>>>,
    rules: Arc<Mutex<Arc<HashSet<String>>>>,
    /// Go `ExprPushDownBlackListReloadTimeStamp`, as a counter rather than a
    /// wall clock: bumped by every publish, and mixed into the prepared plan
    /// cache's key (`plan_cache_utils.go:443`) so a reload invalidates every
    /// cached plan.
    generation: Arc<std::sync::atomic::AtomicU64>,
}

impl PushdownBlacklists {
    /// The two published maps as they stand now, for one statement to plan
    /// against.
    pub fn snapshot(&self) -> (Arc<ExprPushDownBlacklist>, Arc<HashSet<String>>) {
        (
            Arc::clone(&self.expressions.lock().expect("blacklist mutex")),
            Arc::clone(&self.rules.lock().expect("blacklist mutex")),
        )
    }

    pub fn publish_expressions(&self, loaded: ExprPushDownBlacklist) {
        *self.expressions.lock().expect("blacklist mutex") = Arc::new(loaded);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// The reload counter the prepared plan cache mixes into its key.
    pub fn generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn publish_rules(&self, loaded: HashSet<String>) {
        *self.rules.lock().expect("blacklist mutex") = Arc::new(loaded);
    }
}

