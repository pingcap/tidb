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

use std::sync::{Arc, Barrier};

use super::RowContainer;

fn pause_next(set_hook: impl FnOnce(Arc<dyn Fn() + Send + Sync>)) -> (Arc<Barrier>, Arc<Barrier>) {
    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let hook_started = Arc::clone(&started);
    let hook_release = Arc::clone(&release);
    set_hook(Arc::new(move || {
        hook_started.wait();
        hook_release.wait();
    }));
    (started, release)
}

pub(super) fn pause_next_spill(rc: &RowContainer) -> (Arc<Barrier>, Arc<Barrier>) {
    pause_next(|hook| rc.set_spill_start_hook(Some(hook)))
}

pub(super) fn pause_next_reentrant_action(rc: &RowContainer) -> (Arc<Barrier>, Arc<Barrier>) {
    pause_next(|hook| rc.set_reentrant_action_hook(Some(hook)))
}

pub(super) fn pause_next_later_action(rc: &RowContainer) -> (Arc<Barrier>, Arc<Barrier>) {
    pause_next(|hook| rc.set_later_action_hook(Some(hook)))
}

pub(super) fn pause_next_fallback_claim(rc: &RowContainer) -> (Arc<Barrier>, Arc<Barrier>) {
    pause_next(|hook| rc.set_before_fallback_hook(Some(hook)))
}
