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

use super::{LeaderRequest, RegionAttempt};

/// Typed outcome returned for one immutable logical-target/physical-route pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteOutcome {
    /// The physical dispatch returned a usable response from the logical target.
    Success,
    /// The physical dispatch failed before returning a usable response.
    Failure,
}

/// Exact route generations attached to one transport outcome.
///
/// RegionCache can compare both captured generations with its canonical state
/// before applying feedback. A delayed result therefore cannot update a newer
/// logical target or proxy generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteFeedback {
    target: RegionAttempt,
    proxy: Option<RegionAttempt>,
    outcome: RouteOutcome,
}

impl RouteFeedback {
    /// Captures the exact immutable route used by one completed request.
    #[must_use]
    pub fn from_request(request: &LeaderRequest, outcome: RouteOutcome) -> Self {
        Self {
            target: request.target().clone(),
            proxy: request.proxy().cloned(),
            outcome,
        }
    }

    /// Logical target generation which interpreted the region request.
    #[must_use]
    pub const fn target(&self) -> &RegionAttempt {
        &self.target
    }

    /// Optional physical proxy generation used by transport.
    #[must_use]
    pub const fn proxy(&self) -> Option<&RegionAttempt> {
        self.proxy.as_ref()
    }

    /// Physical generation to which this outcome applies.
    #[must_use]
    pub fn dispatch_attempt(&self) -> &RegionAttempt {
        self.proxy.as_ref().unwrap_or(&self.target)
    }

    /// Success or failure class observed on the physical route.
    #[must_use]
    pub const fn outcome(&self) -> RouteOutcome {
        self.outcome
    }
}
