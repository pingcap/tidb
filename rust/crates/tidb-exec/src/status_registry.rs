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

//! Source-shaped status-variable registry metadata from
//! `pkg/sessionctx/variable/statusvar.go`.
//!
//! The Go owner registers statistics providers, asks each provider for a map,
//! and attaches the provider's scope to every returned value. This leaf keeps
//! that deterministic provider/merge boundary typed. Live `SessionVars`, TLS
//! and atomic counters, warning/error construction, and the Go global mutex
//! remain outside this value owner.

use std::collections::BTreeMap;

/// Scope bits used by status-variable metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatusScope(u8);

impl StatusScope {
    /// A status visible at global scope.
    pub const GLOBAL: Self = Self(0b01);
    /// A status visible at session scope.
    pub const SESSION: Self = Self(0b10);
    /// The source default: visible at both global and session scope.
    pub const DEFAULT: Self = Self(Self::GLOBAL.0 | Self::SESSION.0);

    /// Combines scope bits without introducing a new scope value.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns the source bit representation.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

impl std::ops::BitOr for StatusScope {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

/// The dependency-closed scalar values needed by the registry boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusValue {
    /// A textual status value.
    Text(String),
    /// A signed numeric status value.
    Signed(i64),
    /// An unsigned numeric status value.
    Unsigned(u64),
    /// A boolean status value.
    Boolean(bool),
}

/// One collected status value and its provider-selected scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusVal {
    /// Scope in which this status is visible.
    pub scope: StatusScope,
    /// Value returned by the owning provider.
    pub value: StatusValue,
}

/// A source-shaped provider of status-variable maps.
pub trait StatusProvider {
    /// Returns the scope for one status name.
    fn scope(&self, status: &str) -> StatusScope;

    /// Returns this provider's status values.
    fn stats(&self) -> Result<BTreeMap<String, StatusValue>, String>;
}

/// Opaque registration token used to remove one provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Registration(u64);

/// Deterministic status-provider registry.
#[derive(Default)]
pub struct StatusRegistry {
    providers: Vec<(Registration, Box<dyn StatusProvider>)>,
    next_registration: u64,
}

impl StatusRegistry {
    /// Registers a provider and returns its removal token.
    #[must_use = "retain the registration token to unregister this provider"]
    pub fn register<P>(&mut self, provider: P) -> Registration
    where
        P: StatusProvider + 'static,
    {
        let registration = Registration(self.next_registration);
        self.next_registration = self.next_registration.wrapping_add(1);
        self.providers.push((registration, Box::new(provider)));
        registration
    }

    /// Removes a provider, using the source swap-with-last list behavior.
    #[must_use]
    pub fn unregister(&mut self, registration: Registration) -> bool {
        let Some(index) = self
            .providers
            .iter()
            .position(|(candidate, _)| *candidate == registration)
        else {
            return false;
        };
        self.providers.swap_remove(index);
        true
    }

    /// Collects provider values and attaches each provider's scope.
    pub fn collect(&self) -> Result<BTreeMap<String, StatusVal>, String> {
        let mut values = BTreeMap::new();
        for (_, provider) in &self.providers {
            for (name, value) in provider.stats()? {
                let scope = provider.scope(&name);
                values.insert(name, StatusVal { scope, value });
            }
        }
        Ok(values)
    }
}
