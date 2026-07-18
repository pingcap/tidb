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

//! Canonical nullable MAX/MIN partial state.

use std::cmp::Ordering;

use tidb_datatype::Datum;
use tidb_planner::aggregation_descriptor::AggregateKind;

use super::super::value_cmp;
use crate::ExecError;

/// One nullable partial state shared by MAX and MIN update, merge, and result.
#[derive(Clone, Debug, PartialEq)]
pub struct MaxMinState {
    kind: AggregateKind,
    value: Option<Datum>,
}

impl MaxMinState {
    /// Creates an empty state for MAX or MIN, rejecting unrelated kinds.
    #[must_use]
    pub fn new(kind: AggregateKind) -> Option<Self> {
        matches!(kind, AggregateKind::Max | AggregateKind::Min)
            .then_some(Self { kind, value: None })
    }

    /// Ignores NULL and retains the first value unless a strict winner arrives.
    pub fn update(&mut self, value: &Datum) -> Result<(), ExecError> {
        if value.is_null() {
            return Ok(());
        }
        let Some(current) = &self.value else {
            // Validate even the first value. Otherwise a range sentinel or
            // unordered real would become the result without ever reaching
            // the comparator.
            value_cmp(value, value)?;
            self.value = Some(value.clone());
            return Ok(());
        };
        if value_cmp(value, current)? == self.wanted_ordering() {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    /// Merges a source partial into this destination using the same strict rule.
    pub fn merge_from(&mut self, source: &Self) -> Result<(), ExecError> {
        if self.kind != source.kind {
            return Err(ExecError::Unsupported("MAX/MIN aggregate kind mismatch"));
        }
        if let Some(value) = &source.value {
            self.update(value)?;
        }
        Ok(())
    }

    /// Resets the partial to SQL NULL without changing its aggregate kind.
    pub fn reset(&mut self) {
        self.value = None;
    }

    /// Returns SQL NULL for an empty/all-NULL input, otherwise the retained value.
    #[must_use]
    pub fn result(&self) -> Datum {
        self.value.clone().unwrap_or(Datum::Null)
    }

    fn wanted_ordering(&self) -> Ordering {
        match self.kind {
            AggregateKind::Max => Ordering::Greater,
            AggregateKind::Min => Ordering::Less,
            _ => unreachable!("constructor accepts only MAX/MIN"),
        }
    }
}
