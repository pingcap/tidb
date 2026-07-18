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

//! Dependency-closed ORDER BY metadata from `pkg/planner/util/byitem.go`.
//!
//! Go's `ByItems` owns an expression object plus a descending bit. This leaf
//! keeps the expression as caller-owned display text, preserving ordering
//! metadata, equality, display, clone, list formatting, and the source memory
//! accounting boundary without inventing a Rust expression evaluator.

/// One ORDER BY item with an opaque expression rendering.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ByItem {
    expression: Option<String>,
    desc: bool,
}

impl ByItem {
    /// Creates an item from its caller-owned expression rendering.
    #[must_use]
    pub fn new(expression: impl Into<String>, desc: bool) -> Self {
        Self {
            expression: Some(expression.into()),
            desc,
        }
    }

    /// Creates a source-shaped item with no expression object.
    #[must_use]
    pub const fn empty(desc: bool) -> Self {
        Self {
            expression: None,
            desc,
        }
    }

    /// Returns the opaque expression rendering, when present.
    #[must_use]
    pub fn expression(&self) -> Option<&str> {
        self.expression.as_deref()
    }

    /// Returns whether this item sorts descending.
    #[must_use]
    pub const fn is_desc(&self) -> bool {
        self.desc
    }

    /// Returns source-shaped display text (`expr` or `expr true`).
    #[must_use]
    pub fn display(&self) -> String {
        let expression = self.expression.as_deref().unwrap_or("");
        if self.desc {
            format!("{expression} true")
        } else {
            expression.to_owned()
        }
    }

    /// Returns the source memory accounting for this item.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        std::mem::size_of::<bool>() as i64
            + self.expression.as_ref().map_or(0, |text| text.len() as i64)
    }
}

/// Formats a source-ordered list of ORDER BY items.
#[must_use]
pub fn stringify_by_items(items: &[ByItem]) -> String {
    let mut output = String::from("[");
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(' ');
        }
        output.push_str(&item.display());
    }
    output.push(']');
    output
}
