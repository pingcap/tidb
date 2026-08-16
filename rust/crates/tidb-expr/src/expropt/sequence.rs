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

//! Go `pkg/expression/expropt/sequence.go`.

use std::any::Any;
use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// Go `SequenceOperator`: the operations expression code performs on a
/// sequence.
pub trait SequenceOperator: Any + Send + Sync {
    /// Go `SequenceOperator.GetSequenceID`.
    fn get_sequence_id(&self) -> i64;

    /// Go `SequenceOperator.GetSequenceNextVal`.
    fn get_sequence_next_val(&self) -> Result<i64, ExprOptError>;

    /// Go `SequenceOperator.SetSequenceVal`: returns the resulting value and
    /// whether `new_val` was already under the base.
    fn set_sequence_val(&self, new_val: i64) -> Result<(i64, bool), ExprOptError>;
}

/// Go `SequenceOperatorProvider`: `func(db, name string) (SequenceOperator, error)`.
pub struct SequenceOperatorProvider(
    #[allow(clippy::type_complexity)]
    Box<dyn Fn(&str, &str) -> Result<Arc<dyn SequenceOperator>, ExprOptError> + Send + Sync>,
);

impl SequenceOperatorProvider {
    /// Wraps a function as the provider.
    #[must_use]
    pub fn new(
        provide: impl Fn(&str, &str) -> Result<Arc<dyn SequenceOperator>, ExprOptError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        SequenceOperatorProvider(Box::new(provide))
    }

    /// Calls the provider, Go's `p(db, name)`.
    pub fn call(&self, db: &str, name: &str) -> Result<Arc<dyn SequenceOperator>, ExprOptError> {
        (self.0)(db, name)
    }
}

impl OptionalEvalPropProvider for SequenceOperatorProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::SequenceOperator.desc()
    }
}

/// Go `SequenceOperatorPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SequenceOperatorPropReader;

impl RequireOptionalEvalProps for SequenceOperatorPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::SequenceOperator.as_prop_key_set()
    }
}

impl SequenceOperatorPropReader {
    /// Go `SequenceOperatorPropReader.GetSequenceOperator`: the provider's own
    /// error is returned unchanged.
    pub fn get_sequence_operator(
        self,
        ctx: &dyn EvalPropContext,
        db: &str,
        name: &str,
    ) -> Result<Arc<dyn SequenceOperator>, ExprOptError> {
        let provider = get_prop_provider::<SequenceOperatorProvider>(
            ctx,
            OptionalEvalPropKey::SequenceOperator,
        )?;
        provider.call(db, name)
    }
}
