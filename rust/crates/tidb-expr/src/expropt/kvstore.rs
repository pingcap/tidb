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

//! Go `pkg/expression/expropt/kvstore.go`.

use std::any::Any;
use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// boundary: Go `pkg/kv.Storage` — the cluster storage handle. The ported
/// client (`tidb-txnkv`) is an async tonic/tokio stack whose direction of
/// dependency runs the other way from this expression crate, and expropt only
/// stores the handle and hands it back, so an opaque marker trait suffices.
pub trait KvStorage: Any + Send + Sync {}

/// Go `KVStorePropProvider`: `func() kv.Storage`.
pub struct KvStorePropProvider(Box<dyn Fn() -> Arc<dyn KvStorage> + Send + Sync>);

impl KvStorePropProvider {
    /// Wraps a function as the provider.
    #[must_use]
    pub fn new(provide: impl Fn() -> Arc<dyn KvStorage> + Send + Sync + 'static) -> Self {
        KvStorePropProvider(Box::new(provide))
    }

    /// Calls the provider, Go's `p()`.
    #[must_use]
    pub fn call(&self) -> Arc<dyn KvStorage> {
        (self.0)()
    }
}

impl OptionalEvalPropProvider for KvStorePropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::KvStore.desc()
    }
}

/// Go `KVStorePropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KvStorePropReader;

impl RequireOptionalEvalProps for KvStorePropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::KvStore.as_prop_key_set()
    }
}

impl KvStorePropReader {
    /// Go `KVStorePropReader.GetKVStore`.
    pub fn get_kv_store(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<dyn KvStorage>, ExprOptError> {
        let provider = get_prop_provider::<KvStorePropProvider>(ctx, OptionalEvalPropKey::KvStore)?;
        Ok(provider.call())
    }
}
