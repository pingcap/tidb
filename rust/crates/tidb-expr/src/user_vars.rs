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

//! SEED of Go `pkg/sessionctx/variable`, covering `session.go`'s
//! `UserVarsReader` interface and its `UserVars` implementation — the slice
//! `pkg/expression/exprctx`'s `EvalContext` reads user-defined variables
//! through.
//!
//! It lives in this crate because the expression context is its Go consumer
//! and sits below the session tier here; the session's own user-variable
//! storage adopts this type as the single authority rather than keeping a
//! parallel map.
//!
//! Names are stored as given: Go's `SetUserVarVal`/getters do not fold case —
//! callers pass lowercased names — while `UnsetUserVar` lowercases its
//! argument itself. Both behaviors are reproduced exactly. Go guards the maps
//! with an `RWMutex` for cross-goroutine reads; the same sharing arrives here
//! through the interior lock, so clones of a handle still see one store.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[cfg(test)]
use tidb_datatype::FieldTypeCode;
use tidb_datatype::{Datum, FieldType};

/// Go `UserVarsReader`: read access to the session's user-defined variables.
pub trait UserVarsReader {
    /// Go `GetUserVarVal`.
    fn get_user_var_val(&self, name: &str) -> Option<Datum>;
    /// Go `GetUserVarType`.
    fn get_user_var_type(&self, name: &str) -> Option<FieldType>;
    /// Go `Clone`: an independent copy of the variables at this moment.
    fn clone_reader(&self) -> Box<dyn UserVarsReader + Send + Sync>;
}

#[derive(Debug, Default)]
struct UserVarsInner {
    /// Go `values`: the datum of each user variable.
    values: HashMap<String, Datum>,
    /// Go `types`: the field type, kept separately because it cannot be
    /// inferred before a value is set.
    types: HashMap<String, FieldType>,
}

/// Go `UserVars`: the session's user-defined variable store.
#[derive(Clone, Debug, Default)]
pub struct UserVars {
    inner: Arc<RwLock<UserVarsInner>>,
}

impl UserVars {
    /// Go `NewUserVars`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, UserVarsInner> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, UserVarsInner> {
        self.inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Go `SetUserVarVal`. The name is stored as given.
    pub fn set_user_var_val(&self, name: &str, value: Datum) {
        self.write().values.insert(name.to_owned(), value);
    }

    /// Go `UnsetUserVar`: removes both the value and the type. This is the
    /// one entry point that lowercases its argument itself.
    pub fn unset_user_var(&self, var_name: &str) {
        let var_name = var_name.to_lowercase();
        let mut inner = self.write();
        inner.values.remove(&var_name);
        inner.types.remove(&var_name);
    }

    /// Go `SetUserVarType`.
    pub fn set_user_var_type(&self, name: &str, field_type: FieldType) {
        self.write().types.insert(name.to_owned(), field_type);
    }

    /// Go `Clone`: a deep, independent copy.
    #[must_use]
    pub fn clone_vars(&self) -> Self {
        let inner = self.read();
        Self {
            inner: Arc::new(RwLock::new(UserVarsInner {
                values: inner.values.clone(),
                types: inner.types.clone(),
            })),
        }
    }
}

impl UserVarsReader for UserVars {
    fn get_user_var_val(&self, name: &str) -> Option<Datum> {
        self.read().values.get(name).cloned()
    }

    fn get_user_var_type(&self, name: &str) -> Option<FieldType> {
        self.read().types.get(name).cloned()
    }

    fn clone_reader(&self) -> Box<dyn UserVarsReader + Send + Sync> {
        Box::new(self.clone_vars())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Values and types are independent maps; a value without a type and a
    // type without a value are both representable, as Go's comment demands.
    #[test]
    fn values_and_types_are_independent() {
        let vars = UserVars::new();
        assert!(vars.get_user_var_val("x").is_none());
        assert!(vars.get_user_var_type("x").is_none());

        vars.set_user_var_val("x", Datum::Int(42));
        assert_eq!(vars.get_user_var_val("x"), Some(Datum::Int(42)));
        assert!(vars.get_user_var_type("x").is_none());

        vars.set_user_var_type("y", FieldType::parser(FieldTypeCode::Long));
        assert!(vars.get_user_var_val("y").is_none());
        assert!(vars.get_user_var_type("y").is_some());
    }

    // Only unset lowercases; set and get use the name as given.
    #[test]
    fn only_unset_folds_case() {
        let vars = UserVars::new();
        vars.set_user_var_val("x", Datum::Int(1));
        vars.set_user_var_type("x", FieldType::parser(FieldTypeCode::Long));

        // A get with different case misses: the store did not fold.
        assert!(vars.get_user_var_val("X").is_none());

        // Unset folds its own argument, so `X` removes `x`.
        vars.unset_user_var("X");
        assert!(vars.get_user_var_val("x").is_none());
        assert!(vars.get_user_var_type("x").is_none());
    }

    // Go `Clone`: the copy is independent of the original.
    #[test]
    fn clones_are_independent() {
        let vars = UserVars::new();
        vars.set_user_var_val("x", Datum::Int(1));

        let cloned = vars.clone_vars();
        assert_eq!(cloned.get_user_var_val("x"), Some(Datum::Int(1)));

        vars.set_user_var_val("x", Datum::Int(2));
        vars.set_user_var_val("y", Datum::Int(3));
        assert_eq!(cloned.get_user_var_val("x"), Some(Datum::Int(1)));
        assert!(cloned.get_user_var_val("y").is_none());

        // And through the reader trait.
        let reader = vars.clone_reader();
        assert_eq!(reader.get_user_var_val("y"), Some(Datum::Int(3)));
    }

    // Handles of one store share it, like Go passing *UserVars around.
    #[test]
    fn handles_share_one_store() {
        let vars = UserVars::new();
        let handle = vars.clone();
        handle.set_user_var_val("shared", Datum::Int(7));
        assert_eq!(vars.get_user_var_val("shared"), Some(Datum::Int(7)));
    }
}
