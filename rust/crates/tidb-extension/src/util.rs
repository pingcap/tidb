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

//! Go `pkg/extension/util.go`.

/// A clear function a setup step leaves to undo it.
pub(crate) type ClearFunc = Box<dyn FnOnce() + Send>;

/// Go `clearFuncBuilder`: the clear functions of the steps done so far.
#[derive(Default)]
pub(crate) struct ClearFuncBuilder {
    clears: Vec<ClearFunc>,
}

impl ClearFuncBuilder {
    /// Go `DoWithCollectClear`: runs `f` and keeps the clear function it
    /// returns.
    pub(crate) fn do_with_collect_clear<E>(
        &mut self,
        f: impl FnOnce() -> Result<Option<ClearFunc>, E>,
    ) -> Result<(), E> {
        if let Some(clear) = f()? {
            self.clears.push(clear);
        }
        Ok(())
    }

    /// Go `Build`: one function running every collected clear, in order.
    pub(crate) fn build(self) -> ClearFunc {
        Box::new(move || {
            for clear in self.clears {
                clear();
            }
        })
    }
}
