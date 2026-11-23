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

//! Go runs `planner/core`'s `init` (`core_init.go`: `expression.BuildSimpleExpr
//! = buildSimpleExpr`, ...) in every binary that links the package, before
//! `main` or the tests. The server calls [`tidb_planner::init`] itself; a test
//! binary whose tests reach a lower crate's simple-expression build (a
//! partitioned or generated-column table, a TTL or check-constraint
//! expression) names [`link!`] once at its crate root, as Go's
//! `import _ ".../planner/core"`, and the platform loader runs the init.

/// The load-time initializer, in the section the platform loader runs before
/// `main`.
#[used]
#[cfg_attr(target_vendor = "apple", link_section = "__DATA,__mod_init_func")]
#[cfg_attr(not(target_vendor = "apple"), link_section = ".init_array")]
static INIT_AT_LOAD: extern "C" fn() = init_at_load;

extern "C" fn init_at_load() {
    tidb_planner::init();
}

/// What [`link!`] references, so the linker keeps the object that holds the
/// initializer.
pub static LINKED: &(extern "C" fn()) = &INIT_AT_LOAD;

/// Links this crate's load-time `init` into the binary whose crate root names
/// it (Go's blank import of `planner/core`).
#[macro_export]
macro_rules! link {
    () => {
        #[used]
        static TIDB_PLANNER_INIT_LINKED: &&(extern "C" fn()) = &$crate::LINKED;
    };
}
