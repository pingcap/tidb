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


//! Go `pkg/extension`: the registry of extensions (`registry.go`), each
//! one's manifest (`manifest.go`) and the set up extensions
//! (`extensions.go`); a custom SQL function's definition (`function.go`),
//! which `pkg/expression` registers as an `extensionFuncClass`.

mod session;
pub use session::{
    ConnEventInfo, ConnEventTp, SessionExtensions, SessionHandler, SessionHandlerFactory,
    StmtEventInfo, StmtEventTp,
};
mod auth;
pub use auth::{
    AuthConn, AuthPlugin, AuthenticateRequest, VerifyDynamicPrivRequest, VerifyStaticPrivRequest,
};

mod extensions;
mod function;
mod manifest;
mod registry;
mod util;

pub use extensions::Extensions;
pub use function::{EvalIntFunc, EvalStringFunc, FunctionContext, FunctionDef, RequireDynamicPrivileges};
pub use manifest::{
    with_close, with_custom_access_check, with_custom_auth_plugins, with_custom_dyn_privs,
    with_custom_functions, with_custom_sys_variables, with_session_handler_factory,
    AccessCheckFunc, ExtensionOption, Manifest, REGISTER_DYNAMIC_PRIVILEGE,
    REGISTER_EXTENSION_FUNC, REMOVE_DYNAMIC_PRIVILEGE, REMOVE_EXTENSION_FUNC,
};
pub use registry::{get_extensions, register, register_factory, reset, setup};

mod bootstrap;
pub use bootstrap::{Bootstrap, BootstrapContext, SessionPool, SessionResource};
pub use manifest::{with_bootstrap, with_bootstrap_sql};
