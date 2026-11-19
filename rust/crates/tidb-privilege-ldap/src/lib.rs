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

//! Go `pkg/privilege/privileges/ldap`: authentication of
//! `authentication_ldap_simple` and `authentication_ldap_sasl` accounts
//! against an LDAP server, configured by the `authentication_ldap_*` system
//! variables' `SetGlobal` hooks.
//!
//! [`ldap_conn`] is the subset of `github.com/go-ldap/ldap/v3` (TiDB's
//! `YangKeao/ldap` fork) this package calls.

mod consts;
mod ldap_common;
pub mod ldap_conn;
mod sasl;
mod simple;

pub use consts::{
    SASL_AUTH_METHOD_GSSAPI, SASL_AUTH_METHOD_SCRAM_SHA1, SASL_AUTH_METHOD_SCRAM_SHA256,
};
pub use ldap_common::LdapAuthImpl;
pub use sasl::{LDAP_SASL_AUTH_IMPL, LdapSaslAuthImpl};
pub use simple::{LDAP_SIMPLE_AUTH_IMPL, LdapSimpleAuthImpl};
