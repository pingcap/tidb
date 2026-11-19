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

//! Go `sysvar.go`'s `authentication_ldap_*` `SetGlobal` closures: each
//! configures `ldap.LDAPSASLAuthImpl` or `ldap.LDAPSimpleAuthImpl`.

use tidb_privilege_ldap::{LDAP_SASL_AUTH_IMPL, LDAP_SIMPLE_AUTH_IMPL};

use crate::error::VarError;
use crate::vars::SessionVars;

/// Go `TiDBOptOn`.
fn tidb_opt_on(value: &str) -> bool {
    value.eq_ignore_ascii_case("ON") || value == "1"
}

/// Go `strconv.ParseInt(s, 10, 64)` of a validated integer.
fn parse_int(value: &str) -> Result<i64, VarError> {
    value
        .parse::<i64>()
        .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))
}

macro_rules! ldap_set_global {
    ($($name:ident => |$value:ident| $body:expr;)*) => {
        $(
            pub(super) fn $name(_vars: &mut SessionVars, $value: &str) -> Result<(), VarError> {
                $body
            }
        )*
    };
}

ldap_set_global! {
    sasl_auth_method_name => |s| {
        LDAP_SASL_AUTH_IMPL.set_sasl_auth_method(s);
        Ok(())
    };
    sasl_server_host => |s| {
        LDAP_SASL_AUTH_IMPL.set_ldap_server_host(s);
        Ok(())
    };
    sasl_server_port => |s| {
        LDAP_SASL_AUTH_IMPL.set_ldap_server_port(parse_int(s)?);
        Ok(())
    };
    sasl_tls => |s| {
        LDAP_SASL_AUTH_IMPL.set_enable_tls(tidb_opt_on(s));
        Ok(())
    };
    sasl_ca_path => |s| LDAP_SASL_AUTH_IMPL.set_ca_path(s).map_err(tidb_terror::MysqlError::unknown);
    sasl_user_search_attr => |s| {
        LDAP_SASL_AUTH_IMPL.set_search_attr(s);
        Ok(())
    };
    sasl_bind_base_dn => |s| {
        LDAP_SASL_AUTH_IMPL.set_bind_base_dn(s);
        Ok(())
    };
    sasl_bind_root_dn => |s| {
        LDAP_SASL_AUTH_IMPL.set_bind_root_dn(s);
        Ok(())
    };
    sasl_bind_root_pwd => |s| {
        LDAP_SASL_AUTH_IMPL.set_bind_root_pw(s);
        Ok(())
    };
    sasl_init_pool_size => |s| {
        LDAP_SASL_AUTH_IMPL.set_init_capacity(parse_int(s)?);
        Ok(())
    };
    sasl_max_pool_size => |s| {
        LDAP_SASL_AUTH_IMPL.set_max_capacity(parse_int(s)?);
        Ok(())
    };
    // Only "SIMPLE" is supported.
    simple_auth_method_name => |s| {
        let s = s.to_uppercase();
        if s != "SIMPLE" {
            return Err(tidb_terror::MysqlError::unknown(format!("auth method {s} is not supported")));
        }
        Ok(())
    };
    simple_server_host => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_ldap_server_host(s);
        Ok(())
    };
    simple_server_port => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_ldap_server_port(parse_int(s)?);
        Ok(())
    };
    simple_tls => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_enable_tls(tidb_opt_on(s));
        Ok(())
    };
    simple_ca_path => |s| LDAP_SIMPLE_AUTH_IMPL.set_ca_path(s).map_err(tidb_terror::MysqlError::unknown);
    simple_user_search_attr => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_search_attr(s);
        Ok(())
    };
    simple_bind_base_dn => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_bind_base_dn(s);
        Ok(())
    };
    simple_bind_root_dn => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_bind_root_dn(s);
        Ok(())
    };
    simple_bind_root_pwd => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_bind_root_pw(s);
        Ok(())
    };
    simple_init_pool_size => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_init_capacity(parse_int(s)?);
        Ok(())
    };
    simple_max_pool_size => |s| {
        LDAP_SIMPLE_AUTH_IMPL.set_max_capacity(parse_int(s)?);
        Ok(())
    };
}

/// Go `vardef.MaskPwd`.
const MASK_PWD: &str = "******";

/// Go `BoolToOnOff`.
fn bool_to_on_off(value: bool) -> String {
    if value { "ON" } else { "OFF" }.to_owned()
}

macro_rules! ldap_get_global {
    ($($name:ident => $body:expr;)*) => {
        $(
            pub(super) fn $name() -> String {
                $body
            }
        )*
    };
}

ldap_get_global! {
    get_sasl_auth_method_name => LDAP_SASL_AUTH_IMPL.get_sasl_auth_method();
    get_sasl_server_host => LDAP_SASL_AUTH_IMPL.get_ldap_server_host();
    get_sasl_server_port => LDAP_SASL_AUTH_IMPL.get_ldap_server_port().to_string();
    get_sasl_tls => bool_to_on_off(LDAP_SASL_AUTH_IMPL.get_enable_tls());
    get_sasl_ca_path => LDAP_SASL_AUTH_IMPL.get_ca_path();
    get_sasl_user_search_attr => LDAP_SASL_AUTH_IMPL.get_search_attr();
    get_sasl_bind_base_dn => LDAP_SASL_AUTH_IMPL.get_bind_base_dn();
    get_sasl_bind_root_dn => LDAP_SASL_AUTH_IMPL.get_bind_root_dn();
    get_sasl_bind_root_pwd => if LDAP_SASL_AUTH_IMPL.get_bind_root_pw().is_empty() {
        String::new()
    } else {
        MASK_PWD.to_owned()
    };
    get_sasl_init_pool_size => LDAP_SASL_AUTH_IMPL.get_init_capacity().to_string();
    get_sasl_max_pool_size => LDAP_SASL_AUTH_IMPL.get_max_capacity().to_string();
    get_simple_server_host => LDAP_SIMPLE_AUTH_IMPL.get_ldap_server_host();
    get_simple_server_port => LDAP_SIMPLE_AUTH_IMPL.get_ldap_server_port().to_string();
    get_simple_tls => bool_to_on_off(LDAP_SIMPLE_AUTH_IMPL.get_enable_tls());
    get_simple_ca_path => LDAP_SIMPLE_AUTH_IMPL.get_ca_path();
    get_simple_user_search_attr => LDAP_SIMPLE_AUTH_IMPL.get_search_attr();
    get_simple_bind_base_dn => LDAP_SIMPLE_AUTH_IMPL.get_bind_base_dn();
    get_simple_bind_root_dn => LDAP_SIMPLE_AUTH_IMPL.get_bind_root_dn();
    get_simple_bind_root_pwd => if LDAP_SIMPLE_AUTH_IMPL.get_bind_root_pw().is_empty() {
        String::new()
    } else {
        MASK_PWD.to_owned()
    };
    get_simple_init_pool_size => LDAP_SIMPLE_AUTH_IMPL.get_init_capacity().to_string();
    get_simple_max_pool_size => LDAP_SIMPLE_AUTH_IMPL.get_max_capacity().to_string();
}
