// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use std::sync::LazyLock;

use tidb_error::server_errors::*;
use tidb_error::terror::{TerrorClass, TerrorError};

#[test]
fn every_server_error_preserves_class_code_template_and_sqlstate() {
    let cases: &[(&LazyLock<TerrorError>, u16, &str, &str)] = &[
        (&ERR_INVALID_TYPE, 8057, "invalid type", "HY000"),
        (&ERR_INVALID_SEQUENCE, 8052, "invalid sequence", "HY000"),
        (
            &ERR_NOT_ALLOWED_COMMAND,
            1148,
            "The used command is not allowed with this MySQL version",
            "42000",
        ),
        (
            &ERR_ACCESS_DENIED,
            1045,
            "Access denied for user '%-.48s'@'%-.255s' (using password: %s)",
            "28000",
        ),
        (
            &ERR_ACCESS_DENIED_NO_PASSWORD,
            1698,
            "Access denied for user '%-.48s'@'%-.255s'",
            "28000",
        ),
        (&ERR_CON_COUNT, 1040, "Too many connections", "08004"),
        (
            &ERR_TOO_MANY_USER_CONNECTIONS,
            1203,
            "User %-.64s has exceeded the 'max_user_connections' resource",
            "42000",
        ),
        (
            &ERR_SECURE_TRANSPORT_REQUIRED,
            3159,
            "Connections using insecure transport are prohibited while --require_secure_transport=ON.",
            "HY000",
        ),
        (
            &ERR_USER_PREFIX_MISMATCH,
            20003,
            "User name prefix does not match the assigned keyspace.",
            "HY000",
        ),
        (
            &ERR_MULTI_STATEMENT_DISABLED,
            8130,
            "client has multi-statement capability disabled. Run SET GLOBAL tidb_multi_statement_mode='ON' after you understand the security risk",
            "HY000",
        ),
        (
            &ERR_NEW_ABORTING_CONNECTION,
            1184,
            "Aborted connection %d to db: '%-.192s' user: '%-.48s' host: '%-.255s' (%-.64s)",
            "08S01",
        ),
        (
            &ERR_NOT_SUPPORTED_AUTH_MODE,
            1251,
            "Client does not support authentication protocol requested by server; consider upgrading MySQL client",
            "08004",
        ),
        (
            &ERR_NET_PACKET_TOO_LARGE,
            1153,
            "Got a packet bigger than 'max_allowed_packet' bytes",
            "08S01",
        ),
        (
            &ERR_MUST_CHANGE_PASSWORD,
            1820,
            "You must reset your password using ALTER USER statement before executing this statement",
            "HY000",
        ),
        (
            &ERR_SERVER_SHUTDOWN,
            1053,
            "Server shutdown in progress",
            "08S01",
        ),
    ];

    for (error, code, message, state) in cases {
        assert_eq!(error.class(), TerrorClass::Server);
        assert_eq!(
            error.code().value(),
            isize::try_from(*code).expect("u16 error code must fit isize")
        );
        assert_eq!(error.rfc_code(), format!("server:{code}"));
        assert_eq!(error.message(), *message);
        let sql = error.to_sql_error();
        assert_eq!(sql.code, *code);
        assert_eq!(sql.message, *message);
        assert_eq!(sql.state, *state);
    }
}
