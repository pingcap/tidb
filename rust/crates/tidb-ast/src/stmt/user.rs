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

//! User/security DDL payloads with source-fixed option ordering.

use crate::util::{back_quote, escape_string_literal};

/// A user/role account identity or the `CURRENT_USER` pseudo-user.
#[derive(Debug, Clone, PartialEq)]
pub struct UserSpec {
    /// Whether this is the `CURRENT_USER` pseudo-user.
    pub current_user: bool,
    /// The decoded username; unused for `CURRENT_USER`.
    pub user: String,
    /// The decoded host, defaulting to `%`; unused for `CURRENT_USER`.
    pub host: String,
}

impl UserSpec {
    pub(crate) fn restore_into(&self, out: &mut String) {
        if self.current_user {
            out.push_str("CURRENT_USER");
        } else {
            out.push_str(&back_quote(&self.user));
            out.push('@');
            out.push_str(&back_quote(&self.host));
        }
    }
}

/// Authentication clause attached to one user account specification.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateUserAuth {
    /// `IDENTIFIED BY 'password'`.
    By(String),
    /// `IDENTIFIED WITH 'plugin' [BY 'password' | AS 'hash']`.
    With {
        /// Authentication plugin name.
        plugin: String,
        /// Optional plugin credential payload and mode.
        credential: Option<CreateUserCredential>,
    },
}

impl CreateUserAuth {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::By(password) => {
                out.push_str("IDENTIFIED BY '");
                out.push_str(&escape_string_literal(password));
                out.push('\'');
            }
            Self::With { plugin, credential } => {
                out.push_str("IDENTIFIED WITH '");
                out.push_str(&escape_string_literal(plugin));
                out.push('\'');
                if let Some(credential) = credential {
                    credential.restore_into(out);
                }
            }
        }
    }
}

/// Credential payload for `IDENTIFIED WITH` authentication.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateUserCredential {
    /// `BY 'password'` uses an authentication string.
    By(String),
    /// `AS 'hash'` uses a precomputed hash string.
    As(String),
}

impl CreateUserCredential {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::By(_) => out.push_str(" BY '"),
            Self::As(_) => out.push_str(" AS '"),
        }
        let value = match self {
            Self::By(value) | Self::As(value) => value,
        };
        out.push_str(&escape_string_literal(value));
        out.push('\'');
    }
}

/// MySQL 8.0 dual-password action attached to one ALTER USER account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterUserDualPassword {
    /// `RETAIN CURRENT PASSWORD`, valid only after BY-form authentication.
    RetainCurrent,
    /// `DISCARD OLD PASSWORD`, valid only without authentication.
    DiscardOld,
}

impl AlterUserDualPassword {
    fn restore_into(self, out: &mut String) {
        out.push_str(match self {
            Self::RetainCurrent => "RETAIN CURRENT PASSWORD",
            Self::DiscardOld => "DISCARD OLD PASSWORD",
        });
    }
}

/// One account specification shared by CREATE USER and ALTER USER.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateUserSpec {
    /// Account identity.
    pub user: UserSpec,
    /// Optional authentication clause.
    pub auth: Option<CreateUserAuth>,
    /// ALTER USER-only dual-password action.
    pub dual_password: Option<AlterUserDualPassword>,
}

impl CreateUserSpec {
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.user.restore_into(out);
        if let Some(auth) = &self.auth {
            out.push(' ');
            auth.restore_into(out);
        }
        if let Some(dual_password) = self.dual_password {
            out.push(' ');
            dual_password.restore_into(out);
        }
    }
}

/// Statement-level password-expiration policy.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterUserPasswordExpire {
    /// `PASSWORD EXPIRE`.
    Expire,
    /// `PASSWORD EXPIRE DEFAULT`.
    Default,
    /// `PASSWORD EXPIRE NEVER`.
    Never,
    /// `PASSWORD EXPIRE INTERVAL n DAY`.
    Interval(i64),
}

impl AlterUserPasswordExpire {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Expire => out.push_str("PASSWORD EXPIRE"),
            Self::Default => out.push_str("PASSWORD EXPIRE DEFAULT"),
            Self::Never => out.push_str("PASSWORD EXPIRE NEVER"),
            Self::Interval(days) => {
                out.push_str("PASSWORD EXPIRE INTERVAL ");
                out.push_str(&days.to_string());
                out.push_str(" DAY");
            }
        }
    }
}

/// Statement-global password or account-lock option on CREATE/ALTER USER.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateUserPasswordOption {
    /// `PASSWORD EXPIRE` and its optional policy.
    Expire(AlterUserPasswordExpire),
    /// `PASSWORD HISTORY n`.
    History(i64),
    /// `PASSWORD HISTORY DEFAULT`.
    HistoryDefault,
    /// `PASSWORD REUSE INTERVAL n DAY`.
    ReuseInterval(i64),
    /// `PASSWORD REUSE INTERVAL DEFAULT`.
    ReuseDefault,
    /// `PASSWORD REQUIRE CURRENT DEFAULT`.
    RequireCurrentDefault,
    /// `ACCOUNT LOCK`.
    AccountLock,
    /// `ACCOUNT UNLOCK`.
    AccountUnlock,
    /// `FAILED_LOGIN_ATTEMPTS n`.
    FailedLoginAttempts(i64),
    /// `PASSWORD_LOCK_TIME n`.
    PasswordLockTime(i64),
    /// `PASSWORD_LOCK_TIME UNBOUNDED`.
    PasswordLockTimeUnbounded,
}

impl CreateUserPasswordOption {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Expire(expire) => expire.restore_into(out),
            Self::History(count) => {
                out.push_str("PASSWORD HISTORY ");
                out.push_str(&count.to_string());
            }
            Self::HistoryDefault => out.push_str("PASSWORD HISTORY DEFAULT"),
            Self::ReuseInterval(days) => {
                out.push_str("PASSWORD REUSE INTERVAL ");
                out.push_str(&days.to_string());
                out.push_str(" DAY");
            }
            Self::ReuseDefault => out.push_str("PASSWORD REUSE INTERVAL DEFAULT"),
            Self::RequireCurrentDefault => out.push_str("PASSWORD REQUIRE CURRENT DEFAULT"),
            Self::AccountLock => out.push_str("ACCOUNT LOCK"),
            Self::AccountUnlock => out.push_str("ACCOUNT UNLOCK"),
            Self::FailedLoginAttempts(count) => {
                out.push_str("FAILED_LOGIN_ATTEMPTS ");
                out.push_str(&count.to_string());
            }
            Self::PasswordLockTime(count) => {
                out.push_str("PASSWORD_LOCK_TIME ");
                out.push_str(&count.to_string());
            }
            Self::PasswordLockTimeUnbounded => out.push_str("PASSWORD_LOCK_TIME UNBOUNDED"),
        }
    }
}

/// Single account COMMENT or ATTRIBUTE clause on CREATE/ALTER USER.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateUserCommentOrAttribute {
    /// `COMMENT 'text'`.
    Comment(String),
    /// `ATTRIBUTE 'json'`.
    Attribute(String),
}

impl CreateUserCommentOrAttribute {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Comment(_) => out.push_str("COMMENT "),
            Self::Attribute(_) => out.push_str("ATTRIBUTE "),
        }
        let value = match self {
            Self::Comment(value) | Self::Attribute(value) => value,
        };
        out.push('\'');
        out.push_str(&escape_string_literal(value));
        out.push('\'');
    }
}

/// One `REQUIRE` option shared by CREATE USER and ALTER USER.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterUserTlsOption {
    /// `NONE`.
    None,
    /// `SSL`.
    Ssl,
    /// `X509`.
    X509,
    /// `CIPHER 'value'`.
    Cipher(String),
    /// `ISSUER 'value'`.
    Issuer(String),
    /// `SUBJECT 'value'`.
    Subject(String),
    /// `SAN 'value'`.
    San(String),
    /// `TOKEN_ISSUER 'value'`.
    TokenIssuer(String),
}

impl AlterUserTlsOption {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::None => out.push_str("NONE"),
            Self::Ssl => out.push_str("SSL"),
            Self::X509 => out.push_str("X509"),
            Self::Cipher(value)
            | Self::Issuer(value)
            | Self::Subject(value)
            | Self::San(value)
            | Self::TokenIssuer(value) => {
                out.push_str(match self {
                    Self::Cipher(_) => "CIPHER",
                    Self::Issuer(_) => "ISSUER",
                    Self::Subject(_) => "SUBJECT",
                    Self::San(_) => "SAN",
                    Self::TokenIssuer(_) => "TOKEN_ISSUER",
                    Self::None | Self::Ssl | Self::X509 => unreachable!(),
                });
                out.push_str(" '");
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
        }
    }
}

/// Kind of one CREATE/ALTER USER `WITH MAX_*` connection limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterUserResourceKind {
    /// `MAX_QUERIES_PER_HOUR`.
    MaxQueriesPerHour,
    /// `MAX_UPDATES_PER_HOUR`.
    MaxUpdatesPerHour,
    /// `MAX_CONNECTIONS_PER_HOUR`.
    MaxConnectionsPerHour,
    /// `MAX_USER_CONNECTIONS`.
    MaxUserConnections,
}

impl AlterUserResourceKind {
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::MaxQueriesPerHour => "MAX_QUERIES_PER_HOUR",
            Self::MaxUpdatesPerHour => "MAX_UPDATES_PER_HOUR",
            Self::MaxConnectionsPerHour => "MAX_CONNECTIONS_PER_HOUR",
            Self::MaxUserConnections => "MAX_USER_CONNECTIONS",
        }
    }
}

/// One CREATE/ALTER USER `WITH MAX_* count` connection resource limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserResourceOption {
    /// Typed MAX_* resource kind.
    pub kind: AlterUserResourceKind,
    /// Source `int64` count.
    pub count: i64,
}

/// Complete Go-shaped ALTER USER statement payload.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterUserStmt {
    /// Missing-account guard.
    pub if_exists: bool,
    /// Named/current-user account specs.
    pub users: Vec<CreateUserSpec>,
    /// Special `USER()` auth string.
    pub user_function_auth: Option<String>,
    /// Special `USER()` dual-password action.
    pub user_function_dual_password: Option<AlterUserDualPassword>,
    /// Statement-global TLS options.
    pub tls_options: Vec<AlterUserTlsOption>,
    /// Statement-global connection resource limits.
    pub resource_options: Vec<AlterUserResourceOption>,
    /// Statement-global password/account policies.
    pub password_options: Vec<CreateUserPasswordOption>,
    /// Optional account annotation.
    pub comment_or_attribute: Option<CreateUserCommentOrAttribute>,
    /// Optional resource-group assignment.
    pub resource_group: Option<String>,
}

impl AlterUserStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER USER ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        if let Some(password) = &self.user_function_auth {
            out.push_str("USER() IDENTIFIED BY '");
            out.push_str(&crate::util::escape_string_literal(password));
            out.push('\'');
            if let Some(option) = self.user_function_dual_password {
                out.push(' ');
                option.restore_into(out);
            }
        } else if let Some(option) = self.user_function_dual_password {
            out.push_str("USER() ");
            option.restore_into(out);
        } else {
            for (index, user) in self.users.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                user.restore_into(out);
            }
        }
        if !self.tls_options.is_empty() {
            out.push_str(" REQUIRE ");
            for (index, option) in self.tls_options.iter().enumerate() {
                if index > 0 {
                    out.push_str(" AND ");
                }
                option.restore_into(out);
            }
        }
        if !self.resource_options.is_empty() {
            out.push_str(" WITH");
            for option in &self.resource_options {
                out.push(' ');
                out.push_str(option.kind.as_sql());
                out.push(' ');
                out.push_str(&option.count.to_string());
            }
        }
        for option in &self.password_options {
            out.push(' ');
            option.restore_into(out);
        }
        if let Some(option) = &self.comment_or_attribute {
            out.push(' ');
            option.restore_into(out);
        }
        if let Some(resource_group) = &self.resource_group {
            out.push_str(" RESOURCE GROUP ");
            out.push_str(&crate::util::back_quote(resource_group));
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for UserSpec {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            current_user,
            user,
            host,
        } = self;
        let _ = current_user;
        let _ = user;
        let _ = host;
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateUserAuth {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::By(field_0) => {
                let _ = field_0;
            }
            Self::With { plugin, credential } => {
                if let Some(value) = credential.as_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = plugin;
                let _ = credential;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateUserCredential {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::By(field_0) => {
                let _ = field_0;
            }
            Self::As(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterUserDualPassword {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::RetainCurrent => {}
            Self::DiscardOld => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateUserSpec {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            user,
            auth,
            dual_password,
        } = self;
        if !crate::Visitable::accept(user, visitor) {
            return false;
        }
        if let Some(value) = auth.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = dual_password.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = user;
        let _ = auth;
        let _ = dual_password;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterUserPasswordExpire {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Expire => {}
            Self::Default => {}
            Self::Never => {}
            Self::Interval(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateUserPasswordOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Expire(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::History(field_0) => {
                let _ = field_0;
            }
            Self::HistoryDefault => {}
            Self::ReuseInterval(field_0) => {
                let _ = field_0;
            }
            Self::ReuseDefault => {}
            Self::RequireCurrentDefault => {}
            Self::AccountLock => {}
            Self::AccountUnlock => {}
            Self::FailedLoginAttempts(field_0) => {
                let _ = field_0;
            }
            Self::PasswordLockTime(field_0) => {
                let _ = field_0;
            }
            Self::PasswordLockTimeUnbounded => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateUserCommentOrAttribute {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Comment(field_0) => {
                let _ = field_0;
            }
            Self::Attribute(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterUserTlsOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Ssl => {}
            Self::X509 => {}
            Self::Cipher(field_0) => {
                let _ = field_0;
            }
            Self::Issuer(field_0) => {
                let _ = field_0;
            }
            Self::Subject(field_0) => {
                let _ = field_0;
            }
            Self::San(field_0) => {
                let _ = field_0;
            }
            Self::TokenIssuer(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterUserResourceKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::MaxQueriesPerHour => {}
            Self::MaxUpdatesPerHour => {}
            Self::MaxConnectionsPerHour => {}
            Self::MaxUserConnections => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterUserResourceOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { kind, count } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        let _ = kind;
        let _ = count;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterUserStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_exists,
            users,
            user_function_auth,
            user_function_dual_password,
            tls_options,
            resource_options,
            password_options,
            comment_or_attribute,
            resource_group,
        } = self;
        for value in users.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = user_function_dual_password.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in tls_options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in resource_options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in password_options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = comment_or_attribute.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = if_exists;
        let _ = users;
        let _ = user_function_auth;
        let _ = user_function_dual_password;
        let _ = tls_options;
        let _ = resource_options;
        let _ = password_options;
        let _ = comment_or_attribute;
        let _ = resource_group;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
