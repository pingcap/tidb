use crate::{
    set::restore_set_charset,
    util::{back_quote, escape_string_literal},
    BeginStmt, CharsetSetKind, PrepareSource, SetResourceGroupStmt, SetSessionStatesStmt, SetStmt,
    SetUserVarStmt, UserSpec,
};

/// Session-scoped statements, grouped behind [`crate::Stmt::Session`].
#[derive(Debug, Clone, PartialEq)]
pub enum SessionStmt {
    /// `USE dbname`.
    Use(String),
    /// A generic system-variable `SET` statement.
    Set(Box<SetStmt>),
    /// An ordered user-variable `SET` statement.
    SetUserVar(Box<SetUserVarStmt>),
    /// A `SET NAMES`, `SET CHARSET`, or `SET [CHARACTER|CHAR] SET` command.
    SetCharset {
        /// Whether this changes the connection names or character-set group.
        kind: CharsetSetKind,
        /// The canonical charset name, or `None` for `DEFAULT`.
        charset: Option<String>,
        /// A `SET NAMES` collation, if one survives canonical restore.
        collation: Option<String>,
    },
    /// `SET PASSWORD [FOR user] = {'password' | PASSWORD('password')}
    /// [RETAIN CURRENT PASSWORD]`.
    ///
    /// This remains distinct from [`Self::Set`]: TiDB's parser preserves the
    /// target account and dual-password request in `ast.SetPwdStmt`, neither
    /// of which is a system-variable assignment.
    SetPassword(Box<SetPasswordStmt>),
    /// `SET ROLE {DEFAULT | NONE | ALL [EXCEPT role, ...] | role, ...}`.
    SetRole(Box<SetRoleStmt>),
    /// `SET DEFAULT ROLE {NONE | ALL | role, ...} TO user, ...`.
    SetDefaultRole(Box<SetDefaultRoleStmt>),
    /// `SET RESOURCE GROUP name`.
    SetResourceGroup(Box<SetResourceGroupStmt>),
    /// `SET SESSION_STATES 'serialized state'`.
    SetSessionStates(Box<SetSessionStatesStmt>),
    /// `PREPARE name FROM {'sql' | @var}`.
    Prepare {
        /// The statement name (restored back-quoted).
        name: String,
        /// The prepared SQL source — a string literal or user variable.
        source: PrepareSource,
    },
    /// `EXECUTE name [USING @v, ...]`.
    Execute {
        /// The prepared statement's name.
        name: String,
        /// User variables listed by `USING`, without `@`.
        using: Vec<String>,
    },
    /// `DEALLOCATE PREPARE name`.
    Deallocate(String),
    /// `BEGIN [OPTIMISTIC|PESSIMISTIC]` or `START TRANSACTION`.
    Begin(Box<BeginStmt>),
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// `SAVEPOINT name`.
    Savepoint(Box<String>),
    /// `ROLLBACK TO [SAVEPOINT] name`.
    RollbackToSavepoint(Box<String>),
    /// `RELEASE SAVEPOINT name`.
    ReleaseSavepoint(Box<String>),
}

impl SessionStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Use(db) => {
                out.push_str("USE ");
                out.push_str(&back_quote(db));
            }
            Self::Set(set) => set.restore_into(out),
            Self::SetUserVar(set) => set.restore_into(out),
            Self::SetCharset {
                kind,
                charset,
                collation,
            } => restore_set_charset(out, *kind, charset, collation),
            Self::SetPassword(set_password) => set_password.restore_into(out),
            Self::SetRole(set_role) => set_role.restore_into(out),
            Self::SetDefaultRole(set_default_role) => set_default_role.restore_into(out),
            Self::SetResourceGroup(set_resource_group) => set_resource_group.restore_into(out),
            Self::SetSessionStates(set_session_states) => set_session_states.restore_into(out),
            Self::Prepare { name, source } => {
                out.push_str("PREPARE ");
                out.push_str(&back_quote(name));
                out.push_str(" FROM ");
                match source {
                    PrepareSource::Sql(sql) => {
                        out.push('\'');
                        out.push_str(&escape_string_literal(sql));
                        out.push('\'');
                    }
                    PrepareSource::Var(name) => {
                        out.push('@');
                        out.push_str(&back_quote(name));
                    }
                }
            }
            Self::Execute { name, using } => {
                out.push_str("EXECUTE ");
                out.push_str(&back_quote(name));
                if !using.is_empty() {
                    out.push_str(" USING ");
                    for (i, name) in using.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        out.push('@');
                        out.push_str(&back_quote(name));
                    }
                }
            }
            Self::Deallocate(name) => {
                out.push_str("DEALLOCATE PREPARE ");
                out.push_str(&back_quote(name));
            }
            Self::Begin(begin) => begin.restore_into(out),
            Self::Commit => out.push_str("COMMIT"),
            Self::Rollback => out.push_str("ROLLBACK"),
            Self::Savepoint(name) => {
                out.push_str("SAVEPOINT ");
                out.push_str(name);
            }
            Self::RollbackToSavepoint(name) => {
                out.push_str("ROLLBACK TO ");
                out.push_str(name);
            }
            Self::ReleaseSavepoint(name) => {
                out.push_str("RELEASE SAVEPOINT ");
                out.push_str(name);
            }
        }
    }
}

/// One Go `auth.RoleIdentity` in a role-management statement.
///
/// CREATE ROLE constructs this through the strict `Rolename` grammar and
/// therefore always supplies `%` for an omitted host. SET ROLE instead uses
/// `parseUserAsRole`: projecting `CURRENT_USER` produces an empty role and an
/// empty host, whose Go restore is a single empty quoted name with no `@`.
#[derive(Debug, Clone, PartialEq)]
pub struct RoleSpec {
    /// The decoded role name.
    pub role: String,
    /// The decoded host, `%` when it was omitted.
    pub host: String,
}

impl RoleSpec {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(&back_quote(&self.role));
        if !self.host.is_empty() {
            out.push('@');
            out.push_str(&back_quote(&self.host));
        }
    }
}

/// The mutually exclusive selection modes accepted by `SET ROLE`.
///
/// Keeping the role lists inside their applicable variants makes invalid AST
/// states (for example `SET ROLE ALL` with an arbitrary role list) impossible
/// to construct.
#[derive(Debug, Clone, PartialEq)]
pub enum SetRoleSelection {
    /// Activate the account's default roles.
    Default,
    /// Activate no roles.
    None,
    /// Activate all granted roles.
    All,
    /// Activate every granted role except these roles.
    AllExcept(Vec<RoleSpec>),
    /// Activate exactly these roles.
    Roles(Vec<RoleSpec>),
}

/// The complete parser-visible payload of `SET ROLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct SetRoleStmt {
    /// The role-selection request.
    pub selection: SetRoleSelection,
}

impl SetRoleStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("SET ROLE");
        match &self.selection {
            SetRoleSelection::Default => out.push_str(" DEFAULT"),
            SetRoleSelection::None => out.push_str(" NONE"),
            SetRoleSelection::All => out.push_str(" ALL"),
            SetRoleSelection::AllExcept(roles) => {
                out.push_str(" ALL EXCEPT");
                restore_roles(out, roles);
            }
            SetRoleSelection::Roles(roles) => restore_roles(out, roles),
        }
    }
}

/// The mutually exclusive assignment modes accepted by `SET DEFAULT ROLE`.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultRoleSelection {
    /// Clear every default role.
    None,
    /// Make every granted role a default role.
    All,
    /// Make exactly these roles default roles.
    Roles(Vec<RoleSpec>),
}

/// The complete parser-visible payload of `SET DEFAULT ROLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct SetDefaultRoleStmt {
    /// The requested default-role selection.
    pub selection: DefaultRoleSelection,
    /// The accounts whose defaults change.
    pub users: Vec<UserSpec>,
}

impl SetDefaultRoleStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("SET DEFAULT ROLE");
        match &self.selection {
            DefaultRoleSelection::None => out.push_str(" NONE"),
            DefaultRoleSelection::All => out.push_str(" ALL"),
            DefaultRoleSelection::Roles(roles) => restore_roles(out, roles),
        }
        out.push_str(" TO");
        for (index, user) in self.users.iter().enumerate() {
            out.push(' ');
            user.restore_into(out);
            if index + 1 != self.users.len() {
                out.push(',');
            }
        }
    }
}

fn restore_roles(out: &mut String, roles: &[RoleSpec]) {
    for (index, role) in roles.iter().enumerate() {
        out.push(' ');
        role.restore_into(out);
        if index + 1 != roles.len() {
            out.push(',');
        }
    }
}

/// The complete parser-visible payload of `SET PASSWORD`.
///
/// `user: None` is TiDB's current-account form (`SET PASSWORD = ...`);
/// [`Some`] records the explicit `FOR` target, including `CURRENT_USER()`.
/// The executor deliberately rejects this payload until it has an
/// authentication/user catalog rather than pretending that a password update
/// was a session-variable change.
#[derive(Debug, Clone, PartialEq)]
pub struct SetPasswordStmt {
    /// The explicit target after `FOR`, if supplied.
    pub user: Option<UserSpec>,
    /// Decoded password text. Restore always uses a normal quoted literal.
    pub password: String,
    /// MySQL/TiDB dual-password request.
    pub retain_current_password: bool,
}

impl SetPasswordStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("SET PASSWORD");
        if let Some(user) = &self.user {
            out.push_str(" FOR ");
            user.restore_into(out);
        }
        out.push_str("='");
        out.push_str(&escape_string_literal(&self.password));
        out.push('\'');
        if self.retain_current_password {
            out.push_str(" RETAIN CURRENT PASSWORD");
        }
    }
}
