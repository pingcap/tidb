// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! MySQL static privilege bit assignments, names, and scope sets.

#![allow(non_upper_case_globals)]

/// The SQL spelling of the synthetic all-privileges value.
pub const ALL_PRIVILEGE_LITERAL: &str = "ALL PRIVILEGES";

/// Stable privilege bit value used by privilege tables and verification.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct PrivilegeType(pub u64);

impl std::ops::BitOr for PrivilegeType {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}
impl std::ops::BitAnd for PrivilegeType {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}
impl std::ops::Shl<u32> for PrivilegeType {
    type Output = Self;
    fn shl(self, rhs: u32) -> Self::Output {
        Self(self.0 << rhs)
    }
}

macro_rules! privileges {
    ($($name:ident = $bit:expr;)+) => {$(
        #[doc = concat!("Source-compatible `", stringify!($name), "` privilege.")]
        pub const $name: PrivilegeType = PrivilegeType(1_u64 << $bit);
    )+};
}

privileges! {
    UsagePriv = 0; CreatePriv = 1; SelectPriv = 2; InsertPriv = 3;
    UpdatePriv = 4; DeletePriv = 5; ShowDBPriv = 6; SuperPriv = 7;
    CreateUserPriv = 8; TriggerPriv = 9; DropPriv = 10; ProcessPriv = 11;
    GrantPriv = 12; ReferencesPriv = 13; AlterPriv = 14; ExecutePriv = 15;
    IndexPriv = 16; CreateViewPriv = 17; ShowViewPriv = 18; CreateRolePriv = 19;
    DropRolePriv = 20; CreateTMPTablePriv = 21; LockTablesPriv = 22;
    CreateRoutinePriv = 23; AlterRoutinePriv = 24; EventPriv = 25;
    ShutdownPriv = 26; ReloadPriv = 27; FilePriv = 28; ConfigPriv = 29;
    CreateTablespacePriv = 30; ReplicationClientPriv = 31;
    ReplicationSlavePriv = 32; AllPriv = 33; ExtendedPriv = 34;
}

/// Mask with every real static privilege bit below `AllPriv` set.
pub const AllPrivMask: PrivilegeType = PrivilegeType(AllPriv.0 - 1);

/// Complete source privilege-to-SQL-name map.
pub const PRIVILEGE_NAMES: &[(PrivilegeType, &str)] = &[
    (CreatePriv, "Create"),
    (SelectPriv, "Select"),
    (InsertPriv, "Insert"),
    (UpdatePriv, "Update"),
    (DeletePriv, "Delete"),
    (ShowDBPriv, "Show Databases"),
    (SuperPriv, "Super"),
    (CreateUserPriv, "Create User"),
    (CreateTablespacePriv, "Create Tablespace"),
    (TriggerPriv, "Trigger"),
    (DropPriv, "Drop"),
    (ProcessPriv, "Process"),
    (GrantPriv, "Grant Option"),
    (ReferencesPriv, "References"),
    (AlterPriv, "Alter"),
    (ExecutePriv, "Execute"),
    (IndexPriv, "Index"),
    (CreateViewPriv, "Create View"),
    (ShowViewPriv, "Show View"),
    (CreateRolePriv, "Create Role"),
    (DropRolePriv, "Drop Role"),
    (CreateTMPTablePriv, "CREATE TEMPORARY TABLES"),
    (LockTablesPriv, "LOCK TABLES"),
    (CreateRoutinePriv, "CREATE ROUTINE"),
    (AlterRoutinePriv, "ALTER ROUTINE"),
    (EventPriv, "EVENT"),
    (ShutdownPriv, "SHUTDOWN"),
    (ReloadPriv, "RELOAD"),
    (FilePriv, "FILE"),
    (ConfigPriv, "CONFIG"),
    (UsagePriv, "USAGE"),
    (ReplicationClientPriv, "REPLICATION CLIENT"),
    (ReplicationSlavePriv, "REPLICATION SLAVE"),
    (AllPriv, ALL_PRIVILEGE_LITERAL),
];

/// Complete source privilege-to-SET-name map.
pub const PRIVILEGE_SET_NAMES: &[(PrivilegeType, &str)] = &[
    (CreatePriv, "Create"),
    (SelectPriv, "Select"),
    (InsertPriv, "Insert"),
    (UpdatePriv, "Update"),
    (DeletePriv, "Delete"),
    (DropPriv, "Drop"),
    (GrantPriv, "Grant"),
    (ReferencesPriv, "References"),
    (LockTablesPriv, "Lock Tables"),
    (CreateTMPTablePriv, "Create Temporary Tables"),
    (EventPriv, "Event"),
    (CreateRoutinePriv, "Create Routine"),
    (AlterRoutinePriv, "Alter Routine"),
    (AlterPriv, "Alter"),
    (ExecutePriv, "Execute"),
    (IndexPriv, "Index"),
    (CreateViewPriv, "Create View"),
    (ShowViewPriv, "Show View"),
    (CreateRolePriv, "Create Role"),
    (DropRolePriv, "Drop Role"),
    (ShutdownPriv, "Shutdown Role"),
    (TriggerPriv, "Trigger"),
];

/// Exact reverse SET map; intentionally excludes role and shutdown entries.
pub const SET_ENUM_PRIVILEGES: &[(&str, PrivilegeType)] = &[
    ("Create", CreatePriv),
    ("Select", SelectPriv),
    ("Insert", InsertPriv),
    ("Update", UpdatePriv),
    ("Delete", DeletePriv),
    ("Drop", DropPriv),
    ("Grant", GrantPriv),
    ("References", ReferencesPriv),
    ("Lock Tables", LockTablesPriv),
    ("Create Temporary Tables", CreateTMPTablePriv),
    ("Event", EventPriv),
    ("Create Routine", CreateRoutinePriv),
    ("Alter Routine", AlterRoutinePriv),
    ("Alter", AlterPriv),
    ("Execute", ExecutePriv),
    ("Index", IndexPriv),
    ("Create View", CreateViewPriv),
    ("Show View", ShowViewPriv),
    ("Trigger", TriggerPriv),
];

/// Complete source privilege-to-user-table-column map.
pub const PRIVILEGE_USER_COLUMNS: &[(PrivilegeType, &str)] = &[
    (CreatePriv, "Create_priv"),
    (SelectPriv, "Select_priv"),
    (InsertPriv, "Insert_priv"),
    (UpdatePriv, "Update_priv"),
    (DeletePriv, "Delete_priv"),
    (ShowDBPriv, "Show_db_priv"),
    (SuperPriv, "Super_priv"),
    (CreateUserPriv, "Create_user_priv"),
    (CreateTablespacePriv, "Create_tablespace_priv"),
    (TriggerPriv, "Trigger_priv"),
    (DropPriv, "Drop_priv"),
    (ProcessPriv, "Process_priv"),
    (GrantPriv, "Grant_priv"),
    (ReferencesPriv, "References_priv"),
    (AlterPriv, "Alter_priv"),
    (ExecutePriv, "Execute_priv"),
    (IndexPriv, "Index_priv"),
    (CreateViewPriv, "Create_view_priv"),
    (ShowViewPriv, "Show_view_priv"),
    (CreateRolePriv, "Create_role_priv"),
    (DropRolePriv, "Drop_role_priv"),
    (CreateTMPTablePriv, "Create_tmp_table_priv"),
    (LockTablesPriv, "Lock_tables_priv"),
    (CreateRoutinePriv, "Create_routine_priv"),
    (AlterRoutinePriv, "Alter_routine_priv"),
    (EventPriv, "Event_priv"),
    (ShutdownPriv, "Shutdown_priv"),
    (ReloadPriv, "Reload_priv"),
    (FilePriv, "File_priv"),
    (ConfigPriv, "Config_priv"),
    (ReplicationClientPriv, "Repl_client_priv"),
    (ReplicationSlavePriv, "Repl_slave_priv"),
];

impl PrivilegeType {
    /// SQL identifier used by GRANT/SHOW output, or empty for an unknown bit.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        lookup_forward(PRIVILEGE_NAMES, self)
    }
    /// mysql.user/mysql.db column name, or empty for an unknown bit.
    #[must_use]
    pub fn column_string(self) -> &'static str {
        lookup_forward(PRIVILEGE_USER_COLUMNS, self)
    }
    /// SET enum spelling, or empty for a privilege excluded by the source map.
    #[must_use]
    pub fn set_string(self) -> &'static str {
        lookup_forward(PRIVILEGE_SET_NAMES, self)
    }
}

impl std::fmt::Display for PrivilegeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn lookup_forward(
    table: &'static [(PrivilegeType, &'static str)],
    key: PrivilegeType,
) -> &'static str {
    table
        .iter()
        .find_map(|(item, value)| (*item == key).then_some(*value))
        .unwrap_or("")
}

/// Constructs a privilege from an exact privilege-table column name.
#[must_use]
pub fn privilege_from_column(column: &str) -> Option<PrivilegeType> {
    PRIVILEGE_USER_COLUMNS
        .iter()
        .find_map(|(item, value)| (*value == column).then_some(*item))
}

/// Constructs a privilege from an exact privilege SET spelling.
#[must_use]
pub fn privilege_from_set_enum(value: &str) -> Option<PrivilegeType> {
    SET_ENUM_PRIVILEGES
        .iter()
        .find_map(|(name, item)| (*name == value).then_some(*item))
}

/// Returns whether `privileges` contains `privilege`.
#[must_use]
pub fn has_privilege(privileges: &[PrivilegeType], privilege: PrivilegeType) -> bool {
    privileges.contains(&privilege)
}

/// All privileges legal in global scope, in source order.
pub const ALL_GLOBAL_PRIVILEGES: &[PrivilegeType] = &[
    SelectPriv,
    InsertPriv,
    UpdatePriv,
    DeletePriv,
    CreatePriv,
    DropPriv,
    ProcessPriv,
    ReferencesPriv,
    AlterPriv,
    ShowDBPriv,
    SuperPriv,
    ExecutePriv,
    IndexPriv,
    CreateUserPriv,
    CreateTablespacePriv,
    TriggerPriv,
    CreateViewPriv,
    ShowViewPriv,
    CreateRolePriv,
    DropRolePriv,
    CreateTMPTablePriv,
    LockTablesPriv,
    CreateRoutinePriv,
    AlterRoutinePriv,
    EventPriv,
    ShutdownPriv,
    ReloadPriv,
    FilePriv,
    ConfigPriv,
    ReplicationClientPriv,
    ReplicationSlavePriv,
];
/// All privileges legal in database scope, in source order.
pub const ALL_DATABASE_PRIVILEGES: &[PrivilegeType] = &[
    SelectPriv,
    InsertPriv,
    UpdatePriv,
    DeletePriv,
    CreatePriv,
    DropPriv,
    ReferencesPriv,
    LockTablesPriv,
    CreateTMPTablePriv,
    EventPriv,
    CreateRoutinePriv,
    AlterRoutinePriv,
    AlterPriv,
    ExecutePriv,
    IndexPriv,
    CreateViewPriv,
    ShowViewPriv,
    TriggerPriv,
];
/// All privileges legal in table scope, in source order.
pub const ALL_TABLE_PRIVILEGES: &[PrivilegeType] = &[
    SelectPriv,
    InsertPriv,
    UpdatePriv,
    DeletePriv,
    CreatePriv,
    DropPriv,
    IndexPriv,
    ReferencesPriv,
    AlterPriv,
    CreateViewPriv,
    ShowViewPriv,
    TriggerPriv,
];
/// All privileges legal in column scope, in source order.
pub const ALL_COLUMN_PRIVILEGES: &[PrivilegeType] =
    &[SelectPriv, InsertPriv, UpdatePriv, ReferencesPriv];
/// Static privileges that exist only at global scope, in source order.
pub const STATIC_GLOBAL_ONLY_PRIVILEGES: &[PrivilegeType] = &[
    ProcessPriv,
    ShowDBPriv,
    SuperPriv,
    CreateUserPriv,
    CreateTablespacePriv,
    ShutdownPriv,
    ReloadPriv,
    FilePriv,
    ReplicationClientPriv,
    ReplicationSlavePriv,
    ConfigPriv,
];
