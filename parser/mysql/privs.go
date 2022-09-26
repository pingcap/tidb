// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

// AllPrivilegeLiteral is the string literal for All Privilege.
const AllPrivilegeLiteral = "ALL PRIVILEGES"

// Priv2Str is the map for privilege to string.
var Priv2Str = map[PrivilegeType]string{
	CreatePriv:            "Create",
	SelectPriv:            "Select",
	InsertPriv:            "Insert",
	UpdatePriv:            "Update",
	DeletePriv:            "Delete",
	ShowDBPriv:            "Show Databases",
	SuperPriv:             "Super",
	CreateUserPriv:        "Create User",
	CreateTablespacePriv:  "Create Tablespace",
	TriggerPriv:           "Trigger",
	DropPriv:              "Drop",
	ProcessPriv:           "Process",
	GrantPriv:             "Grant Option",
	ReferencesPriv:        "References",
	AlterPriv:             "Alter",
	ExecutePriv:           "Execute",
	IndexPriv:             "Index",
	CreateViewPriv:        "Create View",
	ShowViewPriv:          "Show View",
	CreateRolePriv:        "Create Role",
	DropRolePriv:          "Drop Role",
	CreateTMPTablePriv:    "CREATE TEMPORARY TABLES",
	LockTablesPriv:        "LOCK TABLES",
	CreateRoutinePriv:     "CREATE ROUTINE",
	AlterRoutinePriv:      "ALTER ROUTINE",
	EventPriv:             "EVENT",
	ShutdownPriv:          "SHUTDOWN",
	ReloadPriv:            "RELOAD",
	FilePriv:              "FILE",
	ConfigPriv:            "CONFIG",
	UsagePriv:             "USAGE",
	ReplicationClientPriv: "REPLICATION CLIENT",
	ReplicationSlavePriv:  "REPLICATION SLAVE",
	AllPriv:               AllPrivilegeLiteral,
}

// Priv2SetStr is the map for privilege to string.
var Priv2SetStr = map[PrivilegeType]string{
	CreatePriv:         "Create",
	SelectPriv:         "Select",
	InsertPriv:         "Insert",
	UpdatePriv:         "Update",
	DeletePriv:         "Delete",
	DropPriv:           "Drop",
	GrantPriv:          "Grant",
	ReferencesPriv:     "References",
	LockTablesPriv:     "Lock Tables",
	CreateTMPTablePriv: "Create Temporary Tables",
	EventPriv:          "Event",
	CreateRoutinePriv:  "Create Routine",
	AlterRoutinePriv:   "Alter Routine",
	AlterPriv:          "Alter",
	ExecutePriv:        "Execute",
	IndexPriv:          "Index",
	CreateViewPriv:     "Create View",
	ShowViewPriv:       "Show View",
	CreateRolePriv:     "Create Role",
	DropRolePriv:       "Drop Role",
	ShutdownPriv:       "Shutdown Role",
	TriggerPriv:        "Trigger",
}

// SetStr2Priv is the map for privilege set string to privilege type.
var SetStr2Priv = map[string]PrivilegeType{
	"Create":                  CreatePriv,
	"Select":                  SelectPriv,
	"Insert":                  InsertPriv,
	"Update":                  UpdatePriv,
	"Delete":                  DeletePriv,
	"Drop":                    DropPriv,
	"Grant":                   GrantPriv,
	"References":              ReferencesPriv,
	"Lock Tables":             LockTablesPriv,
	"Create Temporary Tables": CreateTMPTablePriv,
	"Event":                   EventPriv,
	"Create Routine":          CreateRoutinePriv,
	"Alter Routine":           AlterRoutinePriv,
	"Alter":                   AlterPriv,
	"Execute":                 ExecutePriv,
	"Index":                   IndexPriv,
	"Create View":             CreateViewPriv,
	"Show View":               ShowViewPriv,
	"Trigger":                 TriggerPriv,
}

// Priv2UserCol is the privilege to mysql.user table column name.
var Priv2UserCol = map[PrivilegeType]string{
	CreatePriv:            "Create_priv",
	SelectPriv:            "Select_priv",
	InsertPriv:            "Insert_priv",
	UpdatePriv:            "Update_priv",
	DeletePriv:            "Delete_priv",
	ShowDBPriv:            "Show_db_priv",
	SuperPriv:             "Super_priv",
	CreateUserPriv:        "Create_user_priv",
	CreateTablespacePriv:  "Create_tablespace_priv",
	TriggerPriv:           "Trigger_priv",
	DropPriv:              "Drop_priv",
	ProcessPriv:           "Process_priv",
	GrantPriv:             "Grant_priv",
	ReferencesPriv:        "References_priv",
	AlterPriv:             "Alter_priv",
	ExecutePriv:           "Execute_priv",
	IndexPriv:             "Index_priv",
	CreateViewPriv:        "Create_view_priv",
	ShowViewPriv:          "Show_view_priv",
	CreateRolePriv:        "Create_role_priv",
	DropRolePriv:          "Drop_role_priv",
	CreateTMPTablePriv:    "Create_tmp_table_priv",
	LockTablesPriv:        "Lock_tables_priv",
	CreateRoutinePriv:     "Create_routine_priv",
	AlterRoutinePriv:      "Alter_routine_priv",
	EventPriv:             "Event_priv",
	ShutdownPriv:          "Shutdown_priv",
	ReloadPriv:            "Reload_priv",
	FilePriv:              "File_priv",
	ConfigPriv:            "Config_priv",
	ReplicationClientPriv: "Repl_client_priv",
	ReplicationSlavePriv:  "Repl_slave_priv",
}

// Col2PrivType is the privilege tables column name to privilege type.
var Col2PrivType = map[string]PrivilegeType{
	"Create_priv":            CreatePriv,
	"Select_priv":            SelectPriv,
	"Insert_priv":            InsertPriv,
	"Update_priv":            UpdatePriv,
	"Delete_priv":            DeletePriv,
	"Show_db_priv":           ShowDBPriv,
	"Super_priv":             SuperPriv,
	"Create_user_priv":       CreateUserPriv,
	"Create_tablespace_priv": CreateTablespacePriv,
	"Trigger_priv":           TriggerPriv,
	"Drop_priv":              DropPriv,
	"Process_priv":           ProcessPriv,
	"Grant_priv":             GrantPriv,
	"References_priv":        ReferencesPriv,
	"Alter_priv":             AlterPriv,
	"Execute_priv":           ExecutePriv,
	"Index_priv":             IndexPriv,
	"Create_view_priv":       CreateViewPriv,
	"Show_view_priv":         ShowViewPriv,
	"Create_role_priv":       CreateRolePriv,
	"Drop_role_priv":         DropRolePriv,
	"Create_tmp_table_priv":  CreateTMPTablePriv,
	"Lock_tables_priv":       LockTablesPriv,
	"Create_routine_priv":    CreateRoutinePriv,
	"Alter_routine_priv":     AlterRoutinePriv,
	"Event_priv":             EventPriv,
	"Shutdown_priv":          ShutdownPriv,
	"Reload_priv":            ReloadPriv,
	"File_priv":              FilePriv,
	"Config_priv":            ConfigPriv,
	"Repl_client_priv":       ReplicationClientPriv,
	"Repl_slave_priv":        ReplicationSlavePriv,
}

// PrivilegeType privilege
type PrivilegeType uint64

// NewPrivFromColumn constructs priv from a column name. False means invalid priv column name.
func NewPrivFromColumn(col string) (PrivilegeType, bool) {
	p, o := Col2PrivType[col]
	return p, o
}

// NewPrivFromSetEnum constructs priv from a set enum. False means invalid priv enum.
func NewPrivFromSetEnum(e string) (PrivilegeType, bool) {
	p, o := SetStr2Priv[e]
	return p, o
}

// String returns the corresponding identifier in SQLs.
func (p PrivilegeType) String() string {
	if s, ok := Priv2Str[p]; ok {
		return s
	}
	return ""
}

// ColumnString returns the corresponding name of columns in mysql.user/mysql.db.
func (p PrivilegeType) ColumnString() string {
	if s, ok := Priv2UserCol[p]; ok {
		return s
	}
	return ""
}

// SetString returns the corresponding set enum string in Table_priv/Column_priv of mysql.tables_priv/mysql.columns_priv.
func (p PrivilegeType) SetString() string {
	if s, ok := Priv2SetStr[p]; ok {
		return s
	}
	return ""
}

const (
	// UsagePriv is a synonym for “no privileges”
	UsagePriv PrivilegeType = 1 << iota
	// CreatePriv is the privilege to create schema/table.
	CreatePriv
	// SelectPriv is the privilege to read from table.
	SelectPriv
	// InsertPriv is the privilege to insert data into table.
	InsertPriv
	// UpdatePriv is the privilege to update data in table.
	UpdatePriv
	// DeletePriv is the privilege to delete data from table.
	DeletePriv
	// ShowDBPriv is the privilege to run show databases statement.
	ShowDBPriv
	// SuperPriv enables many operations and server behaviors.
	SuperPriv
	// CreateUserPriv is the privilege to create user.
	CreateUserPriv
	// TriggerPriv is not checked yet.
	TriggerPriv
	// DropPriv is the privilege to drop schema/table.
	DropPriv
	// ProcessPriv pertains to display of information about the threads executing within the server.
	ProcessPriv
	// GrantPriv is the privilege to grant privilege to user.
	GrantPriv
	// ReferencesPriv is not checked yet.
	ReferencesPriv
	// AlterPriv is the privilege to run alter statement.
	AlterPriv
	// ExecutePriv is the privilege to run execute statement.
	ExecutePriv
	// IndexPriv is the privilege to create/drop index.
	IndexPriv
	// CreateViewPriv is the privilege to create view.
	CreateViewPriv
	// ShowViewPriv is the privilege to show create view.
	ShowViewPriv
	// CreateRolePriv the privilege to create a role.
	CreateRolePriv
	// DropRolePriv is the privilege to drop a role.
	DropRolePriv
	// CreateTMPTablePriv is the privilege to create a temporary table.
	CreateTMPTablePriv
	// LockTablesPriv is the privilege to lock tables.
	LockTablesPriv
	// CreateRoutinePriv is the privilege to create a stored routine.
	CreateRoutinePriv
	// AlterRoutinePriv is the privilege to alter a stored routine.
	AlterRoutinePriv
	// EventPriv is the privilege to event.
	EventPriv

	// ShutdownPriv the privilege to shutdown a server.
	ShutdownPriv
	// ReloadPriv is the privilege to enable the use of the FLUSH statement.
	ReloadPriv
	// FilePriv is the privilege to enable the use of LOAD DATA and SELECT ... INTO OUTFILE.
	FilePriv
	// ConfigPriv is the privilege to enable the use SET CONFIG statements.
	ConfigPriv

	// CreateTablespacePriv is the privilege to create tablespace.
	CreateTablespacePriv

	// ReplicationClientPriv is used in MySQL replication
	ReplicationClientPriv
	// ReplicationSlavePriv is used in MySQL replication
	ReplicationSlavePriv

	// AllPriv is the privilege for all actions.
	AllPriv
	/*
	 *  Please add the new priv before AllPriv to keep the values consistent across versions.
	 */

	// ExtendedPriv is used to successful parse privileges not included above.
	// these are dynamic privileges in MySQL 8.0 and other extended privileges like LOAD FROM S3 in Aurora.
	ExtendedPriv
)

// AllPrivMask is the mask for PrivilegeType with all bits set to 1.
// If it's passed to RequestVerification, it means any privilege would be OK.
const AllPrivMask = AllPriv - 1

// Privileges is the list of all privileges.
type Privileges []PrivilegeType

// Has checks whether PrivilegeType has the privilege.
func (privs Privileges) Has(p PrivilegeType) bool {
	for _, cp := range privs {
		if cp == p {
			return true
		}
	}
	return false
}

// AllGlobalPrivs is all the privileges in global scope.
var AllGlobalPrivs = Privileges{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, ProcessPriv, ReferencesPriv, AlterPriv, ShowDBPriv, SuperPriv, ExecutePriv, IndexPriv, CreateUserPriv, CreateTablespacePriv, TriggerPriv, CreateViewPriv, ShowViewPriv, CreateRolePriv, DropRolePriv, CreateTMPTablePriv, LockTablesPriv, CreateRoutinePriv, AlterRoutinePriv, EventPriv, ShutdownPriv, ReloadPriv, FilePriv, ConfigPriv, ReplicationClientPriv, ReplicationSlavePriv}

// AllDBPrivs is all the privileges in database scope.
var AllDBPrivs = Privileges{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, ReferencesPriv, LockTablesPriv, CreateTMPTablePriv, EventPriv, CreateRoutinePriv, AlterRoutinePriv, AlterPriv, ExecutePriv, IndexPriv, CreateViewPriv, ShowViewPriv, TriggerPriv}

// AllTablePrivs is all the privileges in table scope.
var AllTablePrivs = Privileges{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, IndexPriv, ReferencesPriv, AlterPriv, CreateViewPriv, ShowViewPriv, TriggerPriv}

// AllColumnPrivs is all the privileges in column scope.
var AllColumnPrivs = Privileges{SelectPriv, InsertPriv, UpdatePriv, ReferencesPriv}

// StaticGlobalOnlyPrivs is all the privileges only in global scope and different from dynamic privileges.
var StaticGlobalOnlyPrivs = Privileges{ProcessPriv, ShowDBPriv, SuperPriv, CreateUserPriv, CreateTablespacePriv, ShutdownPriv, ReloadPriv, FilePriv, ReplicationClientPriv, ReplicationSlavePriv, ConfigPriv}
