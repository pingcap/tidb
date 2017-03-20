// Copyright 2015 PingCAP, Inc.
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

import (
	"strings"
)

// Version informations.
const (
	MinProtocolVersion byte = 10
	MaxPayloadLen      int  = 1<<24 - 1
	// The version number should be three digits.
	// See https://dev.mysql.com/doc/refman/5.7/en/which-version.html
	ServerVersion string = "5.7.1-TiDB-1.0"
)

// Header informations.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

// Server informations.
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSend        uint16 = 0x0080
	ServerStatusDBDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscaped uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerStatusWasSlow            uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
)

// Identifier length limitations.
const (
	MaxTableNameLength    int = 64
	MaxDatabaseNameLength int = 64
	MaxColumnNameLength   int = 64
)

// Command informations.
const (
	ComSleep byte = iota
	ComQuit
	ComInitDB
	ComQuery
	ComFieldList
	ComCreateDB
	ComDropDB
	ComRefresh
	ComShutdown
	ComStatistics
	ComProcessInfo
	ComConnect
	ComProcessKill
	ComDebug
	ComPing
	ComTime
	ComDelayedInsert
	ComChangeUser
	ComBinlogDump
	ComTableDump
	ComConnectOut
	ComRegisterSlave
	ComStmtPrepare
	ComStmtExecute
	ComStmtSendLongData
	ComStmtClose
	ComStmtReset
	ComSetOption
	ComStmtFetch
	ComDaemon
	ComBinlogDumpGtid
	ComResetConnection
)

// Client informations.
const (
	ClientLongPassword uint32 = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithDB
	ClientNoSchema
	ClientCompress
	ClientODBC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPSMultiResults
	ClientPluginAuth
	ClientConnectAtts
	ClientPluginAuthLenencClientData
)

// Cache type informations.
const (
	TypeNoCache byte = 0xff
)

// Auth name informations.
const (
	AuthName = "mysql_native_password"
)

// MySQL database and tables.
const (
	// SystemDB is the name of system database.
	SystemDB = "mysql"
	// UserTable is the table in system db contains user info.
	UserTable = "User"
	// DBTable is the table in system db contains db scope privilege info.
	DBTable = "DB"
	// TablePrivTable is the table in system db contains table scope privilege info.
	TablePrivTable = "Tables_priv"
	// ColumnPrivTable is the table in system db contains column scope privilege info.
	ColumnPrivTable = "Columns_priv"
	// GlobalVariablesTable is the table contains global system variables.
	GlobalVariablesTable = "GLOBAL_VARIABLES"
	// GlobalStatusTable is the table contains global status variables.
	GlobalStatusTable = "GLOBAL_STATUS"
	// TiDBTable is the table contains tidb info.
	TiDBTable = "tidb"
)

// PrivilegeType  privilege
type PrivilegeType uint64

const (
	_ PrivilegeType = 1 << iota
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
	// CreateUserPriv is the privilege to create user.
	CreateUserPriv
	// DropPriv is the privilege to drop schema/table.
	DropPriv
	// GrantPriv is the privilege to grant privilege to user.
	GrantPriv
	// AlterPriv is the privilege to run alter statement.
	AlterPriv
	// ExecutePriv is the privilege to run execute statement.
	ExecutePriv
	// IndexPriv is the privilege to create/drop index.
	IndexPriv
	// ReloadPriv enables use of the FLUSH statement.
	ReloadPriv
	// ShutdownPriv enables use of the SHUTDOWN statement.
	ShutdownPriv
	// ProcessPriv can be used to view the plain text of currently executing statements, including statements that set or change passwords.
	ProcessPriv
	// FilePriv can be abused to read into a database table any files that the MySQL server can read on the server host.
	FilePriv
	// ReferencesPriv is required for creation of a foreign key constraint.
	ReferencesPriv
	// SuperPriv enables many operations and server behaviors.
	SuperPriv
	// CreateTMPTablePriv enables the creation of temporary tables using the CREATE TEMPORARY TABLE statement.
	CreateTMPTablePriv
	// LockTablesPriv enables the use of explicit LOCK TABLES statements to lock tables.
	LockTablesPriv
	// ReplSlavePriv should be granted to accounts that are used by slave servers to connect to the current server as their master.
	ReplSlavePriv
	// ReplClientPriv enables the use of SHOW MASTER STATUS, SHOW SLAVE STATUS, and SHOW BINARY LOGS.
	ReplClientPriv
	// CreateViewPriv enables use of CREATE VIEW.
	CreateViewPriv
	// ShowViewPriv enables use of SHOW CREATE VIEW.
	ShowViewPriv
	// CreateRoutinePriv is needed to create stored routines (procedures and functions).
	CreateRoutinePriv
	// AlterRoutinePriv is needed to alter or drop stored routines (procedures and functions).
	AlterRoutinePriv
	// EventPriv is required to create, alter, drop, or see events for the Event Scheduler.
	EventPriv
	// TriggerPriv enables trigger operations. You must have this privilege for a table to create, drop, execute, or display triggers for that table.
	TriggerPriv
	// CreateTablespacePriv is needed to create, alter, or drop tablespaces and log file groups.
	CreateTablespacePriv
	// AllPriv is the privilege for all actions.
	AllPriv
)

// Priv2UserCol is the privilege to mysql.user table column name.
var Priv2UserCol = map[PrivilegeType]string{
	CreatePriv:           "Create_priv",
	SelectPriv:           "Select_priv",
	InsertPriv:           "Insert_priv",
	UpdatePriv:           "Update_priv",
	DeletePriv:           "Delete_priv",
	ShowDBPriv:           "Show_db_priv",
	CreateUserPriv:       "Create_user_priv",
	DropPriv:             "Drop_priv",
	GrantPriv:            "Grant_priv",
	AlterPriv:            "Alter_priv",
	ExecutePriv:          "Execute_priv",
	IndexPriv:            "Index_priv",
	ReloadPriv:           "Reload_priv",
	ShutdownPriv:         "Shutdown_priv",
	ProcessPriv:          "Process_priv",
	FilePriv:             "File_priv",
	ReferencesPriv:       "References_priv",
	SuperPriv:            "Super_priv",
	CreateTMPTablePriv:   "Create_tmp_table_priv",
	LockTablesPriv:       "Lock_tables_priv",
	ReplSlavePriv:        "Repl_slave_priv",
	ReplClientPriv:       "Repl_client_priv",
	CreateViewPriv:       "Create_view_priv",
	ShowViewPriv:         "Show_view_priv",
	CreateRoutinePriv:    "Create_routine_priv",
	AlterRoutinePriv:     "Alter_routine_priv",
	EventPriv:            "Event_priv",
	TriggerPriv:          "Trigger_priv",
	CreateTablespacePriv: "Create_tablespace_priv",
}

// Col2PrivType is the privilege tables column name to privilege type.
var Col2PrivType = map[string]PrivilegeType{
	"Create_priv":            CreatePriv,
	"Select_priv":            SelectPriv,
	"Insert_priv":            InsertPriv,
	"Update_priv":            UpdatePriv,
	"Delete_priv":            DeletePriv,
	"Show_db_priv":           ShowDBPriv,
	"Create_user_priv":       CreateUserPriv,
	"Drop_priv":              DropPriv,
	"Grant_priv":             GrantPriv,
	"Alter_priv":             AlterPriv,
	"Execute_priv":           ExecutePriv,
	"Index_priv":             IndexPriv,
	"Reload_priv":            ReloadPriv,
	"Shutdown_priv":          ShutdownPriv,
	"Process_priv":           ProcessPriv,
	"File_priv":              FilePriv,
	"References_priv":        ReferencesPriv,
	"Super_priv":             SuperPriv,
	"Create_tmp_table_priv":  CreateTMPTablePriv,
	"Lock_tables_priv":       LockTablesPriv,
	"Repl_slave_priv":        ReplSlavePriv,
	"Repl_client_priv":       ReplClientPriv,
	"Create_view_priv":       CreateViewPriv,
	"Show_view_priv":         ShowViewPriv,
	"Create_routine_priv":    CreateRoutinePriv,
	"Alter_routine_priv":     AlterRoutinePriv,
	"Event_priv":             EventPriv,
	"Trigger_priv":           TriggerPriv,
	"Create_tablespace_priv": CreateTablespacePriv,
}

// AllGlobalPrivs is all the privileges in global scope.
var AllGlobalPrivs = []PrivilegeType{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, GrantPriv, AlterPriv, ShowDBPriv, ExecutePriv, IndexPriv, CreateUserPriv}

// Priv2Str is the map for privilege to string.
var Priv2Str = map[PrivilegeType]string{
	CreatePriv:           "Create",
	SelectPriv:           "Select",
	InsertPriv:           "Insert",
	UpdatePriv:           "Update",
	DeletePriv:           "Delete",
	ShowDBPriv:           "Show Databases",
	CreateUserPriv:       "Create User",
	DropPriv:             "Drop",
	GrantPriv:            "Grant Option",
	AlterPriv:            "Alter",
	ExecutePriv:          "Execute",
	IndexPriv:            "Index",
	ReloadPriv:           "Reload",
	ShutdownPriv:         "Shutdown",
	ProcessPriv:          "Process",
	FilePriv:             "File",
	ReferencesPriv:       "References",
	SuperPriv:            "Super",
	CreateTMPTablePriv:   "Create Temporary Tables",
	LockTablesPriv:       "Lock Tables",
	ReplSlavePriv:        "Replication Slave",
	ReplClientPriv:       "Replication Client",
	CreateViewPriv:       "Create View",
	ShowViewPriv:         "Show View",
	CreateRoutinePriv:    "Create Routine",
	AlterRoutinePriv:     "Alter Routine",
	EventPriv:            "Event",
	TriggerPriv:          "Trigger",
	CreateTablespacePriv: "Create Tablespace",
}

// Priv2SetStr is the map for privilege type to privilege set string.
var Priv2SetStr = map[PrivilegeType]string{
	CreatePriv:     "Create",
	SelectPriv:     "Select",
	InsertPriv:     "Insert",
	UpdatePriv:     "Update",
	DeletePriv:     "Delete",
	DropPriv:       "Drop",
	GrantPriv:      "Grant",
	AlterPriv:      "Alter",
	ExecutePriv:    "Execute",
	IndexPriv:      "Index",
	ReferencesPriv: "References",
	CreateViewPriv: "Create View",
	ShowViewPriv:   "Show View",
	TriggerPriv:    "Trigger",
}

// SetStr2Priv is the map for privilege set string to privilege type.
var SetStr2Priv = map[string]PrivilegeType{
	"Create":      CreatePriv,
	"Select":      SelectPriv,
	"Insert":      InsertPriv,
	"Update":      UpdatePriv,
	"Delete":      DeletePriv,
	"Drop":        DropPriv,
	"Grant":       GrantPriv,
	"Alter":       AlterPriv,
	"Execute":     ExecutePriv,
	"Index":       IndexPriv,
	"References":  ReferencesPriv,
	"Create View": CreateViewPriv,
	"Show View":   ShowViewPriv,
	"Trigger":     TriggerPriv,
}

// AllDBPrivs is all the privileges in database scope.
var AllDBPrivs = []PrivilegeType{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, GrantPriv, AlterPriv, ExecutePriv, IndexPriv}

// AllTablePrivs is all the privileges in table scope.
var AllTablePrivs = []PrivilegeType{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, GrantPriv, AlterPriv, IndexPriv}

// AllColumnPrivs is all the privileges in column scope.
var AllColumnPrivs = []PrivilegeType{SelectPriv, InsertPriv, UpdatePriv}

// AllPrivilegeLiteral is the string literal for All Privilege.
const AllPrivilegeLiteral = "ALL PRIVILEGES"

// DefaultLengthOfMysqlTypes is the map for default physical length of MySQL data types.
// See http://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
var DefaultLengthOfMysqlTypes = map[byte]int{
	TypeYear:      1,
	TypeDate:      3,
	TypeDuration:  3,
	TypeDatetime:  8,
	TypeTimestamp: 4,

	TypeTiny:     1,
	TypeShort:    2,
	TypeInt24:    3,
	TypeLong:     4,
	TypeLonglong: 8,
	TypeFloat:    4,
	TypeDouble:   8,

	TypeEnum:   2,
	TypeString: 1,
	TypeSet:    8,
}

// DefaultLengthOfTimeFraction is the map for default physical length of time fractions.
var DefaultLengthOfTimeFraction = map[int]int{
	0: 0,

	1: 1,
	2: 1,

	3: 2,
	4: 2,

	5: 3,
	6: 3,
}

// SQLMode is the type for MySQL sql_mode.
// See https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html
type SQLMode int

// consts for sql modes.
const (
	ModeNone        SQLMode = 0
	ModeRealAsFloat SQLMode = 1 << iota
	ModePipesAsConcat
	ModeANSIQuotes
	ModeIgnoreSpace
	ModeNotUsed
	ModeOnlyFullGroupBy
	ModeNoUnsignedSubtraction
	ModeNoDirInCreate
	ModePostgreSQL
	ModeOracle
	ModeMsSQL
	ModeDb2
	ModeMaxdb
	ModeNoKeyOptions
	ModeNoTableOptions
	ModeNoFieldOptions
	ModeMySQL323
	ModeMySQL40
	ModeANSI
	ModeNoAutoValueOnZero
	ModeNoBackslashEscapes
	ModeStrictTransTables
	ModeStrictAllTables
	ModeNoZeroInDate
	ModeNoZeroDate
	ModeInvalidDates
	ModeErrorForDivisionByZero
	ModeTraditional
	ModeNoAutoCreateUser
	ModeHighNotPrecedence
	ModeNoEngineSubstitution
	ModePadCharToFullLength
)

// GetSQLMode gets the sql mode for string literal.
func GetSQLMode(str string) SQLMode {
	str = strings.ToUpper(str)
	mode, ok := Str2SQLMode[str]
	if !ok {
		return ModeNone
	}
	return mode
}

// Str2SQLMode is the string represent of sql_mode to sql_mode map.
var Str2SQLMode = map[string]SQLMode{
	"REAL_AS_FLOAT":              ModeRealAsFloat,
	"PIPES_AS_CONCAT":            ModePipesAsConcat,
	"ANSI_QUOTES":                ModeANSIQuotes,
	"IGNORE_SPACE":               ModeIgnoreSpace,
	"NOT_USED":                   ModeNotUsed,
	"ONLY_FULL_GROUP_BY":         ModeOnlyFullGroupBy,
	"NO_UNSIGNED_SUBTRACTION":    ModeNoUnsignedSubtraction,
	"NO_DIR_IN_CREATE":           ModeNoDirInCreate,
	"POSTGRESQL":                 ModePostgreSQL,
	"ORACLE":                     ModeOracle,
	"MSSQL":                      ModeMsSQL,
	"DB2":                        ModeDb2,
	"MAXDB":                      ModeMaxdb,
	"NO_KEY_OPTIONS":             ModeNoKeyOptions,
	"NO_TABLE_OPTIONS":           ModeNoTableOptions,
	"NO_FIELD_OPTIONS":           ModeNoFieldOptions,
	"MYSQL323":                   ModeMySQL323,
	"MYSQL40":                    ModeMySQL40,
	"ANSI":                       ModeANSI,
	"NO_AUTO_VALUE_ON_ZERO":      ModeNoAutoValueOnZero,
	"NO_BACKSLASH_ESCAPES":       ModeNoBackslashEscapes,
	"STRICT_TRANS_TABLES":        ModeStrictTransTables,
	"STRICT_ALL_TABLES":          ModeStrictAllTables,
	"NO_ZERO_IN_DATE":            ModeNoZeroInDate,
	"NO_ZERO_DATE":               ModeNoZeroDate,
	"INVALID_DATES":              ModeInvalidDates,
	"ERROR_FOR_DIVISION_BY_ZERO": ModeErrorForDivisionByZero,
	"TRADITIONAL":                ModeTraditional,
	"NO_AUTO_CREATE_USER":        ModeNoAutoCreateUser,
	"HIGH_NOT_PRECEDENCE":        ModeHighNotPrecedence,
	"NO_ENGINE_SUBSTITUTION":     ModeNoEngineSubstitution,
	"PAD_CHAR_TO_FULL_LENGTH":    ModePadCharToFullLength,
}
