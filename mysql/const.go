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
type PrivilegeType uint32

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
	// AlterPriv is the privilege to run alter statement.
	AlterPriv
	// ExecutePriv is the privilege to run execute statement.
	ExecutePriv
	// IndexPriv is the privilege to create/drop index.
	IndexPriv
	// AllPriv is the privilege for all actions.
	AllPriv
)

// AllPrivMask is the mask for PrivilegeType with all bits set to 1.
const AllPrivMask = AllPriv - 1

// Priv2UserCol is the privilege to mysql.user table column name.
var Priv2UserCol = map[PrivilegeType]string{
	CreatePriv:     "Create_priv",
	SelectPriv:     "Select_priv",
	InsertPriv:     "Insert_priv",
	UpdatePriv:     "Update_priv",
	DeletePriv:     "Delete_priv",
	ShowDBPriv:     "Show_db_priv",
	SuperPriv:      "Super_priv",
	CreateUserPriv: "Create_user_priv",
	TriggerPriv:    "Trigger_priv",
	DropPriv:       "Drop_priv",
	ProcessPriv:    "Process_priv",
	GrantPriv:      "Grant_priv",
	AlterPriv:      "Alter_priv",
	ExecutePriv:    "Execute_priv",
	IndexPriv:      "Index_priv",
}

// Col2PrivType is the privilege tables column name to privilege type.
var Col2PrivType = map[string]PrivilegeType{
	"Create_priv":      CreatePriv,
	"Select_priv":      SelectPriv,
	"Insert_priv":      InsertPriv,
	"Update_priv":      UpdatePriv,
	"Delete_priv":      DeletePriv,
	"Show_db_priv":     ShowDBPriv,
	"Super_priv":       SuperPriv,
	"Create_user_priv": CreateUserPriv,
	"Trigger_priv":     TriggerPriv,
	"Drop_priv":        DropPriv,
	"Process_priv":     ProcessPriv,
	"Grant_priv":       GrantPriv,
	"Alter_priv":       AlterPriv,
	"Execute_priv":     ExecutePriv,
	"Index_priv":       IndexPriv,
}

// AllGlobalPrivs is all the privileges in global scope.
var AllGlobalPrivs = []PrivilegeType{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, ProcessPriv, GrantPriv, AlterPriv, ShowDBPriv, SuperPriv, ExecutePriv, IndexPriv, CreateUserPriv, TriggerPriv}

// Priv2Str is the map for privilege to string.
var Priv2Str = map[PrivilegeType]string{
	CreatePriv:     "Create",
	SelectPriv:     "Select",
	InsertPriv:     "Insert",
	UpdatePriv:     "Update",
	DeletePriv:     "Delete",
	ShowDBPriv:     "Show Databases",
	SuperPriv:      "Super",
	CreateUserPriv: "Create User",
	TriggerPriv:    "Trigger",
	DropPriv:       "Drop",
	ProcessPriv:    "Process",
	GrantPriv:      "Grant Option",
	AlterPriv:      "Alter",
	ExecutePriv:    "Execute",
	IndexPriv:      "Index",
}

// Priv2SetStr is the map for privilege to string.
var Priv2SetStr = map[PrivilegeType]string{
	CreatePriv:  "Create",
	SelectPriv:  "Select",
	InsertPriv:  "Insert",
	UpdatePriv:  "Update",
	DeletePriv:  "Delete",
	DropPriv:    "Drop",
	GrantPriv:   "Grant",
	AlterPriv:   "Alter",
	ExecutePriv: "Execute",
	IndexPriv:   "Index",
}

// SetStr2Priv is the map for privilege set string to privilege type.
var SetStr2Priv = map[string]PrivilegeType{
	"Create":  CreatePriv,
	"Select":  SelectPriv,
	"Insert":  InsertPriv,
	"Update":  UpdatePriv,
	"Delete":  DeletePriv,
	"Drop":    DropPriv,
	"Grant":   GrantPriv,
	"Alter":   AlterPriv,
	"Execute": ExecutePriv,
	"Index":   IndexPriv,
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

// FormatFunc is the locale format function signature.
type FormatFunc func(string, string) (string, error)

// GetLocaleFormatFunction get the format function for sepcific locale.
func GetLocaleFormatFunction(loc string) FormatFunc {
	locale, exist := locale2FormatFunction[loc]
	if !exist {
		return formatNotSupport
	}
	return locale
}

// locale2FormatFunction is the string represent of locale format function.
var locale2FormatFunction = map[string]FormatFunc{
	"en_US": formatENUS,
	"zh_CN": formatZHCN,
}
