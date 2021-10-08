// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/format"
)

func newInvalidModeErr(s string) error {
	return NewErr(ErrWrongValueForVar, "sql_mode", s)
}

// Version information.
var (
	// TiDBReleaseVersion is initialized by (git describe --tags) in Makefile.
	TiDBReleaseVersion = "None"

	// ServerVersion is the version information of this tidb-server in MySQL's format.
	ServerVersion = fmt.Sprintf("5.7.25-TiDB-%s", TiDBReleaseVersion)
)

// Header information.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

// Protocol Features
const AuthSwitchRequest byte = 0xfe

// Server information.
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

// HasCursorExistsFlag return true if cursor exists indicated by server status.
func HasCursorExistsFlag(serverStatus uint16) bool {
	return serverStatus&ServerStatusCursorExists > 0
}

// Identifier length limitations.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
const (
	// MaxPayloadLen is the max packet payload length.
	MaxPayloadLen = 1<<24 - 1
	// MaxTableNameLength is max length of table name identifier.
	MaxTableNameLength = 64
	// MaxDatabaseNameLength is max length of database name identifier.
	MaxDatabaseNameLength = 64
	// MaxColumnNameLength is max length of column name identifier.
	MaxColumnNameLength = 64
	// MaxKeyParts is max length of key parts.
	MaxKeyParts = 16
	// MaxIndexIdentifierLen is max length of index identifier.
	MaxIndexIdentifierLen = 64
	// MaxConstraintIdentifierLen is max length of constrain identifier.
	MaxConstraintIdentifierLen = 64
	// MaxViewIdentifierLen is max length of view identifier.
	MaxViewIdentifierLen = 64
	// MaxAliasIdentifierLen is max length of alias identifier.
	MaxAliasIdentifierLen = 256
	// MaxUserDefinedVariableLen is max length of user-defined variable.
	MaxUserDefinedVariableLen = 64
)

// ErrTextLength error text length limit.
const ErrTextLength = 80

// Command information.
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
	ComEnd
)

// Client information.
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

// Cache type information.
const (
	TypeNoCache byte = 0xff
)

// Auth name information.
const (
	AuthNativePassword      = "mysql_native_password"
	AuthCachingSha2Password = "caching_sha2_password"
)

// MySQL database and tables.
const (
	// SystemDB is the name of system database.
	SystemDB = "mysql"
	// GlobalPrivTable is the table in system db contains global scope privilege info.
	GlobalPrivTable = "global_priv"
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
	//  RoleEdgesTable is the table contains role relation info
	RoleEdgeTable = "role_edges"
	// DefaultRoleTable is the table contain default active role info
	DefaultRoleTable = "default_roles"
)

// MySQL type maximum length.
const (
	// For arguments that have no fixed number of decimals, the decimals value is set to 31,
	// which is 1 more than the maximum number of decimals permitted for the DECIMAL, FLOAT, and DOUBLE data types.
	NotFixedDec = 31

	MaxIntWidth              = 20
	MaxRealWidth             = 23
	MaxFloatingTypeScale     = 30
	MaxFloatingTypeWidth     = 255
	MaxDecimalScale          = 30
	MaxDecimalWidth          = 65
	MaxDateWidth             = 10 // YYYY-MM-DD.
	MaxDatetimeWidthNoFsp    = 19 // YYYY-MM-DD HH:MM:SS
	MaxDatetimeWidthWithFsp  = 26 // YYYY-MM-DD HH:MM:SS[.fraction]
	MaxDatetimeFullWidth     = 29 // YYYY-MM-DD HH:MM:SS.###### AM
	MaxDurationWidthNoFsp    = 10 // HH:MM:SS
	MaxDurationWidthWithFsp  = 15 // HH:MM:SS[.fraction]
	MaxBlobWidth             = 16777216
	MaxBitDisplayWidth       = 64
	MaxFloatPrecisionLength  = 24
	MaxDoublePrecisionLength = 53
)

// MySQL max type field length.
const (
	MaxFieldCharLength    = 255
	MaxFieldVarCharLength = 65535
)

// MaxTypeSetMembers is the number of set members.
const MaxTypeSetMembers = 64

// PWDHashLen is the length of password's hash.
const PWDHashLen = 40

// Command2Str is the command information to command name.
var Command2Str = map[byte]string{
	ComSleep:            "Sleep",
	ComQuit:             "Quit",
	ComInitDB:           "Init DB",
	ComQuery:            "Query",
	ComFieldList:        "Field List",
	ComCreateDB:         "Create DB",
	ComDropDB:           "Drop DB",
	ComRefresh:          "Refresh",
	ComShutdown:         "Shutdown",
	ComStatistics:       "Statistics",
	ComProcessInfo:      "Processlist",
	ComConnect:          "Connect",
	ComProcessKill:      "Kill",
	ComDebug:            "Debug",
	ComPing:             "Ping",
	ComTime:             "Time",
	ComDelayedInsert:    "Delayed Insert",
	ComChangeUser:       "Change User",
	ComBinlogDump:       "Binlog Dump",
	ComTableDump:        "Table Dump",
	ComConnectOut:       "Connect out",
	ComRegisterSlave:    "Register Slave",
	ComStmtPrepare:      "Prepare",
	ComStmtExecute:      "Execute",
	ComStmtSendLongData: "Long Data",
	ComStmtClose:        "Close stmt",
	ComStmtReset:        "Reset stmt",
	ComSetOption:        "Set option",
	ComStmtFetch:        "Fetch",
	ComDaemon:           "Daemon",
	ComBinlogDumpGtid:   "Binlog Dump",
	ComResetConnection:  "Reset connect",
}

// DefaultSQLMode for GLOBAL_VARIABLES
const DefaultSQLMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"

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

// HasNoZeroDateMode detects if 'NO_ZERO_DATE' mode is set in SQLMode
func (m SQLMode) HasNoZeroDateMode() bool {
	return m&ModeNoZeroDate == ModeNoZeroDate
}

// HasNoZeroInDateMode detects if 'NO_ZERO_IN_DATE' mode is set in SQLMode
func (m SQLMode) HasNoZeroInDateMode() bool {
	return m&ModeNoZeroInDate == ModeNoZeroInDate
}

// HasErrorForDivisionByZeroMode detects if 'ERROR_FOR_DIVISION_BY_ZERO' mode is set in SQLMode
func (m SQLMode) HasErrorForDivisionByZeroMode() bool {
	return m&ModeErrorForDivisionByZero == ModeErrorForDivisionByZero
}

// HasOnlyFullGroupBy detects if 'ONLY_FULL_GROUP_BY' mode is set in SQLMode
func (m SQLMode) HasOnlyFullGroupBy() bool {
	return m&ModeOnlyFullGroupBy == ModeOnlyFullGroupBy
}

// HasStrictMode detects if 'STRICT_TRANS_TABLES' or 'STRICT_ALL_TABLES' mode is set in SQLMode
func (m SQLMode) HasStrictMode() bool {
	return m&ModeStrictTransTables == ModeStrictTransTables || m&ModeStrictAllTables == ModeStrictAllTables
}

// HasPipesAsConcatMode detects if 'PIPES_AS_CONCAT' mode is set in SQLMode
func (m SQLMode) HasPipesAsConcatMode() bool {
	return m&ModePipesAsConcat == ModePipesAsConcat
}

// HasNoUnsignedSubtractionMode detects if 'NO_UNSIGNED_SUBTRACTION' mode is set in SQLMode
func (m SQLMode) HasNoUnsignedSubtractionMode() bool {
	return m&ModeNoUnsignedSubtraction == ModeNoUnsignedSubtraction
}

// HasHighNotPrecedenceMode detects if 'HIGH_NOT_PRECEDENCE' mode is set in SQLMode
func (m SQLMode) HasHighNotPrecedenceMode() bool {
	return m&ModeHighNotPrecedence == ModeHighNotPrecedence
}

// HasANSIQuotesMode detects if 'ANSI_QUOTES' mode is set in SQLMode
func (m SQLMode) HasANSIQuotesMode() bool {
	return m&ModeANSIQuotes == ModeANSIQuotes
}

// HasRealAsFloatMode detects if 'REAL_AS_FLOAT' mode is set in SQLMode
func (m SQLMode) HasRealAsFloatMode() bool {
	return m&ModeRealAsFloat == ModeRealAsFloat
}

// HasPadCharToFullLengthMode detects if 'PAD_CHAR_TO_FULL_LENGTH' mode is set in SQLMode
func (m SQLMode) HasPadCharToFullLengthMode() bool {
	return m&ModePadCharToFullLength == ModePadCharToFullLength
}

// HasNoBackslashEscapesMode detects if 'NO_BACKSLASH_ESCAPES' mode is set in SQLMode
func (m SQLMode) HasNoBackslashEscapesMode() bool {
	return m&ModeNoBackslashEscapes == ModeNoBackslashEscapes
}

// HasIgnoreSpaceMode detects if 'IGNORE_SPACE' mode is set in SQLMode
func (m SQLMode) HasIgnoreSpaceMode() bool {
	return m&ModeIgnoreSpace == ModeIgnoreSpace
}

// HasNoAutoCreateUserMode detects if 'NO_AUTO_CREATE_USER' mode is set in SQLMode
func (m SQLMode) HasNoAutoCreateUserMode() bool {
	return m&ModeNoAutoCreateUser == ModeNoAutoCreateUser
}

// HasAllowInvalidDatesMode detects if 'ALLOW_INVALID_DATES' mode is set in SQLMode
func (m SQLMode) HasAllowInvalidDatesMode() bool {
	return m&ModeAllowInvalidDates == ModeAllowInvalidDates
}

// consts for sql modes.
// see https://dev.mysql.com/doc/internals/en/query-event.html#q-sql-mode-code
const (
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
	ModeAllowInvalidDates
	ModeNone = 0
)

// FormatSQLModeStr re-format 'SQL_MODE' variable.
func FormatSQLModeStr(s string) string {
	s = strings.ToUpper(strings.TrimRight(s, " "))
	parts := strings.Split(s, ",")
	var nonEmptyParts []string
	existParts := make(map[string]string)
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		if modeParts, ok := CombinationSQLMode[part]; ok {
			for _, modePart := range modeParts {
				if _, exist := existParts[modePart]; !exist {
					nonEmptyParts = append(nonEmptyParts, modePart)
					existParts[modePart] = modePart
				}
			}
		}
		if _, exist := existParts[part]; !exist {
			nonEmptyParts = append(nonEmptyParts, part)
			existParts[part] = part
		}
	}
	return strings.Join(nonEmptyParts, ",")
}

// GetSQLMode gets the sql mode for string literal. SQL_mode is a list of different modes separated by commas.
// The input string must be formatted by 'FormatSQLModeStr'
func GetSQLMode(s string) (SQLMode, error) {
	strs := strings.Split(s, ",")
	var sqlMode SQLMode
	for i, length := 0, len(strs); i < length; i++ {
		mode, ok := Str2SQLMode[strs[i]]
		if !ok && strs[i] != "" {
			return sqlMode, newInvalidModeErr(strs[i])
		}
		sqlMode = sqlMode | mode
	}
	return sqlMode, nil
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
	"ALLOW_INVALID_DATES":        ModeAllowInvalidDates,
}

// CombinationSQLMode is the special modes that provided as shorthand for combinations of mode values.
// See https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-combo.
var CombinationSQLMode = map[string][]string{
	"ANSI":        {"REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "ONLY_FULL_GROUP_BY"},
	"DB2":         {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS"},
	"MAXDB":       {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "NO_AUTO_CREATE_USER"},
	"MSSQL":       {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS"},
	"MYSQL323":    {"MYSQL323", "HIGH_NOT_PRECEDENCE"},
	"MYSQL40":     {"MYSQL40", "HIGH_NOT_PRECEDENCE"},
	"ORACLE":      {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "NO_AUTO_CREATE_USER"},
	"POSTGRESQL":  {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS"},
	"TRADITIONAL": {"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE", "NO_ZERO_DATE", "ERROR_FOR_DIVISION_BY_ZERO", "NO_AUTO_CREATE_USER", "NO_ENGINE_SUBSTITUTION"},
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

// PriorityEnum is defined for Priority const values.
type PriorityEnum int

// Priority const values.
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
const (
	NoPriority PriorityEnum = iota
	LowPriority
	HighPriority
	DelayedPriority
)

// Priority2Str is used to convert the statement priority to string.
var Priority2Str = map[PriorityEnum]string{
	NoPriority:      "NO_PRIORITY",
	LowPriority:     "LOW_PRIORITY",
	HighPriority:    "HIGH_PRIORITY",
	DelayedPriority: "DELAYED",
}

// Str2Priority is used to convert a string to a priority.
func Str2Priority(val string) PriorityEnum {
	val = strings.ToUpper(val)
	switch val {
	case "NO_PRIORITY":
		return NoPriority
	case "HIGH_PRIORITY":
		return HighPriority
	case "LOW_PRIORITY":
		return LowPriority
	case "DELAYED":
		return DelayedPriority
	default:
		return NoPriority
	}
}

// Restore implements Node interface.
func (n *PriorityEnum) Restore(ctx *format.RestoreCtx) error {
	switch *n {
	case NoPriority:
		return nil
	case LowPriority:
		ctx.WriteKeyWord("LOW_PRIORITY")
	case HighPriority:
		ctx.WriteKeyWord("HIGH_PRIORITY")
	case DelayedPriority:
		ctx.WriteKeyWord("DELAYED")
	default:
		return errors.Errorf("undefined PriorityEnum Type[%d]", *n)
	}
	return nil
}

// PrimaryKeyName defines primary key name.
const (
	PrimaryKeyName = "PRIMARY"
)
