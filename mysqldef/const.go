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

package mysqldef

// Version informations.
const (
	MinProtocolVersion byte   = 10
	MaxPayloadLen      int    = 1<<24 - 1
	ServerVersion      string = "5.5.31-TiDB-1.0"
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
	// ShowPriv is the privilege to run show statement.
	ShowPriv
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
)
