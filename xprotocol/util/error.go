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

package util

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
)

// Error messages
var (
	ErrXBadMessage                = ErrorMessage(mysql.ErrXBadMessage, mysql.MySQLErrName[mysql.ErrXBadMessage])
	ErrXCapabilitiesPrepareFailed = ErrorMessage(mysql.ErrXCapabilitiesPrepareFailed, mysql.MySQLErrName[mysql.ErrXCapabilitiesPrepareFailed])
	ErrAccessDenied               = ErrorMessage(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDenied])
	ErrXBadSchema                 = ErrorMessage(mysql.ErrXBadSchema, mysql.MySQLErrName[mysql.ErrXBadSchema])
	ErrXBadTable                  = ErrorMessage(mysql.ErrXBadTable, mysql.MySQLErrName[mysql.ErrXBadTable])
	ErrTableExists                = ErrorMessage(mysql.ErrTableExists, mysql.MySQLErrName[mysql.ErrTableExists])
	ErrXInvalidCollection         = ErrorMessage(mysql.ErrXInvalidCollection, mysql.MySQLErrName[mysql.ErrXInvalidCollection])
	ErrJSONUsedAsKey              = ErrorMessage(mysql.ErrJSONUsedAsKey, mysql.MySQLErrName[mysql.ErrJSONUsedAsKey])
	ErrXBadNotice                 = ErrorMessage(mysql.ErrXBadNotice, mysql.MySQLErrName[mysql.ErrXBadNotice])
	ErrXInvalidNamespace          = ErrorMessage(mysql.ErrXInvalidNamespace, mysql.MySQLErrName[mysql.ErrXInvalidNamespace])
	ErrXInvalidAdminCommand       = ErrorMessage(mysql.ErrXInvalidAdminCommand, mysql.MySQLErrName[mysql.ErrXInvalidAdminCommand])
	ErrXCmdNumArguments           = ErrorMessage(mysql.ErrXCmdNumArguments, mysql.MySQLErrName[mysql.ErrXCmdNumArguments])
	ErrXCmdArgumentType           = ErrorMessage(mysql.ErrXCmdArgumentType, mysql.MySQLErrName[mysql.ErrXCmdArgumentType])
	ErrXCannotDisableNotice       = ErrorMessage(mysql.ErrXCannotDisableNotice, mysql.MySQLErrName[mysql.ErrXCannotDisableNotice])
	ErrNotSupportedAuthMode       = ErrorMessage(mysql.ErrNotSupportedAuthMode, mysql.MySQLErrName[mysql.ErrNotSupportedAuthMode])
	ErrXExprBadNumArgs            = ErrorMessage(mysql.ErrXExprBadNumArgs, mysql.MySQLErrName[mysql.ErrXExprBadNumArgs])
	ErrXExprBadOperator           = ErrorMessage(mysql.ErrXExprBadOperator, mysql.MySQLErrName[mysql.ErrXExprBadOperator])
	ErrXExprBadTypeValue          = ErrorMessage(mysql.ErrXExprBadTypeValue, mysql.MySQLErrName[mysql.ErrXExprBadTypeValue])
	ErrXExprBadValue              = ErrorMessage(mysql.ErrXExprBadValue, mysql.MySQLErrName[mysql.ErrXExprBadValue])
	ErrXInvalidArgument           = ErrorMessage(mysql.ErrXInvalidArgument, mysql.MySQLErrName[mysql.ErrXInvalidArgument])
	ErrXProjBadKeyName            = ErrorMessage(mysql.ErrXProjBadKeyName, mysql.MySQLErrName[mysql.ErrXProjBadKeyName])
	ErrXBadProjection             = ErrorMessage(mysql.ErrXBadProjection, mysql.MySQLErrName[mysql.ErrXBadProjection])
	ErrXBadUpdateData             = ErrorMessage(mysql.ErrXBadUpdateData, mysql.MySQLErrName[mysql.ErrXBadUpdateData])
	ErrXBadColumnToUpdate         = ErrorMessage(mysql.ErrXBadColumnToUpdate, mysql.MySQLErrName[mysql.ErrXBadColumnToUpdate])
	ErrXBadTypeOfUpdate           = ErrorMessage(mysql.ErrXBadTypeOfUpdate, mysql.MySQLErrName[mysql.ErrXBadTypeOfUpdate])
	ErrXBadMemberToUpdate         = ErrorMessage(mysql.ErrXBadMemberToUpdate, mysql.MySQLErrName[mysql.ErrXBadMemberToUpdate])
)

// Error Codes
const (
	codeErrXBadMessage                terror.ErrCode = terror.ErrCode(mysql.ErrXBadMessage)
	codeErrXAccessDenied                             = terror.ErrCode(mysql.ErrAccessDenied)
	codeErrXBadSchema                                = terror.ErrCode(mysql.ErrXBadSchema)
	codeErrXBadTable                                 = terror.ErrCode(mysql.ErrXBadTable)
	codeErrTableExists                               = terror.ErrCode(mysql.ErrTableExists)
	codeErrXInvalidCollection                        = terror.ErrCode(mysql.ErrXInvalidCollection)
	codeErrJSONUsedAsKey                             = terror.ErrCode(mysql.ErrJSONUsedAsKey)
	codeErrXBadNotice                                = terror.ErrCode(mysql.ErrXBadNotice)
	codeErrXCapabilitiesPrepareFailed                = terror.ErrCode(mysql.ErrXCapabilitiesPrepareFailed)
	CodeErrXBadProjection                            = terror.ErrCode(mysql.ErrXBadProjection)
	CodeErrXBadInsertData                            = terror.ErrCode(mysql.ErrXBadInsertData)
	CodeErrXExprMissingArg                           = terror.ErrCode(mysql.ErrXExprMissingArg)
	codeErrXInvalidNamespace                         = terror.ErrCode(mysql.ErrXInvalidNamespace)
	codeErrXInvalidAdminCommand                      = terror.ErrCode(mysql.ErrXInvalidAdminCommand)
	codeErrXCmdNumArguments                          = terror.ErrCode(mysql.ErrXCmdNumArguments)
	codeErrXCmdArgumentType                          = terror.ErrCode(mysql.ErrXCmdArgumentType)
	codeErrXCannotDisableNotice                      = terror.ErrCode(mysql.ErrXCannotDisableNotice)
	codeErrNotSupportedAuthMode                      = terror.ErrCode(mysql.ErrNotSupportedAuthMode)
	CodeErrXExprBadTypeValue                         = terror.ErrCode(mysql.ErrXExprBadTypeValue)
	CodeErrXExprBadValue                             = terror.ErrCode(mysql.ErrXExprBadValue)
	codeErrXExprBadNumArgs                           = terror.ErrCode(mysql.ErrXExprBadNumArgs)
	codeErrXExprBadOperator                          = terror.ErrCode(mysql.ErrXExprBadOperator)
	codeErrXInvalidArgument                          = terror.ErrCode(mysql.ErrXInvalidArgument)
	codeErrXProjBadKeyName                           = terror.ErrCode(mysql.ErrXProjBadKeyName)
	codeErrXBadUpdateData                            = terror.ErrCode(mysql.ErrXBadUpdateData)
	codeErrXBadColumnToUpdate                        = terror.ErrCode(mysql.ErrXBadColumnToUpdate)
	codeErrXBadTypeOfUpdate                          = terror.ErrCode(mysql.ErrXBadTypeOfUpdate)
	codeErrXBadMemberToUpdate                        = terror.ErrCode(mysql.ErrXBadMemberToUpdate)
)

func init() {
	xProtocolMySQLErrCodes := map[terror.ErrCode]uint16{
		codeErrXBadMessage:                mysql.ErrXBadMessage,
		codeErrXCapabilitiesPrepareFailed: mysql.ErrXCapabilitiesPrepareFailed,
		codeErrXAccessDenied:              mysql.ErrAccessDenied,
		codeErrXBadSchema:                 mysql.ErrXBadSchema,
		codeErrXBadTable:                  mysql.ErrXBadTable,
		codeErrTableExists:                mysql.ErrTableExists,
		codeErrXInvalidCollection:         mysql.ErrXInvalidCollection,
		codeErrJSONUsedAsKey:              mysql.ErrJSONUsedAsKey,
		codeErrXBadNotice:                 mysql.ErrXBadNotice,
		CodeErrXBadProjection:             mysql.ErrXBadProjection,
		CodeErrXBadInsertData:             mysql.ErrXBadInsertData,
		CodeErrXExprMissingArg:            mysql.ErrXExprMissingArg,
		codeErrXInvalidNamespace:          mysql.ErrXInvalidNamespace,
		codeErrXInvalidAdminCommand:       mysql.ErrXInvalidAdminCommand,
		codeErrXCmdNumArguments:           mysql.ErrXCmdNumArguments,
		codeErrXCmdArgumentType:           mysql.ErrXCmdArgumentType,
		codeErrXCannotDisableNotice:       mysql.ErrXCannotDisableNotice,
		codeErrNotSupportedAuthMode:       mysql.ErrNotSupportedAuthMode,
		CodeErrXExprBadTypeValue:          mysql.ErrXExprBadTypeValue,
		CodeErrXExprBadValue:              mysql.ErrXExprBadValue,
		codeErrXExprBadNumArgs:            mysql.ErrXExprBadNumArgs,
		codeErrXExprBadOperator:           mysql.ErrXExprBadOperator,
		codeErrXInvalidArgument:           mysql.ErrXInvalidArgument,
		codeErrXProjBadKeyName:            mysql.ErrXProjBadKeyName,
		codeErrXBadUpdateData:             mysql.ErrXBadUpdateData,
		codeErrXBadColumnToUpdate:         mysql.ErrXBadColumnToUpdate,
		codeErrXBadTypeOfUpdate:           mysql.ErrXBadTypeOfUpdate,
		codeErrXBadMemberToUpdate:         mysql.ErrXBadMemberToUpdate,
	}
	terror.ErrClassToMySQLCodes[terror.ClassXProtocol] = xProtocolMySQLErrCodes
}

// ErrorMessage returns terror Error.
func ErrorMessage(code terror.ErrCode, msg string) *terror.Error {
	return terror.ClassXProtocol.New(code, msg)
}

// XErrorMessage returns Mysqlx Error.
func XErrorMessage(errcode uint16, msg string, state string) *Mysqlx.Error {
	code := uint32(errcode)
	sqlState := state
	errMsg := Mysqlx.Error{
		Severity: Mysqlx.Error_ERROR.Enum(),
		Code:     &code,
		SqlState: &sqlState,
		Msg:      &msg,
	}
	return &errMsg
}

// SendOK is used to send server message OK.
func SendOK(pkt *xpacketio.XPacketIO, content string) error {
	msg := Mysqlx.Ok{
		Msg: &content,
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	if err := pkt.WritePacket(Mysqlx.ServerMessages_OK, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}
