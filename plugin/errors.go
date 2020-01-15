// Copyright 2019 PingCAP, Inc.
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

package plugin

import (
	pterror "github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

var (
	errInvalidPluginID         = createPluginError(mysql.ErrInvalidPluginID)
	errInvalidPluginManifest   = createPluginError(mysql.ErrInvalidPluginManifest)
	errInvalidPluginName       = createPluginError(mysql.ErrInvalidPluginName)
	errInvalidPluginVersion    = createPluginError(mysql.ErrInvalidPluginVersion)
	errDuplicatePlugin         = createPluginError(mysql.ErrDuplicatePlugin)
	errInvalidPluginSysVarName = createPluginError(mysql.ErrInvalidPluginSysVarName)
	errRequireVersionCheckFail = createPluginError(mysql.ErrRequireVersionCheckFail)
)

func createPluginError(code pterror.ErrCode) *terror.TError {
	return terror.New(pterror.ClassPlugin, code, mysql.MySQLErrName[uint16(code)])
}

func init() {
	pluginMySQLErrCodes := map[pterror.ErrCode]uint16{
		mysql.ErrInvalidPluginID:            mysql.ErrInvalidPluginID,
		mysql.ErrInvalidPluginManifest:      mysql.ErrInvalidPluginManifest,
		mysql.ErrInvalidPluginName:          mysql.ErrInvalidPluginName,
		mysql.ErrInvalidPluginVersion:       mysql.ErrInvalidPluginVersion,
		mysql.ErrDuplicatePlugin:            mysql.ErrDuplicatePlugin,
		mysql.ErrInvalidPluginSysVarName:    mysql.ErrInvalidPluginSysVarName,
		mysql.ErrRequireVersionCheckFail:    mysql.ErrRequireVersionCheckFail,
		mysql.ErrUnsupportedReloadPlugin:    mysql.ErrUnsupportedReloadPlugin,
		mysql.ErrUnsupportedReloadPluginVar: mysql.ErrUnsupportedReloadPluginVar,
	}
	terror.ErrClassToMySQLCodes[pterror.ClassPlugin] = pluginMySQLErrCodes
}
