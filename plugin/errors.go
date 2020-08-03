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
	"github.com/pingcap/tidb/errno"
	terror "github.com/pingcap/tidb/errno"
)

var (
	errInvalidPluginID         = terror.ClassPlugin.New(errno.ErrInvalidPluginID, errno.MySQLErrName[errno.ErrInvalidPluginID])
	errInvalidPluginManifest   = terror.ClassPlugin.New(errno.ErrInvalidPluginManifest, errno.MySQLErrName[errno.ErrInvalidPluginManifest])
	errInvalidPluginName       = terror.ClassPlugin.New(errno.ErrInvalidPluginName, errno.MySQLErrName[errno.ErrInvalidPluginName])
	errInvalidPluginVersion    = terror.ClassPlugin.New(errno.ErrInvalidPluginVersion, errno.MySQLErrName[errno.ErrInvalidPluginVersion])
	errDuplicatePlugin         = terror.ClassPlugin.New(errno.ErrDuplicatePlugin, errno.MySQLErrName[errno.ErrDuplicatePlugin])
	errInvalidPluginSysVarName = terror.ClassPlugin.New(errno.ErrInvalidPluginSysVarName, errno.MySQLErrName[errno.ErrInvalidPluginSysVarName])
	errRequireVersionCheckFail = terror.ClassPlugin.New(errno.ErrRequireVersionCheckFail, errno.MySQLErrName[errno.ErrRequireVersionCheckFail])
)
