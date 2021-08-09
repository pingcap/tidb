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
	"github.com/pingcap/tidb/util/dbterror"
)

var (
	errInvalidPluginID         = dbterror.ClassPlugin.NewStd(errno.ErrInvalidPluginID)
	errInvalidPluginManifest   = dbterror.ClassPlugin.NewStd(errno.ErrInvalidPluginManifest)
	errInvalidPluginName       = dbterror.ClassPlugin.NewStd(errno.ErrInvalidPluginName)
	errInvalidPluginVersion    = dbterror.ClassPlugin.NewStd(errno.ErrInvalidPluginVersion)
	errDuplicatePlugin         = dbterror.ClassPlugin.NewStd(errno.ErrDuplicatePlugin)
	errRequireVersionCheckFail = dbterror.ClassPlugin.NewStd(errno.ErrRequireVersionCheckFail)
)
