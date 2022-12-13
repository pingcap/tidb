// Copyright 2020 PingCAP, Inc.
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

package dbterror

import (
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
)

// ErrClass represents a class of errors.
type ErrClass struct{ terror.ErrClass }

// Error classes.
var (
	ClassAutoid     = ErrClass{terror.ClassAutoid}
	ClassDDL        = ErrClass{terror.ClassDDL}
	ClassDomain     = ErrClass{terror.ClassDomain}
	ClassExecutor   = ErrClass{terror.ClassExecutor}
	ClassExpression = ErrClass{terror.ClassExpression}
	ClassAdmin      = ErrClass{terror.ClassAdmin}
	ClassKV         = ErrClass{terror.ClassKV}
	ClassMeta       = ErrClass{terror.ClassMeta}
	ClassOptimizer  = ErrClass{terror.ClassOptimizer}
	ClassPrivilege  = ErrClass{terror.ClassPrivilege}
	ClassSchema     = ErrClass{terror.ClassSchema}
	ClassServer     = ErrClass{terror.ClassServer}
	ClassStructure  = ErrClass{terror.ClassStructure}
	ClassVariable   = ErrClass{terror.ClassVariable}
	ClassXEval      = ErrClass{terror.ClassXEval}
	ClassTable      = ErrClass{terror.ClassTable}
	ClassTypes      = ErrClass{terror.ClassTypes}
	ClassJSON       = ErrClass{terror.ClassJSON}
	ClassTiKV       = ErrClass{terror.ClassTiKV}
	ClassSession    = ErrClass{terror.ClassSession}
	ClassPlugin     = ErrClass{terror.ClassPlugin}
	ClassUtil       = ErrClass{terror.ClassUtil}
)

// NewStd calls New using the standard message for the error code
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) NewStd(code terror.ErrCode) *terror.Error {
	return ec.NewStdErr(code, errno.MySQLErrName[uint16(code)])
}
