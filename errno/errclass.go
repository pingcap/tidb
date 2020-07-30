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

package errno

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	RegDB           = errors.NewRegistry("DB")
	ClassAutoid     = RegDB.RegisterErrorClass(1, "autoid")
	ClassDDL        = RegDB.RegisterErrorClass(2, "ddl")
	ClassDomain     = RegDB.RegisterErrorClass(3, "domain")
	ClassEvaluator  = RegDB.RegisterErrorClass(4, "evaluator")
	ClassExecutor   = RegDB.RegisterErrorClass(5, "executor")
	ClassExpression = RegDB.RegisterErrorClass(6, "expression")
	ClassAdmin      = RegDB.RegisterErrorClass(7, "admin")
	ClassKV         = RegDB.RegisterErrorClass(8, "kv")
	ClassMeta       = RegDB.RegisterErrorClass(9, "meta")
	ClassOptimizer  = RegDB.RegisterErrorClass(10, "planner")
	ClassParser     = RegDB.RegisterErrorClass(11, "parser")
	ClassPerfSchema = RegDB.RegisterErrorClass(12, "perfschema")
	ClassPrivilege  = RegDB.RegisterErrorClass(13, "privilege")
	ClassSchema     = RegDB.RegisterErrorClass(14, "schema")
	ClassServer     = RegDB.RegisterErrorClass(15, "server")
	ClassStructure  = RegDB.RegisterErrorClass(16, "structure")
	ClassVariable   = RegDB.RegisterErrorClass(17, "variable")
	ClassXEval      = RegDB.RegisterErrorClass(18, "xeval")
	ClassTable      = RegDB.RegisterErrorClass(19, "table")
	ClassTypes      = RegDB.RegisterErrorClass(20, "types")
	ClassGlobal     = RegDB.RegisterErrorClass(21, "global")
	ClassMockTikv   = RegDB.RegisterErrorClass(22, "mocktikv")
	ClassJSON       = RegDB.RegisterErrorClass(23, "json")
	ClassTiKV       = RegDB.RegisterErrorClass(24, "tikv")
	ClassSession    = RegDB.RegisterErrorClass(25, "session")
	ClassPlugin     = RegDB.RegisterErrorClass(26, "plugin")
	ClassUtil       = RegDB.RegisterErrorClass(27, "util")
)


// Log logs the error if it is not nil.
func Log(err error) {
	if err != nil {
		log.Error("encountered error", zap.Error(errors.WithStack(err)))
	}
}
