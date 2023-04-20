// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"strings"
	"sync"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// sessionProcedureContext use in session.
type sessionProcedureContext struct {
	// lock is for user defined variables. values and types is read/write protected.
	lock    sync.RWMutex
	context *ProcedureContext
}

// ProcedureContext procedure environment variable.
type ProcedureContext struct {
	root *ProcedureContext
	// save procedure variable.
	Vars []*ProcedureVars
}

// ProcedureVars procedure variable struct.
type ProcedureVars struct {
	name  string
	field *types.FieldType
	vars  types.Datum
}

// GetVariableVars get value by name from ProcedureContext.
func (context *ProcedureContext) GetVariableVars(name string) (*types.FieldType, types.Datum, bool) {
	if context == nil {
		return nil, types.NewDatum(""), true
	}
	name = strings.ToLower(name)
	for _, procedureVar := range context.Vars {
		if procedureVar.name == name {
			return procedureVar.field, procedureVar.vars, false
		}
	}
	if context.root != nil {
		return context.root.GetVariableVars(name)
	}
	return nil, types.NewDatum(""), true
}

// UpdateVariableVars update variable value.
func (context *ProcedureContext) UpdateVariableVars(name string, val types.Datum, stmtCtx *stmtctx.StatementContext) error {
	if context == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	name = strings.ToLower(name)
	for _, procedureVar := range context.Vars {
		if procedureVar.name == name {
			varVar, err := val.Clone().ConvertTo(stmtCtx, procedureVar.field)
			if err != nil {
				return err
			}
			procedureVar.vars = varVar
			return nil
		}
	}
	if context.root != nil {
		return context.root.UpdateVariableVars(name, val, stmtCtx)
	}
	return ErrUnknownSystemVar.GenWithStackByArgs(name)
}

// GetVariableVars get value by name from SessionVars.
func (s *SessionVars) GetVariableVars(name string) (*types.FieldType, types.Datum, bool) {
	if !s.inCallProcedure {
		return nil, types.NewDatum(""), true
	}
	s.procedureContext.lock.Lock()
	defer s.procedureContext.lock.Unlock()
	return s.procedureContext.context.GetVariableVars(name)
}
