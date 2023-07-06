// Copyright 2022 PingCAP, Inc.
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

package extension

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// FunctionContext is an interface to provide context to the custom function
type FunctionContext interface {
	context.Context
	User() *auth.UserIdentity
	ActiveRoles() []*auth.RoleIdentity
	CurrentDB() string
	ConnectionInfo() *variable.ConnectionInfo
	EvalArgs(row chunk.Row) ([]types.Datum, error)
}

// FunctionDef is the definition for the custom function
type FunctionDef struct {
	// Name is the function's name
	Name string
	// EvalTp is the type of the return value
	EvalTp types.EvalType
	// ArgTps is the argument types
	ArgTps []types.EvalType
	// OptionalArgsLen is the length of the optional args
	OptionalArgsLen int
	// EvalStringFunc is the eval function when `EvalTp` is `types.ETString`
	EvalStringFunc func(ctx FunctionContext, row chunk.Row) (string, bool, error)
	// EvalIntFunc is the eval function when `EvalTp` is `types.ETInt`
	EvalIntFunc func(ctx FunctionContext, row chunk.Row) (int64, bool, error)
	// RequireDynamicPrivileges is a function to return a list of dynamic privileges to check.
	RequireDynamicPrivileges func(sem bool) []string
}

// Validate validates the function definition
func (def *FunctionDef) Validate() error {
	if def.Name == "" {
		return errors.New("extension function name should not be empty")
	}

	for def.OptionalArgsLen < 0 || def.OptionalArgsLen > len(def.ArgTps) {
		return errors.Errorf("invalid OptionalArgsLen: %d", def.OptionalArgsLen)
	}

	return nil
}

// RegisterExtensionFunc is to avoid dependency cycle
var RegisterExtensionFunc func(*FunctionDef) error

// RemoveExtensionFunc is to avoid dependency cycle
var RemoveExtensionFunc func(string)
