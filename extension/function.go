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

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// FunctionContext is a interface to provide context to the custom function
type FunctionContext interface {
	context.Context
	EvalArgs(row chunk.Row) ([]types.Datum, error)
}

// FunctionDef is the definition for the custom function
type FunctionDef struct {
	Name   string
	EvalTp types.EvalType
	ArgTps []types.EvalType
	// EvalStringFunc is the eval function when `EvalTp` is `types.ETString`
	EvalStringFunc func(ctx FunctionContext, row chunk.Row) (string, bool, error)
	// EvalIntFunc is the eval function when `EvalTp` is `types.ETInt`
	EvalIntFunc func(ctx FunctionContext, row chunk.Row) (int64, bool, error)
	// RequireDynamicPrivileges is the dynamic privileges needed to invoke the function
	// If `RequireDynamicPrivileges` is empty, it means every one can invoke this function
	RequireDynamicPrivileges []string
	// SemRequireDynamicPrivileges is the dynamic privileges needed to invoke the function in sem mode
	// If `SemRequireDynamicPrivileges` is empty, `DynamicPrivileges` will be used in sem mode
	SemRequireDynamicPrivileges []string
}

// RegisterExtensionFunc is to avoid dependency cycle
var RegisterExtensionFunc func(*FunctionDef) error

// RemoveExtensionFunc is to avoid dependency cycle
var RemoveExtensionFunc func(string)
