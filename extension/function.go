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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type FunctionContext interface {
	EvalArgs(row chunk.Row) ([]types.Datum, error)
}

type FunctionDef struct {
	Name           string
	EvalTp         types.EvalType
	ArgTps         []types.EvalType
	EvalStringFunc func(ctx FunctionContext, row chunk.Row) (string, bool, error)
	EvalIntFunc    func(ctx FunctionContext, row chunk.Row) (int64, bool, error)
}

var RegisterExtensionFunc func(*FunctionDef) error
var RemoveExtensionFunc func(string)
