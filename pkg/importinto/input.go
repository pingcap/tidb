// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importinto

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// Option is a resolved IMPORT option. SQL planning guarantees a constant value;
// flag options have no value. Options are transient preparation input, not task metadata.
type Option struct {
	Name  string
	Value *expression.Constant
}

// Input contains the resolved metadata and arguments needed to prepare an import.
// Name resolution, privilege checks and SELECT planning belong to the caller.
// Unlike Plan, Input is transient and may contain AST nodes and checked constants.
type Input struct {
	DBName             string
	DBID               int64
	TableInfo          *model.TableInfo
	Path               string
	Format             *string
	DataSourceType     DataSourceType
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	ColumnAssignments  []*ast.Assignment
	Options            []*Option
}
