// Copyright 2024 PingCAP, Inc.
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

package resolve

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
)

// ResultField represents a result field which can be a column from a table,
// or an expression in select field. It is a generated property during
// binding process. ResultField is the key element to evaluate a ColumnNameExpr.
// After resolving process, every ColumnNameExpr will be resolved to a ResultField.
// During execution, every row retrieved from table will set the row value to
// ResultFields of that table, so ColumnNameExpr resolved to that ResultField can be
// easily evaluated.
type ResultField struct {
	Column       *model.ColumnInfo
	ColumnAsName pmodel.CIStr
	// EmptyOrgName indicates whether this field has an empty org_name. A field has an empty org name, if it's an
	// expression. It's not sure whether it's safe to use empty string in `.Column.Name`, so a new field is added to
	// indicate whether it's empty.
	EmptyOrgName bool

	Table       *model.TableInfo
	TableAsName pmodel.CIStr
	DBName      pmodel.CIStr
}
