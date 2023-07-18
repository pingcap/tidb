// Copyright 2022 PingCAP, Inc.
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

package generator

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	tidb_types "github.com/pingcap/tidb/types"
)

// GenConfig is a config of Gen
type GenConfig struct {
	IsInExprIndex        bool
	EnableLeftRightJoin  bool
	IsInUpdateDeleteStmt bool
	IsPQSMode            bool
	IsNoRECMode          bool
}

// GenCtx is context for generator
type GenCtx struct {
	GenConfig

	tmpTableIndex   int
	tmpColIndex     int
	UsedTables      []types.Table
	ResultTables    []types.Table
	TableAlias      map[string]string
	PivotRows       map[string]*connection.QueryItem
	unwrapPivotRows map[string]interface{}
}

// NewGenCtx is to create a new GenContext
func NewGenCtx(usedTables []types.Table, pivotRows map[string]*connection.QueryItem) *GenCtx {
	row := map[string]interface{}{}
	for key, value := range pivotRows {
		row[key], _ = getTypedValue(value)
	}
	return &GenCtx{
		tmpTableIndex:   0,
		tmpColIndex:     0,
		UsedTables:      usedTables,
		ResultTables:    make([]types.Table, 0),
		TableAlias:      make(map[string]string),
		PivotRows:       pivotRows,
		unwrapPivotRows: row,
		GenConfig: GenConfig{
			IsInExprIndex:        false,
			EnableLeftRightJoin:  true,
			IsInUpdateDeleteStmt: false,
			IsNoRECMode:          false,
			IsPQSMode:            false,
		},
	}
}

func (g *GenCtx) createTmpTable() string {
	g.tmpTableIndex++
	return fmt.Sprintf("tmp%d", g.tmpTableIndex)
}

func (g *GenCtx) createTmpColumn() string {
	g.tmpColIndex++
	return fmt.Sprintf("col_%d", g.tmpColIndex)
}

func (g *GenCtx) findUsedTableByName(name string) *types.Table {
	for _, table := range g.UsedTables {
		if table.Name.EqString(name) {
			t := table.Clone()
			return &t
		}
	}
	return nil
}

func getTypedValue(it *connection.QueryItem) (interface{}, byte) {
	if it.Null {
		return nil, mysql.TypeNull
	}
	switch it.ValType.DatabaseTypeName() {
	case "VARCHAR", "TEXT", "CHAR":
		return it.ValString, mysql.TypeString
	case "INT", "BIGINT", "TINYINT":
		i, _ := strconv.ParseInt(it.ValString, 10, 64)
		return i, mysql.TypeLong
	case "TIMESTAMP", "DATE", "DATETIME":
		t, _ := time.Parse("2006-01-02 15:04:05", it.ValString)
		return tidb_types.NewTime(tidb_types.FromGoTime(t), mysql.TypeTimestamp, 6), mysql.TypeDatetime
	case "FLOAT", "DOUBLE", "DECIMAL":
		f, _ := strconv.ParseFloat(it.ValString, 64)
		return f, mysql.TypeDouble
	default:
		panic(fmt.Sprintf("unreachable type %s", it.ValType.DatabaseTypeName()))
	}
}
