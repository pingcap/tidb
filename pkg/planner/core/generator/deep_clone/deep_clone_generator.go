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

package main

import (
	"bytes"
	"fmt"
	"go/format"
	"log"
	"os"
	"reflect"

	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// GenDeepClone4LogicalOps generates DeepClone() for logical operators.
func GenDeepClone4LogicalOps() ([]byte, error) {
	structures := []any{
		logicalop.LogicalSchemaProducer{},
		logicalop.LogicalJoin{}, logicalop.LogicalAggregation{}, logicalop.LogicalProjection{}, logicalop.LogicalSelection{},
		logicalop.LogicalApply{}, logicalop.LogicalMaxOneRow{}, logicalop.LogicalTableDual{}, logicalop.DataSource{},
		logicalop.TiKVSingleGather{}, logicalop.LogicalTableScan{}, logicalop.LogicalIndexScan{}, logicalop.LogicalUnionAll{},
		logicalop.LogicalPartitionUnionAll{}, logicalop.LogicalSort{}, logicalop.LogicalLock{}, logicalop.LogicalLimit{},
		logicalop.LogicalWindow{}, logicalop.LogicalExpand{}, logicalop.LogicalUnionScan{}, logicalop.LogicalMemTable{},
		logicalop.LogicalShow{}, logicalop.LogicalShowDDLJobs{}, logicalop.LogicalCTE{}, logicalop.LogicalCTETable{},
		logicalop.LogicalSequence{}, logicalop.LogicalTopN{}, logicalop.MockDataSource{},
	}
	c := new(codeGen)
	c.write(codeGenDeepClonePrefix)
	for _, s := range structures {
		code, err := genDeepClone4OneLogicalOp(s)
		if err != nil {
			return nil, err
		}
		c.write("%s", string(code))
	}
	return c.format()
}

func genDeepClone4OneLogicalOp(x any) ([]byte, error) {
	c := new(codeGen)
	vType := reflect.TypeOf(x)
	typeName := vType.Name()
	c.write("// DeepClone implements base.LogicalPlan.")
	c.write("func (op *%v) DeepClone() base.LogicalPlan {", typeName)
	c.write("if op == nil {")
	c.write("return nil")
	c.write("}")
	c.write("cloned := new(%v)", typeName)
	c.write("*cloned = *op")
	for i := range vType.NumField() {
		f := vType.Field(i)
		fieldName := f.Name
		switch {
		case f.Type.String() == "logicalop.BaseLogicalPlan":
			c.write("cloned.%v = cloneBaseLogicalPlan(&op.%v, cloned)", fieldName, fieldName)
			continue
		case f.Type.String() == "logicalop.LogicalSchemaProducer":
			c.write("cloned.%v = cloneLogicalSchemaProducer(&op.%v, cloned)", fieldName, fieldName)
			continue
		case f.Type.String() == "logicalop.LogicalJoin":
			c.write("cloned.%v = cloneLogicalJoinAs(&op.%v, cloned)", fieldName, fieldName)
			continue
		case f.Type.String() == "logicalop.LogicalUnionAll":
			c.write("cloned.%v = cloneLogicalUnionAllAs(&op.%v, cloned)", fieldName, fieldName)
			continue
		}
		if codeLine, ok := cloneCodeByFieldType(f.Type.String(), fieldName); ok {
			c.write("%s", codeLine)
		}
	}
	c.write("return cloned")
	c.write("}")
	return c.format()
}

func cloneCodeByFieldType(fieldType, fieldName string) (string, bool) {
	switch fieldType {
	case "[]expression.Expression":
		return fmt.Sprintf("cloned.%s = cloneExprSlice(op.%s)", fieldName, fieldName), true
	case "expression.CNFExprs":
		return fmt.Sprintf("cloned.%s = cloneCNFExprs(op.%s)", fieldName, fieldName), true
	case "[][]expression.Expression":
		return fmt.Sprintf("cloned.%s = cloneExpr2DSlice(op.%s)", fieldName, fieldName), true
	case "[]*expression.ScalarFunction":
		return fmt.Sprintf("cloned.%s = cloneScalarFunctionSlice(op.%s)", fieldName, fieldName), true
	case "[]*expression.CorrelatedColumn":
		return fmt.Sprintf("cloned.%s = cloneCorrelatedColumnSlice(op.%s)", fieldName, fieldName), true
	case "[]*aggregation.AggFuncDesc":
		return fmt.Sprintf("cloned.%s = cloneAggFuncDescSlice(op.%s)", fieldName, fieldName), true
	case "[]*aggregation.WindowFuncDesc":
		return fmt.Sprintf("cloned.%s = cloneWindowFuncDescSlice(op.%s)", fieldName, fieldName), true
	case "[]*expression.Column":
		return fmt.Sprintf("cloned.%s = cloneColumnSlice(op.%s)", fieldName, fieldName), true
	case "[][]*expression.Column":
		return fmt.Sprintf("cloned.%s = cloneColumn2DSlice(op.%s)", fieldName, fieldName), true
	case "*expression.Column":
		return fmt.Sprintf("cloned.%s = cloneColumnPtr(op.%s)", fieldName, fieldName), true
	case "*expression.Schema":
		return fmt.Sprintf("cloned.%s = cloneSchemaPtr(op.%s)", fieldName, fieldName), true
	case "[]*expression.Schema":
		return fmt.Sprintf("cloned.%s = cloneSchemaSlice(op.%s)", fieldName, fieldName), true
	case "[]*util.ByItems":
		return fmt.Sprintf("cloned.%s = cloneByItemsSlice(op.%s)", fieldName, fieldName), true
	case "[]property.SortItem":
		return fmt.Sprintf("cloned.%s = cloneSortItems(op.%s)", fieldName, fieldName), true
	case "[]*ranger.Range":
		return fmt.Sprintf("cloned.%s = cloneRangeSlice(op.%s)", fieldName, fieldName), true
	case "[]*util.AccessPath":
		return fmt.Sprintf("cloned.%s = cloneAccessPathSlice(op.%s)", fieldName, fieldName), true
	case "[]types.Datum":
		return fmt.Sprintf("cloned.%s = cloneDatumSlice(op.%s)", fieldName, fieldName), true
	case "[]*types.FieldName":
		return fmt.Sprintf("cloned.%s = cloneFieldNameSlice(op.%s)", fieldName, fieldName), true
	case "types.NameSlice":
		return fmt.Sprintf("cloned.%s = cloneNameSlice(op.%s)", fieldName, fieldName), true
	case "util.HandleCols":
		return fmt.Sprintf("cloned.%s = cloneHandleCols(op.%s)", fieldName, fieldName), true
	case "[]*model.ColumnInfo":
		return fmt.Sprintf("cloned.%s = cloneModelColumnInfoSlice(op.%s)", fieldName, fieldName), true
	case "map[int64]*expression.Column":
		return fmt.Sprintf("cloned.%s = cloneMapInt64Column(op.%s)", fieldName, fieldName), true
	case "map[int64][]util.HandleCols":
		return fmt.Sprintf("cloned.%s = cloneMapInt64HandleCols(op.%s)", fieldName, fieldName), true
	case "map[int][]ast.CIStr":
		return fmt.Sprintf("cloned.%s = cloneMapIntCIStrSlice(op.%s)", fieldName, fieldName), true
	case "map[int]map[uint64]struct {}":
		return fmt.Sprintf("cloned.%s = cloneMapIntUint64Set(op.%s)", fieldName, fieldName), true
	case "expression.GroupingSets":
		return fmt.Sprintf("cloned.%s = cloneGroupingSets(op.%s)", fieldName, fieldName), true
	case "map[string]*expression.Column":
		return fmt.Sprintf("cloned.%s = cloneColumnMap(op.%s)", fieldName, fieldName), true
	case "*logicalop.CTEClass":
		return fmt.Sprintf("cloned.%s = cloneCTEClass(op.%s)", fieldName, fieldName), true
	case "*logicalop.WindowFrame":
		return fmt.Sprintf("cloned.%s = cloneWindowFrame(op.%s)", fieldName, fieldName), true
	case "logicalop.ShowContents":
		return fmt.Sprintf("cloned.%s = cloneShowContents(op.%s)", fieldName, fieldName), true
	case "*logicalop.DataSource":
		return fmt.Sprintf("if op.%s != nil { cloned.%s = op.%s.DeepClone().(*DataSource) }", fieldName, fieldName, fieldName), true
	case "[]*ast.IndexHint":
		return fmt.Sprintf("cloned.%s = cloneIndexHintSlice(op.%s)", fieldName, fieldName), true
	case "[]hint.HintedIndex":
		return fmt.Sprintf("cloned.%s = cloneHintedIndexSlice(op.%s)", fieldName, fieldName), true
	case "[]ast.CIStr":
		return fmt.Sprintf("cloned.%s = cloneCIStrSlice(op.%s)", fieldName, fieldName), true
	case "[]string":
		return fmt.Sprintf("cloned.%s = cloneStringSlice(op.%s)", fieldName, fieldName), true
	case "[]int":
		return fmt.Sprintf("cloned.%s = cloneIntSlice(op.%s)", fieldName, fieldName), true
	case "[]uint64":
		return fmt.Sprintf("cloned.%s = cloneUint64Slice(op.%s)", fieldName, fieldName), true
	default:
		return "", false
	}
}

type codeGen struct {
	buffer bytes.Buffer
}

func (c *codeGen) write(format string, args ...any) {
	c.buffer.WriteString(fmt.Sprintf(format, args...))
	c.buffer.WriteString("\n")
}

func (c *codeGen) format() ([]byte, error) {
	return format.Source(c.buffer.Bytes())
}

const codeGenDeepClonePrefix = `// Copyright 2026 PingCAP, Inc.
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

// Code generated by deep_clone_generator; DO NOT EDIT IT DIRECTLY.

package logicalop

import "github.com/pingcap/tidb/pkg/planner/core/base"
`

func main() {
	fileData, err := GenDeepClone4LogicalOps()
	if err != nil {
		log.Fatalln("failed to generate deep_clone_generated.go", err)
	}
	if err := os.WriteFile("deep_clone_generated.go", fileData, 0644); err != nil {
		log.Fatalln("failed to write deep_clone_generated.go", err)
	}
}
