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

package core

import (
	"bytes"
	"fmt"
	"go/format"
	"reflect"
	"strings"
)

// genPlanCloneForPlanCacheCode generates CloneForPlanCache for all physical plan nodes in plan_clone_generated.go.
// Using code-gen is safer than writing by hand, for example, if someone adds a new field to a struct,
// the code-gen can update the Clone method correctly and automatically.
// To update plan_clone_generated.go, please run TestUpdatePlanCloneCode manually.
// This function relies on Golang field tags to determine whether to shallow clone a field or not.
// If a field is tagged with `plan-cache-clone:"shallow"`, then it will be shallow cloned.
// If a field is tagged with `plan-cache-clone:"must-nil"`, then it will be checked for nil before cloning.
// If a field is not tagged, then it will be deep cloned.
func genPlanCloneForPlanCacheCode() ([]byte, error) {
	var structures = []any{PhysicalTableScan{}, PhysicalIndexScan{}, PhysicalSelection{}, PhysicalProjection{},
		PhysicalSort{}, PhysicalTopN{}, PhysicalStreamAgg{}, PhysicalHashAgg{},
		PhysicalHashJoin{}, PhysicalMergeJoin{}}
	c := new(codeGen)
	c.write(codeGenPrefix)
	for _, s := range structures {
		code, err := genPlanCloneForPlanCache(s)
		if err != nil {
			return nil, err
		}
		c.write(string(code))
	}
	return c.format()
}

func genPlanCloneForPlanCache(x any) ([]byte, error) {
	c := new(codeGen)
	vType := reflect.TypeOf(x)
	c.write("// CloneForPlanCache implements the base.Plan interface.")
	c.write("func (op *%v) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {", vType.Name())
	c.write("cloned := new(%v)", vType.Name())
	c.write("*cloned = *op")
	for i := 0; i < vType.NumField(); i++ {
		f := vType.Field(i)
		if allowShallowClone(f) {
			continue
		}
		if mustNilField(f) {
			c.write(`if op.%v != nil {return nil, false}`, f.Name)
			continue
		}
		switch f.Type.String() {
		case "[]int", "[]byte", "[]float", "[]bool": // simple slice
			c.write("cloned.%v = make(%v, len(op.%v))", f.Name, f.Type, f.Name)
			c.write("copy(cloned.%v, op.%v)", f.Name, f.Name)
		case "core.physicalSchemaProducer", "core.basePhysicalPlan", "core.basePhysicalAgg", "core.basePhysicalJoin":
			fieldName := strings.Split(f.Type.String(), ".")[1]
			c.write(`basePlan, baseOK := op.%v.cloneForPlanCacheWithSelf(newCtx, cloned)
							if !baseOK {
								return nil, false
							}
							cloned.%v = *basePlan`, fieldName, fieldName)
		case "[]expression.Expression":
			c.write("cloned.%v = util.CloneExprs(op.%v)", f.Name, f.Name)
		case "[]*ranger.Range":
			c.write("cloned.%v = util.CloneRanges(op.%v)", f.Name, f.Name)
		case "[]*util.ByItems":
			c.write("cloned.%v = util.CloneByItems(op.%v)", f.Name, f.Name)
		case "[]*expression.Column":
			c.write("cloned.%v = util.CloneCols(op.%v)", f.Name, f.Name)
		case "[]*expression.ScalarFunction":
			c.write("cloned.%v = util.CloneScalarFunctions(op.%v)", f.Name, f.Name)
		case "[]property.SortItem":
			c.write("cloned.%v = util.CloneSortItem(op.%v)", f.Name, f.Name)
		case "util.HandleCols":
			c.write("cloned.%v = op.%v.Clone(newCtx.GetSessionVars().StmtCtx)", f.Name, f.Name)
		case "*core.PhysPlanPartInfo":
			c.write("cloned.%v = op.%v.Clone()", f.Name, f.Name)
		case "*expression.Column":
			c.write("cloned.%v = op.%v.Clone().(*expression.Column)", f.Name, f.Name)
		default:
			return nil, fmt.Errorf("can't generate Clone method for type %v in %v", f.Type.String(), vType.String())
		}
	}
	c.write("return cloned, true")
	c.write("}")
	return c.format()
}

func mustNilField(fType reflect.StructField) bool {
	return fType.Tag.Get("plan-cache-clone") == "must-nil"
}

func allowShallowClone(fType reflect.StructField) bool {
	if fType.Tag.Get("plan-cache-clone") == "shallow" {
		return true // allow shallow clone for this field
	}
	switch fType.Type.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8,
		reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return true
	default:
		return false
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

const codeGenPrefix = `// Copyright 2024 PingCAP, Inc.
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

// Code generated by plan_clone_generator; DO NOT EDIT IT DIRECTLY.

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
)
`
