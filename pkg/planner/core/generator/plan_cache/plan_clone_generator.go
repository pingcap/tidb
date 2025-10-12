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

package main

import (
	"bytes"
	"fmt"
	"go/format"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)

// GenPlanCloneForPlanCacheCode generates CloneForPlanCache for all physical plan nodes in plan_clone_generated.go.
// Using code-gen is safer than writing by hand, for example, if someone adds a new field to a struct,
// the code-gen can update the Clone method correctly and automatically.
// To update plan_clone_generated.go, please run TestUpdatePlanCloneCode manually.
// This function relies on Golang field tags to determine whether to shallow clone a field or not.
// If a field is tagged with `plan-cache-clone:"shallow"`, then it will be shallow cloned.
// If a field is tagged with `plan-cache-clone:"must-nil"`, then it will be checked for nil before cloning.
// If a field is not tagged, then it will be deep cloned.
func GenPlanCloneForPlanCacheCode() ([]byte, error) {
	var structures = []any{
		physicalop.Update{}, physicalop.Delete{}, physicalop.Insert{},
		physicalop.PhysicalTableScan{}, physicalop.PhysicalIndexScan{},
		physicalop.PhysicalSelection{}, physicalop.PhysicalProjection{}, physicalop.PhysicalTopN{}, physicalop.PhysicalLimit{},
		physicalop.PhysicalStreamAgg{}, physicalop.PhysicalHashAgg{},
		physicalop.PhysicalHashJoin{}, physicalop.PhysicalMergeJoin{}, physicalop.PhysicalIndexJoin{},
		physicalop.PhysicalIndexHashJoin{},
		physicalop.PhysicalIndexReader{}, physicalop.PhysicalTableReader{}, physicalop.PhysicalIndexMergeReader{},
		physicalop.PhysicalIndexLookUpReader{}, physicalop.PhysicalLocalIndexLookUp{},
		physicalop.BatchPointGetPlan{}, physicalop.PointGetPlan{},
		physicalop.PhysicalUnionScan{}, physicalop.PhysicalUnionAll{}, physicalop.PhysicalTableDual{},
	}
	c := new(codeGen)
	c.write(codeGenPlanCachePrefix)
	for _, s := range structures {
		code, err := genPlanCloneForPlanCache(s)
		if err != nil {
			return nil, err
		}
		c.write("%s", string(code))
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
	for i := range vType.NumField() {
		f := vType.Field(i)
		if allowShallowClone(f) {
			continue
		}
		if mustNilField(f) {
			c.write(`if op.%v != nil {return nil, false}`, f.Name)
			continue
		}

		fullFieldName := fmt.Sprintf("%v.%v", vType.String(), vType.Field(i).Name)
		switch fullFieldName { // handle some fields specially
		case "physicalop.PhysicalTableReader.TablePlans", "physicalop.PhysicalIndexLookUpReader.TablePlans",
			"physicalop.PhysicalIndexMergeReader.TablePlans":
			c.write("cloned.TablePlans = FlattenListPushDownPlan(cloned.TablePlan)")
			continue
		case "physicalop.PhysicalIndexReader.IndexPlans":
			c.write("cloned.IndexPlans = FlattenListPushDownPlan(cloned.IndexPlan)")
			continue
		case "physicalop.PhysicalIndexLookUpReader.IndexPlans":
			c.write("if cloned.IndexLookUpPushDown {")
			c.write("cloned.IndexPlans, cloned.IndexPlansUnNatureOrders = FlattenTreePushDownPlan(cloned.IndexPlan)")
			c.write("} else {")
			c.write("cloned.IndexPlans = FlattenListPushDownPlan(cloned.IndexPlan)")
			c.write("}")
			continue
		case "physicalop.PhysicalIndexMergeReader.PartialPlans":
			c.write("cloned.PartialPlans = make([][]base.PhysicalPlan, len(op.PartialPlans))")
			c.write("for i, plan := range cloned.PartialPlansRaw {")
			c.write("cloned.PartialPlans[i] = FlattenListPushDownPlan(plan)")
			c.write("}")
			continue
		}
		switch f.Type.String() {
		case "[]int", "[]byte", "[]float", "[]bool", "[]uint32": // simple slice
			c.write("cloned.%v = make(%v, len(op.%v))", f.Name, f.Type, f.Name)
			c.write("copy(cloned.%v, op.%v)", f.Name, f.Name)
		case "physicalop.BasePhysicalAgg":
			fieldName := strings.Split(f.Type.String(), ".")[1]
			c.write(`basePlan, baseOK := op.%v.CloneForPlanCacheWithSelf(newCtx, cloned)
							if !baseOK {return nil, false}
							cloned.%v = *basePlan`, fieldName, fieldName)
		case "physicalop.BasePhysicalJoin":
			fieldName := strings.Split(f.Type.String(), ".")[1]
			c.write(`basePlan, baseOK := op.%v.CloneForPlanCacheWithSelf(newCtx, cloned)
							if !baseOK {return nil, false}
							cloned.%v = *basePlan`, fieldName, fieldName)
		case "physicalop.BasePhysicalPlan", "physicalop.PhysicalSchemaProducer":
			fieldName := strings.Split(f.Type.String(), ".")[1]
			c.write(`basePlan, baseOK := op.%v.CloneForPlanCacheWithSelf(newCtx, cloned)
							if !baseOK {return nil, false}
							cloned.%v = *basePlan`, fieldName, fieldName)
		case "baseimpl.Plan":
			c.write("cloned.%v = *op.%v.CloneWithNewCtx(newCtx)", f.Name, f.Name)
		case "physicalop.SimpleSchemaProducer":
			c.write("cloned.%v = *op.%v.CloneSelfForPlanCache(newCtx)", f.Name, f.Name)
		case "[]expression.Expression", "[]*expression.Column",
			"[]*expression.Constant", "[]*expression.ScalarFunction":
			structureName := strings.Split(f.Type.String(), ".")[1] + "s"
			c.write("cloned.%v = utilfuncp.Clone%vForPlanCache(op.%v, nil)", f.Name, structureName, f.Name)
		case "[][]expression.Expression":
			structureName := strings.Split(f.Type.String(), ".")[1]
			c.write("cloned.%v = utilfuncp.Clone%v2DForPlanCache(op.%v)", f.Name, structureName, f.Name)
		case "[][]*expression.Constant":
			structureName := strings.Split(f.Type.String(), ".")[1]
			c.write("cloned.%v = Clone%v2DForPlanCache(op.%v)", f.Name, structureName, f.Name)
		case "[]*ranger.Range", "[]*util.ByItems", "[]property.SortItem":
			c.write("cloned.%v = sliceutil.DeepClone(op.%v)", f.Name, f.Name)
		case "[]model.CIStr",
			"[]types.Datum", "[]kv.Handle", "[]*expression.Assignment":
			structureName := strings.Split(f.Type.String(), ".")[1] + "s"
			c.write("cloned.%v = util.Clone%v(op.%v)", f.Name, structureName, f.Name)
		case "[][]types.Datum":
			structureName := strings.Split(f.Type.String(), ".")[1]
			c.write("cloned.%v = util.Clone%v2D(op.%v)", f.Name, structureName, f.Name)
		case "[]*types.FieldName":
			c.write("cloned.%v = util.CloneFieldNames(op.%v)", f.Name, f.Name)
		case "planctx.PlanContext":
			c.write("cloned.%v = newCtx", f.Name)
		case "util.HandleCols":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = op.%v.Clone()", f.Name, f.Name)
			c.write("}")
		case "*physicalop.PushedDownLimit":
			c.write("cloned.%v = op.%v.Clone()", f.Name, f.Name)
		case "*physicalop.PhysPlanPartInfo":
			c.write("cloned.%v = op.%v.CloneForPlanCache()", f.Name, f.Name)
		case "*physicalop.ColWithCmpFuncManager", "physicalop.InsertGeneratedColumns":
			c.write("cloned.%v = op.%v.cloneForPlanCache()", f.Name, f.Name)
		case "kv.Handle":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = op.%v.Copy()", f.Name, f.Name)
			c.write("}")
		case "*expression.Column", "*expression.Constant":
			c.write("if op.%v != nil {", f.Name)
			c.write("if op.%v.SafeToShareAcrossSession() {", f.Name)
			c.write("cloned.%v = op.%v", f.Name, f.Name)
			c.write("} else {")
			c.write("cloned.%v = op.%v.Clone().(%v)", f.Name, f.Name, f.Type.String())
			c.write("}")
			c.write("}")
		case "physicalop.PhysicalIndexJoin":
			c.write("inlj, ok := op.%v.CloneForPlanCache(newCtx)", f.Name)
			c.write("if !ok {return nil, false}")
			c.write("cloned.%v = *inlj.(*PhysicalIndexJoin)", f.Name)
			c.write("cloned.Self = cloned")
		case "base.PhysicalPlan":
			c.write("if op.%v != nil {", f.Name)
			c.write("%v, ok := op.%v.CloneForPlanCache(newCtx)", f.Name, f.Name)
			c.write("if !ok {return nil, false}")
			c.write("cloned.%v = %v.(base.PhysicalPlan)", f.Name, f.Name)
			c.write("}")
		case "[]base.PhysicalPlan":
			c.write("%v, ok := ClonePhysicalPlansForPlanCache(newCtx, op.%v)", f.Name, f.Name)
			c.write("if !ok {return nil, false}")
			c.write("cloned.%v = %v", f.Name, f.Name)
		case "*int":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = new(int)", f.Name)
			c.write("*cloned.%v = *op.%v", f.Name, f.Name)
			c.write("}")
		case "ranger.MutableRanges":
			c.write("cloned.%v = op.%v.CloneForPlanCache()", f.Name, f.Name)
		case "map[int64][]util.HandleCols":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = make(map[int64][]util.HandleCols, len(op.%v))", f.Name, f.Name)
			c.write("for k, v := range op.%v {", f.Name)
			c.write("cloned.%v[k] = util.CloneHandleCols(v)", f.Name)
			c.write("}}")
		case "map[int64]*expression.Column":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = make(map[int64]*expression.Column, len(op.%v))", f.Name, f.Name)
			c.write("for k, v := range op.%v {", f.Name)
			c.write("cloned.%v[k] = v.Clone().(*expression.Column)", f.Name)
			c.write("}}")
		case "map[int]int":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = make(map[int]int, len(op.%v))", f.Name, f.Name)
			c.write("for k, v := range op.%v {", f.Name)
			c.write("cloned.%v[k] = v", f.Name)
			c.write("}}")
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

const codeGenPlanCachePrefix = `// Copyright 2024 PingCAP, Inc.
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

package physicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
)
`

func main() {
	fileData, err := GenPlanCloneForPlanCacheCode()
	if err != nil {
		log.Fatalln("failed to generate plan_clone_generated.go", err)
	}
	if err := os.WriteFile("plan_clone_generated.go", fileData, 0644); err != nil {
		log.Fatalln("failed to write plan_clone_generated.go", err)
	}
}
