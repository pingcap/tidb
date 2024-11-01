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

	"github.com/pingcap/tidb/pkg/planner/core"
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
	var structures = []any{core.PhysicalTableScan{}, core.PhysicalIndexScan{}, core.PhysicalSelection{},
		core.PhysicalProjection{}, core.PhysicalSort{}, core.PhysicalTopN{}, core.PhysicalStreamAgg{},
		core.PhysicalHashAgg{}, core.PhysicalHashJoin{}, core.PhysicalMergeJoin{}, core.PhysicalTableReader{},
		core.PhysicalIndexReader{}, core.PointGetPlan{}, core.BatchPointGetPlan{}, core.PhysicalLimit{},
		core.PhysicalIndexJoin{}, core.PhysicalIndexHashJoin{}, core.PhysicalIndexLookUpReader{}, core.PhysicalIndexMergeReader{},
		core.Update{}, core.Delete{}, core.Insert{}, core.PhysicalLock{}, core.PhysicalUnionScan{}, core.PhysicalUnionAll{}}
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
	for i := 0; i < vType.NumField(); i++ {
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
		case "core.PhysicalTableReader.TablePlans", "core.PhysicalIndexLookUpReader.TablePlans",
			"core.PhysicalIndexMergeReader.TablePlans":
			c.write("cloned.TablePlans = flattenPushDownPlan(cloned.tablePlan)")
			continue
		case "core.PhysicalIndexReader.IndexPlans", "core.PhysicalIndexLookUpReader.IndexPlans":
			c.write("cloned.IndexPlans = flattenPushDownPlan(cloned.indexPlan)")
			continue
		case "core.PhysicalIndexMergeReader.PartialPlans":
			c.write("cloned.PartialPlans = make([][]base.PhysicalPlan, len(op.PartialPlans))")
			c.write("for i, plan := range cloned.partialPlans {")
			c.write("cloned.PartialPlans[i] = flattenPushDownPlan(plan)")
			c.write("}")
			continue
		}

		switch f.Type.String() {
		case "[]int", "[]byte", "[]float", "[]bool": // simple slice
			c.write("cloned.%v = make(%v, len(op.%v))", f.Name, f.Type, f.Name)
			c.write("copy(cloned.%v, op.%v)", f.Name, f.Name)
		case "core.physicalSchemaProducer", "core.basePhysicalAgg", "core.basePhysicalJoin":
			fieldName := strings.Split(f.Type.String(), ".")[1]
			c.write(`basePlan, baseOK := op.%v.cloneForPlanCacheWithSelf(newCtx, cloned)
							if !baseOK {return nil, false}
							cloned.%v = *basePlan`, fieldName, fieldName)
		case "physicalop.BasePhysicalPlan":
			fieldName := strings.Split(f.Type.String(), ".")[1]
			c.write(`basePlan, baseOK := op.%v.CloneForPlanCacheWithSelf(newCtx, cloned)
							if !baseOK {return nil, false}
							cloned.%v = *basePlan`, fieldName, fieldName)
		case "baseimpl.Plan", "core.baseSchemaProducer":
			c.write("cloned.%v = *op.%v.CloneWithNewCtx(newCtx)", f.Name, f.Name)
		case "[]expression.Expression", "[]*ranger.Range", "[]*util.ByItems", "[]*expression.Column", "[]model.CIStr",
			"[]*expression.Constant", "[]*expression.ScalarFunction", "[]property.SortItem", "[]types.Datum",
			"[]kv.Handle", "[]*expression.Assignment":
			structureName := strings.Split(f.Type.String(), ".")[1] + "s"
			c.write("cloned.%v = util.Clone%v(op.%v)", f.Name, structureName, f.Name)
		case "[][]*expression.Constant", "[][]types.Datum", "[][]expression.Expression":
			structureName := strings.Split(f.Type.String(), ".")[1]
			c.write("cloned.%v = util.Clone%v2D(op.%v)", f.Name, structureName, f.Name)
		case "planctx.PlanContext":
			c.write("cloned.%v = newCtx", f.Name)
		case "util.HandleCols":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = op.%v.Clone(newCtx.GetSessionVars().StmtCtx)", f.Name, f.Name)
			c.write("}")
		case "*core.PhysPlanPartInfo", "*core.PushedDownLimit", "*expression.Schema":
			c.write("cloned.%v = op.%v.Clone()", f.Name, f.Name)
		case "kv.Handle":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = op.%v.Copy()", f.Name, f.Name)
			c.write("}")
		case "*core.ColWithCmpFuncManager", "core.InsertGeneratedColumns":
			c.write("cloned.%v = op.%v.Copy()", f.Name, f.Name)
		case "*expression.Column", "*expression.Constant":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = op.%v.Clone().(%v)", f.Name, f.Name, f.Type.String())
			c.write("}")
		case "core.PhysicalIndexJoin":
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
			c.write("%v, ok := clonePhysicalPlansForPlanCache(newCtx, op.%v)", f.Name, f.Name)
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
			c.write("cloned.%v[k] = util.CloneHandleCols(newCtx.GetSessionVars().StmtCtx, v)", f.Name)
			c.write("}}")
		case "map[int64]*expression.Column":
			c.write("if op.%v != nil {", f.Name)
			c.write("cloned.%v = make(map[int64]*expression.Column, len(op.%v))", f.Name, f.Name)
			c.write("for k, v := range op.%v {", f.Name)
			c.write("cloned.%v[k] = v.Clone().(*expression.Column)", f.Name)
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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
)

func clonePhysicalPlansForPlanCache(newCtx base.PlanContext, plans []base.PhysicalPlan) ([]base.PhysicalPlan, bool) {
	clonedPlans := make([]base.PhysicalPlan, len(plans))
	for i, plan := range plans {
		cloned, ok := plan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		clonedPlans[i] = cloned.(base.PhysicalPlan)
	}
	return clonedPlans, true
}
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
