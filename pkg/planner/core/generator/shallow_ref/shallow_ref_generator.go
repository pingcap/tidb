// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// GenShallowRef4LogicalOps generates ShallowRef() for all logical plan nodes in logical_op_cow_generator.go.
// Using code-gen is safer than writing by hand, for example, if someone adds a new field to a struct,
// the code-gen can update the Clone method correctly and automatically.
// To update logical_op_cow_generated.go, please run TestUpdateLogicalOpCOWCode manually.
// This function relies on Golang field tags to determine whether to COW involve a field or not.
// If a field is tagged with `cow`, then it will be computed in ShadowRef and DeepClone func.
// If a field is not tagged, then it will be skipped.
//
// Actually:
// * ShadowRef will create new this owner's data structure and fill those elements back in them. It’s
// mainly for rearrangement or remove or appending from the owner's structure. For special case for changing
// one of the original shallow copy's element, it will create a new one and fill the elements back. example:
// clonedJoin._EqualConditionsShallowRef()
// clonedJoin.EqualConditions[0] = newFunction(xx, clonedJoin.EqualConditions[0].FuncName, clodJoin.EqualConditions[0].tp|nullable)
func GenShallowRef4LogicalOps() ([]byte, error) {
	var structures = []any{logicalop.LogicalJoin{}}
	c := new(cc)
	c.write(codeGenLogicalOpCowPrefix)
	for _, s := range structures {
		code, err := genShallowRef4LogicalOps(s)
		if err != nil {
			return nil, err
		}
		c.write("%s", string(code))
	}
	return c.format()
}

func refineFieldTypeName(fieldTypeName string) string {
	if strings.HasPrefix(fieldTypeName, "logicalop.") {
		return fieldTypeName[10:]
	}
	return fieldTypeName
}

func genShallowRef4LogicalOps(x any) ([]byte, error) {
	c := new(cc)
	vType := reflect.TypeOf(x)
	// Gen the shallowRef for operator itself.
	funcName := vType.Name() + "ShallowRef"
	returnType := "*" + refineFieldTypeName(vType.String())
	c.write("// %v implements the copy-on-write usage.", funcName)
	c.write("func (op *%v) %v() %v {", vType.Name(), funcName, returnType)
	c.write("shallow := %v", "*op")
	c.write("return &shallow")
	c.write("}")

	// For each field tagged with shallow-ref, generate a XXXShallowRef function.
	for i := 0; i < vType.NumField(); i++ {
		f := vType.Field(i)
		if !isShallowRefField(f) {
			continue
		}
		funcName := vType.Field(i).Name + "ShallowRef"
		returnType := vType.Field(i).Type.String()
		c.write("// %v implements the copy-on-write usage.", funcName)
		c.write("func (op *%v) %v() %v {", vType.Name(), funcName, returnType)
		c.shallowRefElement(f.Type, "op.", vType.Field(i).Name)
		c.write("return %v", "op."+vType.Field(i).Name)
		c.write("}")
	}
	return c.format()
}

func (c *cc) shallowRefElement(fType reflect.Type, caller, fieldName string) string {
	switch fType.Kind() {
	case reflect.Slice:
		//equalConditionsShallowRef := make([]*expression.ScalarFunction, 0, len(p.EqualConditions))
		tmpFieldName := fieldName + "CP"
		c.write("%v := make(%v, 0, len(%v))", tmpFieldName, fType.String(), caller+fieldName)
		itemName := "one"
		if strings.HasPrefix(fieldName, "one") {
			itemName = fieldName + "e"
		}
		c.write("for _, %v := range %v {", itemName, caller+fieldName)
		res := c.shallowRefElement(fType.Elem(), "", itemName)
		if res == "" {
			// which means they are basic types or pointer/structure, just shallow them.
			// join.[]bool, we just new a slice bool to value copy the original bool ones.
			// join.[]*pointer, we just new a slice of pointer to value ref the original pointer ones.
			// join.[]structure, we just new a slice of structure to value copy the original structure ones.
			// we are targeting at not changing the original slices or maps.
			res = itemName
		}
		c.write("%v = append(%v, %v)", tmpFieldName, tmpFieldName, res)
		c.write("}")
		c.write("%v = %v", caller+fieldName, tmpFieldName)
	case reflect.Pointer: // just ref it.
	case reflect.String:
	case reflect.Interface:
	case reflect.Bool: // just value cp.
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	case reflect.Float32, reflect.Float64:
	default:
		panic("doesn't support element type" + fType.Kind().String())
	}
	return ""
}

func isShallowRefField(fType reflect.StructField) bool {
	return fType.Tag.Get("shallow-ref") == "true"
}

type cc struct {
	buffer bytes.Buffer
}

func (c *cc) write(format string, args ...any) {
	c.buffer.WriteString(fmt.Sprintf(format, args...))
	c.buffer.WriteString("\n")
}

func (c *cc) format() ([]byte, error) {
	return format.Source(c.buffer.Bytes())
}

const codeGenLogicalOpCowPrefix = `// Copyright 2025 PingCAP, Inc.
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

// Code generated by hash64_equals_generator; DO NOT EDIT IT DIRECTLY.

package logicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
)
`

func main() {
	fileData, err := GenShallowRef4LogicalOps()
	if err != nil {
		log.Fatalln("failed to generate shallow_ref_generated.go", err)
	}
	if err := os.WriteFile("shallow_ref_generated.go", fileData, 0644); err != nil {
		log.Fatalln("failed to write shallow_ref_generated.go", err)
	}
}
