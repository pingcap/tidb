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

	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// GenHash64Equals4LogicalOps generates Hash64(xxx) and Equals(xxx) for all logical plan nodes in hash64_equals_generated.go.
// Using code-gen is safer than writing by hand, for example, if someone adds a new field to a struct,
// the code-gen can update the Clone method correctly and automatically.
// To update hash64_equals_generated.go, please run TestUpdateHash64EqualsCode manually.
// This function relies on Golang field tags to determine whether to hash64/equals involve a field or not.
// If a field is tagged with `hash64-equals`, then it will be computed in hash64 and equals func.
// If a field is not tagged, then it will be skipped.
func GenHash64Equals4LogicalOps() ([]byte, error) {
	var structures = []any{logicalop.LogicalJoin{}, logicalop.LogicalAggregation{}, logicalop.LogicalApply{},
		logicalop.LogicalExpand{}, logicalop.LogicalLimit{}, logicalop.LogicalMaxOneRow{}, logicalop.DataSource{},
		logicalop.LogicalMemTable{}, logicalop.LogicalUnionAll{}, logicalop.LogicalPartitionUnionAll{}, logicalop.LogicalProjection{},
		logicalop.LogicalSelection{}, logicalop.LogicalShow{}, logicalop.LogicalShowDDLJobs{}, logicalop.LogicalSort{},
	}
	c := new(cc)
	c.write(codeGenHash64EqualsPrefix)
	for _, s := range structures {
		code, err := genHash64EqualsForLogicalOps(s)
		if err != nil {
			return nil, err
		}
		c.write("%s", string(code))
	}
	return c.format()
}

// IHashEquals is the interface for hash64 and equals inside parser pkg.
type IHashEquals interface {
	Hash64(h types.IHasher)
	Equals(other any) bool
}

var hashEqualsType = reflect.TypeOf((*base.HashEquals)(nil)).Elem()
var iHashEqualsType = reflect.TypeOf((*IHashEquals)(nil)).Elem()

func genHash64EqualsForLogicalOps(x any) ([]byte, error) {
	c := new(cc)
	vType := reflect.TypeOf(x)
	// for Hash64 function.
	c.write("// Hash64 implements the Hash64Equals interface.")
	c.write("func (op *%v) Hash64(h base.Hasher) {", vType.Name())
	c.write("h.HashString(%v)", logicalOpName2PlanCodecString(vType.Name()))
	for i := 0; i < vType.NumField(); i++ {
		f := vType.Field(i)
		if !isHash64EqualsField(f) {
			continue
		}
		callName := "op." + vType.Field(i).Name
		// if a field is a pointer, we should encode the Nil/NotNil flag inside hash64.
		if f.Type.Kind() == reflect.Pointer || f.Type.Kind() == reflect.Slice {
			c.write("if %v == nil { h.HashByte(base.NilFlag) } else {", callName)
			c.write("h.HashByte(base.NotNilFlag)")
			c.Hash64Element(f.Type, callName)
			c.write("}")
		} else {
			c.Hash64Element(f.Type, callName)
		}
	}
	c.write("}")
	// for Equals function.
	c.write("// Equals implements the Hash64Equals interface, only receive *%v pointer.", vType.Name())
	c.write("func (op *%v) Equals(other any) bool {", vType.Name())
	c.write("op2, ok := other.(*%v)", vType.Name())
	c.write("if !ok { return false }")
	c.write("if op == nil { return op2 == nil }")
	c.write("if op2 == nil { return false }")
	hasValidField := false
	for i := 0; i < vType.NumField(); i++ {
		f := vType.Field(i)
		if !isHash64EqualsField(f) {
			continue
		}
		hasValidField = true
		leftCallName := "op." + vType.Field(i).Name
		rightCallName := "op2." + vType.Field(i).Name
		c.EqualsElement(f.Type, leftCallName, rightCallName, "i")
	}
	if !hasValidField {
		c.write("_ = op2")
	}
	c.write("return true")
	c.write("}")
	return c.format()
}

func logicalOpName2PlanCodecString(name string) string {
	switch name {
	case "LogicalJoin":
		return "plancodec.TypeJoin"
	case "LogicalAggregation":
		return "plancodec.TypeAgg"
	case "LogicalApply":
		return "plancodec.TypeApply"
	case "LogicalExpand":
		return "plancodec.TypeExpand"
	case "LogicalLimit":
		return "plancodec.TypeLimit"
	case "LogicalMaxOneRow":
		return "plancodec.TypeMaxOneRow"
	case "DataSource":
		return "plancodec.TypeDataSource"
	case "LogicalMemTable":
		return "plancodec.TypeMemTableScan"
	case "LogicalUnionAll":
		return "plancodec.TypeUnion"
	case "LogicalPartitionUnionAll":
		return "plancodec.TypePartitionUnion"
	case "LogicalProjection":
		return "plancodec.TypeProj"
	case "LogicalSelection":
		return "plancodec.TypeSel"
	case "LogicalShow":
		return "plancodec.TypeShow"
	case "LogicalShowDDLJobs":
		return "plancodec.TypeShowDDLJobs"
	case "LogicalSort":
		return "plancodec.TypeSort"
	default:
		return ""
	}
}

func isHash64EqualsField(fType reflect.StructField) bool {
	return fType.Tag.Get("hash64-equals") == "true"
}

// EqualsElement EqualsElements generate the equals function for every field inside logical op.
func (c *cc) EqualsElement(fType reflect.Type, lhs, rhs string, i string) {
	switch fType.Kind() {
	case reflect.Slice:
		c.write("if (%v == nil && %v != nil) || (%v != nil && %v == nil) || len(%v) != len(%v) { return false }", lhs, rhs, lhs, rhs, lhs, rhs)
		itemName := "one"
		if strings.HasPrefix(lhs, "one") {
			itemName = lhs + "e"
		}
		c.write("for %v, %v := range %v {", i, itemName, lhs)
		// one more round
		rhs = rhs + "[" + i + "]"
		// for ?, one := range [][][][]...
		// for use i for out-most ref, for each level deeper, appending another i for simple.
		// and you will see:
		// for i, one range := [][][]
		//    for ii, one range :=  [][]
		//        for iii, one := range []
		// and so on...
		newi := i + "i"
		c.EqualsElement(fType.Elem(), itemName, rhs, newi)
		c.write("}")
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		c.write("if %v != %v {return false}", lhs, rhs)
	default:
		if fType.Implements(hashEqualsType) || fType.Implements(iHashEqualsType) ||
			reflect.PtrTo(fType).Implements(hashEqualsType) || reflect.PtrTo(fType).Implements(iHashEqualsType) {
			if fType.Kind() == reflect.Struct {
				rhs = "&" + rhs
			}
			c.write("if !%v.Equals(%v) {return false}", lhs, rhs)
		} else {
			panic("doesn't support element type" + fType.Kind().String())
		}
	}
}

func (c *cc) Hash64Element(fType reflect.Type, callName string) {
	switch fType.Kind() {
	case reflect.Slice:
		c.write("h.HashInt(len(%v))", callName)
		itemName := "one"
		if strings.HasPrefix(callName, "one") {
			itemName = callName + "e"
		}
		c.write("for _, %v := range %v {", itemName, callName)
		// one more round
		c.Hash64Element(fType.Elem(), itemName)
		c.write("}")
	case reflect.String:
		c.write("h.HashString(%v)", callName)
	case reflect.Bool:
		c.write("h.HashBool(%v)", callName)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		c.write("h.HashInt64(int64(%v))", callName)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		c.write("h.HashUint64(uint64(%v))", callName)
	case reflect.Float32, reflect.Float64:
		c.write("h.HashFloat64(float64(%v))", callName)
	default:
		if fType.Implements(hashEqualsType) || fType.Implements(iHashEqualsType) ||
			reflect.PtrTo(fType).Implements(hashEqualsType) || reflect.PtrTo(fType).Implements(iHashEqualsType) {
			c.write("%v.Hash64(h)", callName)
		} else {
			panic("doesn't support element type" + fType.Kind().String())
		}
	}
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

const codeGenHash64EqualsPrefix = `// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)
`

func main() {
	fileData, err := GenHash64Equals4LogicalOps()
	if err != nil {
		log.Fatalln("failed to generate hash64_equals_generated.go", err)
	}
	if err := os.WriteFile("hash64_equals_generated.go", fileData, 0644); err != nil {
		log.Fatalln("failed to write hash64_equals_generated.go", err)
	}
}
