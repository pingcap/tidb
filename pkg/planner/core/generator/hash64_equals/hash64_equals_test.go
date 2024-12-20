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
	"bufio"
	"bytes"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

type Test interface {
	Hello() string
}

type A struct {
}

type B1 struct {
	b *A
}

func (*B1) Hello() string {
	return "B1"
}

type B2 struct {
	b A
}

func (*B2) Hello() string {
	return "B2"
}

func TestGenHash64EqualsField(t *testing.T) {
	vType1 := reflect.TypeOf(B1{})
	vType2 := reflect.TypeOf(B2{})
	f1 := vType1.Field(0)
	f2 := vType2.Field(0)
	k1 := f1.Type.Kind()
	k2 := f2.Type.Kind()
	require.Equal(t, k1, reflect.Pointer)
	require.Equal(t, k2, reflect.Struct)

	vValue1 := reflect.ValueOf(B1{})
	// vValue2 := reflect.ValueOf(B2{})
	require.True(t, vValue1.Field(0).IsNil())
	// we can't call IsNil on a non-pointer field.
	// require.False(t, vValue2.Field(0).IsNil())

	vType1 = reflect.TypeOf(&B1{})
	vType2 = reflect.TypeOf(&B2{})
	// pointer do not have a filed.
	// f1 := vType1.Field(0)
	// f2 := vType2.Field(0)
	k1 = vType1.Kind()
	k2 = vType2.Kind()
	require.Equal(t, k1, reflect.Pointer)
	require.Equal(t, k2, reflect.Pointer)

	vValue1 = reflect.ValueOf(&B1{})
	vValue2 := reflect.ValueOf(&B2{})
	// value still pointer, no fields.
	// require.Equal(t, 0, vValue1.NumField())
	// require.Equal(t, 0, vValue2.NumField())

	vValue1 = reflect.Indirect(vValue1)
	vValue2 = reflect.Indirect(vValue2)
	require.Equal(t, 1, vValue1.NumField())
	require.Equal(t, 1, vValue2.NumField())

	TestType := reflect.TypeOf((*Test)(nil)).Elem()
	require.True(t, vType1.Implements(TestType))
	require.True(t, vType2.Implements(TestType))
}

func TestHash64Equals(t *testing.T) {
	updatedCode, err := GenHash64Equals4LogicalOps()
	if err != nil {
		t.Errorf("Generate CloneForPlanCache code error: %v", err)
		return
	}
	currentCode, err := os.ReadFile("../../operator/logicalop/hash64_equals_generated.go")
	if err != nil {
		t.Errorf("Read current hash64_equals_generated.go code error: %v", err)
		return
	}
	updateLines := bufio.NewReader(strings.NewReader(string(updatedCode)))
	currentLines := bufio.NewReader(strings.NewReader(string(currentCode)))
	for {
		line1, err1 := util.ReadLine(updateLines, 1024)
		line2, err2 := util.ReadLine(currentLines, 1024)
		if err1 == nil && err2 != nil || err1 != nil && err2 == nil || err1 != nil && err2 != nil && err1.Error() != err2.Error() || !bytes.Equal(line1, line2) {
			t.Errorf("line unmatched, line1: %s, line2: %s", string(line1), string(line2))
			break
		}
		if err1 == io.EOF && err2 == io.EOF {
			break
		}
	}
	if !bytes.Equal(updatedCode, currentCode) {
		t.Errorf("hash64_equals_generated.go should be updated, please run 'make gogenerate' to update it.")
	}
}
