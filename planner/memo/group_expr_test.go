// Copyright 2018 PingCAP, Inc.
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

package memo

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/stretchr/testify/require"
)

func TestNewGroupExpr(t *testing.T) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	require.Equal(t, p, expr.ExprNode)
	require.Nil(t, expr.Children)
	require.False(t, expr.Explored(0))
}

func TestGroupExprFingerprint(t *testing.T) {
	p := &plannercore.LogicalLimit{Count: 3}
	expr := NewGroupExpr(p)
	childGroup := NewGroupWithSchema(nil, expression.NewSchema())
	expr.SetChildren(childGroup)
	// ChildNum(2 bytes) + ChildPointer(8 bytes) + LogicalLimit HashCode
	planHash := p.HashCode()
	buffer := make([]byte, 10+len(planHash))
	binary.BigEndian.PutUint16(buffer, 1)
	binary.BigEndian.PutUint64(buffer[2:], uint64(reflect.ValueOf(childGroup).Pointer()))
	copy(buffer[10:], planHash)
	require.Equal(t, string(buffer), expr.FingerPrint())
}
