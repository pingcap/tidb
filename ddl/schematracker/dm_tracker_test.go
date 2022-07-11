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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package schematracker

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCreateTableNoNumLimit(t *testing.T) {
	sql := "create table test.t_too_large ("
	cnt := 3000
	for i := 1; i <= cnt; i++ {
		sql += fmt.Sprintf("a%d double, b%d double, c%d double, d%d double", i, i, i, i)
		if i != cnt {
			sql += ","
		}
	}
	sql += ");"

	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	tracker := NewSchemaTracker(2)
	tracker.createTestDB()
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)

	sql = "create table test.t_too_many_indexes ("
	for i := 0; i < 100; i++ {
		if i != 0 {
			sql += ","
		}
		sql += fmt.Sprintf("c%d int", i)
	}
	for i := 0; i < 100; i++ {
		sql += ","
		sql += fmt.Sprintf("key k%d(c%d)", i, i)
	}
	sql += ");"
	stmt, err = p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
}
