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

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestInlineHybridIndexMetadata(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt(`create table t(a int, hybrid index h(a) parameter '{"inverted":[{"columns":["a"]}],"sharding_key":{"columns":["a"]}}')`, "", "")
	require.NoError(t, err)
	createStmt := stmt.(*ast.CreateTableStmt)
	require.Equal(t, pmodel.IndexTypeInvalid, createStmt.Constraints[0].Option.Tp)
	tbl, err := BuildTableInfoFromAST(metabuild.NewContext(), createStmt)
	require.NoError(t, err)
	require.Len(t, tbl.Indices, 1)
	index := tbl.Indices[0]
	require.False(t, index.Primary)
	require.False(t, index.Unique)
	require.Zero(t, tbl.Columns[0].GetFlag()&(mysql.PriKeyFlag|mysql.UniqueKeyFlag|mysql.MultipleKeyFlag))
	require.Equal(t, ast.ConstraintHybrid, createStmt.Constraints[0].Tp)
	require.Equal(t, pmodel.IndexTypeInvalid, createStmt.Constraints[0].Option.Tp)
	require.NotNil(t, index.HybridInfo)
	require.Equal(t, pmodel.IndexTypeHybrid, index.Tp)
	require.True(t, index.IsTiCIIndex())
	require.True(t, index.IsNonKVIndex())
	require.Len(t, index.HybridInfo.Inverted, 1)
	require.Len(t, index.HybridInfo.Sharding.Columns, 1)
	require.Equal(t, "a", index.HybridInfo.Sharding.Columns[0].Name.L)
}

func TestInlineFulltextPartitionMetadata(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt(`CREATE TABLE t(id INT,body VARCHAR(200),FULLTEXT(body)) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10))`, "", "")
	require.NoError(t, err)
	tbl, err := BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	require.NotNil(t, tbl.Partition)
	require.Len(t, tbl.Indices, 1)
	require.NotNil(t, tbl.Indices[0].FullTextInfo)
	require.True(t, tbl.Indices[0].IsTiCIIndex())
}

func TestInlineHybridIndexRejectInvalidOptions(t *testing.T) {
	for _, sql := range []string{
		`create table t(a int, hybrid index h(a) using btree)`,
		`create table t(a int, index h(a) parameter '{}')`,
		`create table t(a int, hybrid index h(a) parameter 'invalid')`,
		`create table t(a int, hybrid index h((a+1)) parameter '{}')`,
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(sql, "", "")
			require.NoError(t, err)
			_, err = BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
			require.Error(t, err)
		})
	}
}
