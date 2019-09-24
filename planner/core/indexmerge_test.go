// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testIndexMergeSuite{})

type testIndexMergeSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context
}

func (s *testIndexMergeSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{MockSignedTable(), MockView()})
	s.ctx = MockContext()
	s.Parser = parser.New()
}

func getIndexMergePathDigest(paths []*accessPath, startIndex int) string {
	if len(paths) == startIndex {
		return "[]"
	}
	idxMergeDisgest := "["
	for i := startIndex; i < len(paths); i++ {
		if i != startIndex {
			idxMergeDisgest += ","
		}
		path := paths[i]
		idxMergeDisgest += "{Idxs:["
		for j := 0; j < len(path.partialIndexPaths); j++ {
			if j > 0 {
				idxMergeDisgest += ","
			}
			idxMergeDisgest += path.partialIndexPaths[j].index.Name.L
		}
		idxMergeDisgest += "],TbFilters:["
		for j := 0; j < len(path.tableFilters); j++ {
			if j > 0 {
				idxMergeDisgest += ","
			}
			idxMergeDisgest += path.tableFilters[j].String()
		}
		idxMergeDisgest += "]}"
	}
	idxMergeDisgest += "]"
	return idxMergeDisgest
}

func (s *testIndexMergeSuite) TestIndexMergePathGenerateion(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql            string
		idxMergeDigest string
	}{
		{
			sql:            "select * from t",
			idxMergeDigest: "[]",
		},
		{
			sql:            "select * from t where c < 1",
			idxMergeDigest: "[]",
		},
		{
			sql:            "select * from t where c < 1 or f > 2",
			idxMergeDigest: "[{Idxs:[c_d_e,f_g],TbFilters:[]}]",
		},
		{
			sql: "select * from t where (c < 1 or f > 2) and (c > 5 or f < 7)",
			idxMergeDigest: "[{Idxs:[c_d_e,f_g],TbFilters:[or(gt(Column#3, 5), lt(Column#9, 7))]}," +
				"{Idxs:[c_d_e,f_g],TbFilters:[or(lt(Column#3, 1), gt(Column#9, 2))]}]",
		},
		{
			sql: "select * from t where (c < 1 or f > 2) and (c > 5 or f < 7) and (c < 1 or g > 2)",
			idxMergeDigest: "[{Idxs:[c_d_e,f_g],TbFilters:[or(gt(Column#3, 5), lt(Column#9, 7)),or(lt(Column#3, 1), gt(Column#10, 2))]}," +
				"{Idxs:[c_d_e,f_g],TbFilters:[or(lt(Column#3, 1), gt(Column#9, 2)),or(lt(Column#3, 1), gt(Column#10, 2))]}," +
				"{Idxs:[c_d_e,g],TbFilters:[or(lt(Column#3, 1), gt(Column#9, 2)),or(gt(Column#3, 5), lt(Column#9, 7))]}]",
		},
		{
			sql: "select * from t where (c < 1 or f > 2) and (c > 5 or f < 7) and (e < 1 or f > 2)",
			idxMergeDigest: "[{Idxs:[c_d_e,f_g],TbFilters:[or(gt(Column#3, 5), lt(Column#9, 7)),or(lt(Column#5, 1), gt(Column#9, 2))]}," +
				"{Idxs:[c_d_e,f_g],TbFilters:[or(lt(Column#3, 1), gt(Column#9, 2)),or(lt(Column#5, 1), gt(Column#9, 2))]}]",
		},
	}
	ctx := context.TODO()
	for i, tc := range tests {
		comment := Commentf("case:%v sql:%s", i, tc.sql)
		stmt, err := s.ParseOneStmt(tc.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		if err != nil {
			c.Assert(err.Error(), Equals, tc.idxMergeDigest, comment)
			continue
		}
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		lp := p.(LogicalPlan)
		c.Assert(err, IsNil)
		var ds *DataSource
		for ds == nil {
			switch v := lp.(type) {
			case *DataSource:
				ds = v
			default:
				lp = lp.Children()[0]
			}
		}
		ds.ctx.GetSessionVars().SetEnableIndexMerge(true)
		idxMergeStartIndex := len(ds.possibleAccessPaths)
		_, err = lp.recursiveDeriveStats()
		c.Assert(err, IsNil)
		c.Assert(getIndexMergePathDigest(ds.possibleAccessPaths, idxMergeStartIndex), Equals, tc.idxMergeDigest, comment)
	}
}
