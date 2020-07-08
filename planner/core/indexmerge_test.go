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
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIndexMergeSuite{})

type testIndexMergeSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	testdata testutil.TestData
}

func (s *testIndexMergeSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{MockSignedTable(), MockView()})
	s.ctx = MockContext()
	s.Parser = parser.New()
	var err error
	s.testdata, err = testutil.LoadTestSuiteData("testdata", "index_merge_suite")
	c.Assert(err, IsNil)
}

func (s *testIndexMergeSuite) TearDownSuite(c *C) {
	c.Assert(s.testdata.GenerateOutputIfNeeded(), IsNil)
}

func getIndexMergePathDigest(paths []*util.AccessPath, startIndex int) string {
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
		for j := 0; j < len(path.PartialIndexPaths); j++ {
			if j > 0 {
				idxMergeDisgest += ","
			}
			idxMergeDisgest += path.PartialIndexPaths[j].Index.Name.L
		}
		idxMergeDisgest += "],TbFilters:["
		for j := 0; j < len(path.TableFilters); j++ {
			if j > 0 {
				idxMergeDisgest += ","
			}
			idxMergeDisgest += path.TableFilters[j].String()
		}
		idxMergeDisgest += "]}"
	}
	idxMergeDisgest += "]"
	return idxMergeDisgest
}

func (s *testIndexMergeSuite) TestIndexMergePathGeneration(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testdata.GetTestCases(c, &input, &output)
	ctx := context.TODO()
	for i, tc := range input {
		comment := Commentf("case:%v sql:%s", i, tc)
		stmt, err := s.ParseOneStmt(tc, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		if err != nil {
			s.testdata.OnRecord(func() {
				output[i] = err.Error()
			})
			c.Assert(err.Error(), Equals, output[i], comment)
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
		result := getIndexMergePathDigest(ds.possibleAccessPaths, idxMergeStartIndex)
		s.testdata.OnRecord(func() {
			output[i] = result
		})
		c.Assert(result, Equals, output[i], comment)
	}
}
