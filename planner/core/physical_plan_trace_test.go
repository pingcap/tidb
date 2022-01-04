// Copyright 2021 PingCAP, Inc.
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

package core

import (
	"context"
	"sort"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/tracing"
)

func (s *testPlanSuite) TestPhysicalOptimizeWithTraceEnabled(c *C) {
	sql := "select * from t where a in (1,2)"
	defer testleak.AfterTest(c)()

	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
	c.Assert(err, IsNil)
	sctx := MockContext()
	sctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = true
	builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
	ctx := context.TODO()
	p, err := builder.Build(ctx, stmt)
	c.Assert(err, IsNil)
	flag := uint64(0)
	logical, err := logicalOptimize(ctx, flag, p.(LogicalPlan))
	c.Assert(err, IsNil)
	_, _, err = physicalOptimize(logical, &PlanCounterDisabled)
	c.Assert(err, IsNil)
	otrace := sctx.GetSessionVars().StmtCtx.PhysicalOptimizeTrace
	c.Assert(otrace, NotNil)
	logicalList, physicalList, bests := getList(otrace)
	c.Assert(checkList(logicalList, []string{"Projection_3", "Selection_2"}), IsTrue)
	c.Assert(checkList(physicalList, []string{"Projection_4", "Selection_5"}), IsTrue)
	c.Assert(checkList(bests, []string{"Projection_4", "Selection_5"}), IsTrue)
}

func checkList(d []string, s []string) bool {
	if len(d) != len(s) {
		return false
	}
	for i := 0; i < len(d); i++ {
		if strings.Compare(d[i], s[i]) != 0 {
			return false
		}
	}
	return true
}

func getList(otrace *tracing.PhysicalOptimizeTracer) (ll []string, pl []string, bests []string) {
	for logicalPlan, v := range otrace.State {
		ll = append(ll, logicalPlan)
		for _, info := range v {
			bests = append(bests, tracing.CodecPlanName(info.BestTask.TP, info.BestTask.ID))
			for _, task := range info.Candidates {
				pl = append(pl, tracing.CodecPlanName(task.TP, task.ID))
			}
		}
	}
	sort.Strings(ll)
	sort.Strings(pl)
	sort.Strings(bests)
	return ll, pl, bests
}
