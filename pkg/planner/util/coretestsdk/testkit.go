// Copyright 2023 PingCAP, Inc.
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

package coretestsdk

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// GetFieldValue is to get field value.
func GetFieldValue(prefix, row string) string {
	if idx := strings.Index(row, prefix); idx > 0 {
		start := idx + len(prefix)
		end := strings.Index(row[start:], " ")
		if end > 0 {
			value := row[start : start+end]
			value = strings.Trim(value, ",")
			return value
		}
	}
	return ""
}

// PlannerSuite is exported for test
type PlannerSuite struct {
	p    *parser.Parser
	is   infoschema.InfoSchema
	sctx sessionctx.Context
	ctx  base.PlanContext
}

// GetParser get the parser inside.
func (p *PlannerSuite) GetParser() *parser.Parser {
	return p.p
}

// GetIS get the is inside.
func (p *PlannerSuite) GetIS() infoschema.InfoSchema {
	return p.is
}

// GetSCtx get the sctx inside.
func (p *PlannerSuite) GetSCtx() sessionctx.Context {
	return p.sctx
}

// GetCtx get the ctx inside.
func (p *PlannerSuite) GetCtx() base.PlanContext {
	return p.ctx
}

// CreatePlannerSuite create a planner suite with specified is and sctx.
func CreatePlannerSuite(sctx sessionctx.Context, is infoschema.InfoSchema) (s *PlannerSuite) {
	s = new(PlannerSuite)
	s.is = is
	s.p = parser.New()
	s.sctx = sctx
	s.ctx = sctx.GetPlanCtx()
	return s
}

// CreatePlannerSuiteElems is to export createPlannerSuite for test outside core.
func CreatePlannerSuiteElems() (s *PlannerSuite) {
	return createPlannerSuite()
}

func createPlannerSuite() (s *PlannerSuite) {
	s = new(PlannerSuite)
	tblInfos := []*model.TableInfo{
		MockSignedTable(),
		MockUnsignedTable(),
		MockView(),
		MockNoPKTable(),
		MockRangePartitionTable(),
		MockHashPartitionTable(),
		MockListPartitionTable(),
		MockStateNoneColumnTable(),
		MockGlobalIndexHashPartitionTable(),
	}
	id := int64(1)
	for _, tblInfo := range tblInfos {
		tblInfo.ID = id
		id++
		pi := tblInfo.GetPartitionInfo()
		if pi == nil {
			continue
		}
		for i := range pi.Definitions {
			pi.Definitions[i].ID = id
			id++
		}
	}
	s.is = infoschema.MockInfoSchema(tblInfos)
	ctx := mock.NewContext()
	ctx.Store = &mock.Store{
		Client: &mock.Client{},
	}
	ctx.GetSessionVars().CurrentDB = "test"
	do := domain.NewMockDomain()
	if err := do.CreateStatsHandle(context.Background()); err != nil {
		panic(fmt.Sprintf("create mock context panic: %+v", err))
	}
	ctx.BindDomainAndSchValidator(do, nil)
	ctx.SetInfoSchema(s.is)
	s.ctx = ctx
	s.sctx = ctx
	domain.GetDomain(s.ctx).MockInfoCacheAndLoadInfoSchema(s.is)
	s.ctx.GetSessionVars().EnableWindowFunction = true
	s.p = parser.New()
	s.p.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})
	return
}

// Close closes the planner suite.
func (p *PlannerSuite) Close() {
	domain.GetDomain(p.ctx).StatsHandle().Close()
}
