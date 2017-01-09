// Copyright 2017 PingCAP, Inc.
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

package statscache

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStatsCacheSuite{})

type testStatsCacheSuite struct{}

func (s *testStatsCacheSuite) TestStatsCache(c *C) {
	tblInfo := &model.TableInfo{
		ID: 1,
	}
	columns := []*model.ColumnInfo{
		{
			ID:        2,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
	}
	tblInfo.Columns = columns
	ctx := mock.NewContext()
	ctx.Store = kv.NewMockStorage()
	statsTbl := GetStatisticsTableCache(ctx, tblInfo)
	c.Check(len(statsTbl.Columns), Equals, 1)

	columns = []*model.ColumnInfo{
		{
			ID:        2,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
		{
			ID:        3,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
	}
	tblInfo = &model.TableInfo{
		ID: 1,
	}
	tblInfo.Columns = columns
	statsTbl = GetStatisticsTableCache(ctx, tblInfo)
	c.Check(len(statsTbl.Columns), Equals, 2)

	si, ok := statsTblCache.cache[tblInfo.ID]
	c.Assert(ok, IsTrue)
	oriLoadTime := si.loadTime - expireDuration
	si.loadTime = oriLoadTime
	GetStatisticsTableCache(ctx, tblInfo)
	si, ok = statsTblCache.cache[tblInfo.ID]
	c.Assert(ok, IsTrue)
	c.Check(si.loadTime, Greater, oriLoadTime)
}
