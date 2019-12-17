// Copyright 2015 PingCAP, Inc.
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

package domain

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	dto "github.com/prometheus/client_model/go"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func mockFactory() (pools.Resource, error) {
	return nil, errors.New("mock factory should not be called")
}

func sysMockFactory(dom *Domain) (pools.Resource, error) {
	return nil, nil
}

func (*testSuite) TestT(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	ddlLease := 80 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, mockFactory)
	err = dom.Init(ddlLease, sysMockFactory)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
	}()
	store = dom.Store()
	ctx := mock.NewContext()
	ctx.Store = store
	snapTS := oracle.EncodeTSO(oracle.GetPhysical(time.Now()))
	dd := dom.DDL()
	c.Assert(dd, NotNil)
	c.Assert(dd.GetLease(), Equals, 80*time.Millisecond)
	cs := &ast.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}
	err = dd.CreateSchema(ctx, model.NewCIStr("aaa"), cs)
	c.Assert(err, IsNil)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)

	// for setting lease
	lease := 100 * time.Millisecond

	// for updating the self schema version
	goCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	c.Assert(err, IsNil)
	snapIs, err := dom.GetSnapshotInfoSchema(snapTS)
	c.Assert(snapIs, NotNil)
	c.Assert(err, IsNil)
	// Make sure that the self schema version doesn't be changed.
	goCtx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	c.Assert(err, IsNil)

	// for GetSnapshotInfoSchema
	snapTS = oracle.EncodeTSO(oracle.GetPhysical(time.Now()))
	snapIs, err = dom.GetSnapshotInfoSchema(snapTS)
	c.Assert(err, IsNil)
	c.Assert(snapIs, NotNil)
	c.Assert(snapIs.SchemaMetaVersion(), Equals, is.SchemaMetaVersion())

	// for schemaValidator
	schemaVer := dom.SchemaValidator.(*schemaValidator).LatestSchemaVersion()
	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	ts := ver.Ver

	succ := dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	dom.MockReloadFailed.SetValue(true)
	err = dom.Reload()
	c.Assert(err, NotNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	time.Sleep(lease)

	ver, err = store.CurrentVersion()
	c.Assert(err, IsNil)
	ts = ver.Ver
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultUnknown)
	dom.MockReloadFailed.SetValue(false)
	err = dom.Reload()
	c.Assert(err, IsNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)

	// For slow query.
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	// Collecting slow queries is asynchronous, wait a while to ensure it's done.
	time.Sleep(5 * time.Millisecond)

	res := dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2})
	c.Assert(res, HasLen, 2)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	c.Assert(*res[1], Equals, SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2, Kind: ast.ShowSlowKindInternal})
	c.Assert(res, HasLen, 1)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 4, Kind: ast.ShowSlowKindAll})
	c.Assert(res, HasLen, 3)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	c.Assert(*res[1], Equals, SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	c.Assert(*res[2], Equals, SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowRecent, Count: 2})
	c.Assert(res, HasLen, 2)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	c.Assert(*res[1], Equals, SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})

	metrics.PanicCounter.Reset()
	// Since the stats lease is 0 now, so create a new ticker will panic.
	// Test that they can recover from panic correctly.
	dom.updateStatsWorker(ctx, nil)
	dom.autoAnalyzeWorker(nil)
	counter := metrics.PanicCounter.WithLabelValues(metrics.LabelDomain)
	pb := &dto.Metric{}
	counter.Write(pb)
	c.Assert(pb.GetCounter().GetValue(), Equals, float64(2))

	err = store.Close()
	c.Assert(err, IsNil)
}
