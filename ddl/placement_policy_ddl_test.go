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
package ddl

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
)

func testPlacementPolicyInfo(c *C, d *ddl, name string, settings *model.PlacementSettings) *model.PolicyInfo {
	policy := &model.PolicyInfo{
		Name:              model.NewCIStr(name),
		PlacementSettings: settings,
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	policy.ID = genIDs[0]
	return policy
}

func testCreatePlacementPolicy(c *C, ctx sessionctx.Context, d *ddl, policyInfo *model.PolicyInfo) *model.Job {
	job := &model.Job{
		SchemaName: policyInfo.Name.L,
		Type:       model.ActionCreatePlacementPolicy,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{policyInfo},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	policyInfo.State = model.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v})
	policyInfo.State = model.StateNone
	return job
}

func (s *testDDLSuite) TestPlacementPolicyInUse(c *C) {
	store := testCreateStore(c, "test_placement_policy_in_use")
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	ctx := context.Background()
	d := testNewDDLAndStart(ctx, c, WithStore(store))
	sctx := testNewContext(d)

	db1 := testSchemaInfo(c, d, "db1")
	testCreateSchema(c, sctx, d, db1)
	db2 := testSchemaInfo(c, d, "db2")
	testCreateSchema(c, sctx, d, db2)

	policySettings := &model.PlacementSettings{PrimaryRegion: "r1", Regions: "r1,r2"}
	p1 := testPlacementPolicyInfo(c, d, "p1", policySettings)
	p2 := testPlacementPolicyInfo(c, d, "p2", policySettings)
	p3 := testPlacementPolicyInfo(c, d, "p3", policySettings)
	testCreatePlacementPolicy(c, sctx, d, p1)
	testCreatePlacementPolicy(c, sctx, d, p2)
	testCreatePlacementPolicy(c, sctx, d, p3)

	t1 := testTableInfo(c, d, "t1", 1)
	t1.PlacementPolicyRef = &model.PolicyRefInfo{ID: p1.ID, Name: p1.Name}
	testCreateTable(c, sctx, d, db1, t1)

	t2 := testTableInfo(c, d, "t2", 1)
	t2.PlacementPolicyRef = &model.PolicyRefInfo{ID: p1.ID, Name: p1.Name}
	testCreateTable(c, sctx, d, db2, t2)

	t3 := testTableInfo(c, d, "t3", 1)
	t3.PlacementPolicyRef = &model.PolicyRefInfo{ID: p2.ID, Name: p2.Name}
	testCreateTable(c, sctx, d, db1, t3)

	c.Assert(kv.RunInNewTxn(ctx, sctx.GetStore(), false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)

		c.Assert(ErrPlacementPolicyInUse.Equal(checkPlacementPolicyNotInUseFromMeta(m, p1)), IsTrue)
		c.Assert(ErrPlacementPolicyInUse.Equal(checkPlacementPolicyNotInUseFromMeta(m, p2)), IsTrue)
		c.Assert(checkPlacementPolicyNotInUseFromMeta(m, p3), IsNil)
		return nil
	}), IsNil)
}
