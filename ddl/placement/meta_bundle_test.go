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

package placement_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

var _ = Suite(&testMetaBundleSuite{})

type testMetaBundleSuite struct {
	policy1 *model.PolicyInfo
	policy2 *model.PolicyInfo
	policy3 *model.PolicyInfo
	tbl1    *model.TableInfo
	tbl2    *model.TableInfo
	tbl3    *model.TableInfo
	tbl4    *model.TableInfo
}

func (s *testMetaBundleSuite) SetUpSuite(c *C) {
	s.policy1 = &model.PolicyInfo{
		ID:   11,
		Name: model.NewCIStr("p1"),
		PlacementSettings: &model.PlacementSettings{
			PrimaryRegion: "r1",
			Regions:       "r1,r2",
		},
		State: model.StatePublic,
	}
	s.policy2 = &model.PolicyInfo{
		ID:   12,
		Name: model.NewCIStr("p2"),
		PlacementSettings: &model.PlacementSettings{
			PrimaryRegion: "r2",
			Regions:       "r1,r2",
		},
		State: model.StatePublic,
	}
	s.policy3 = &model.PolicyInfo{
		ID:   13,
		Name: model.NewCIStr("p3"),
		PlacementSettings: &model.PlacementSettings{
			LeaderConstraints: "[+region=bj]",
		},
		State: model.StatePublic,
	}
	s.tbl1 = &model.TableInfo{
		ID:   101,
		Name: model.NewCIStr("t1"),
		PlacementPolicyRef: &model.PolicyRefInfo{
			ID:   11,
			Name: model.NewCIStr("p1"),
		},
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{
					ID:   1000,
					Name: model.NewCIStr("par0"),
				},
				{
					ID:                 1001,
					Name:               model.NewCIStr("par1"),
					PlacementPolicyRef: &model.PolicyRefInfo{ID: 12, Name: model.NewCIStr("p2")},
				},
				{
					ID:   1002,
					Name: model.NewCIStr("par2"),
				},
			},
		},
	}
	s.tbl2 = &model.TableInfo{
		ID:   102,
		Name: model.NewCIStr("t2"),
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{
					ID:                 1000,
					Name:               model.NewCIStr("par0"),
					PlacementPolicyRef: &model.PolicyRefInfo{ID: 11, Name: model.NewCIStr("p1")},
				},
				{
					ID:   1001,
					Name: model.NewCIStr("par1"),
				},
				{
					ID:   1002,
					Name: model.NewCIStr("par2"),
				},
			},
		},
	}
	s.tbl3 = &model.TableInfo{
		ID:                 103,
		Name:               model.NewCIStr("t3"),
		PlacementPolicyRef: &model.PolicyRefInfo{ID: 13, Name: model.NewCIStr("p3")},
	}
	s.tbl4 = &model.TableInfo{
		ID:   104,
		Name: model.NewCIStr("t4"),
	}
}

func (s *testMetaBundleSuite) prepareMeta(c *C, store kv.Storage) {
	c.Assert(kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		c.Assert(t.CreatePolicy(s.policy1), IsNil)
		c.Assert(t.CreatePolicy(s.policy2), IsNil)
		c.Assert(t.CreatePolicy(s.policy3), IsNil)
		return nil
	}), IsNil)
}

func (s *testMetaBundleSuite) TestNewTableBundle(c *C) {
	store := newMockStore(c)
	defer func() {
		c.Assert(store.Close(), IsNil)
	}()
	s.prepareMeta(c, store)
	c.Assert(kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)

		// tbl1
		bundle, err := placement.NewTableBundle(t, s.tbl1)
		c.Assert(err, IsNil)
		s.checkTableBundle(c, s.tbl1, bundle)

		// tbl2
		bundle, err = placement.NewTableBundle(t, s.tbl2)
		c.Assert(err, IsNil)
		s.checkTableBundle(c, s.tbl2, bundle)

		// tbl3
		bundle, err = placement.NewTableBundle(t, s.tbl3)
		c.Assert(err, IsNil)
		s.checkTableBundle(c, s.tbl3, bundle)

		// tbl4
		bundle, err = placement.NewTableBundle(t, s.tbl4)
		c.Assert(err, IsNil)
		s.checkTableBundle(c, s.tbl4, bundle)

		return nil
	}), IsNil)
}

func (s *testMetaBundleSuite) TestNewPartitionBundle(c *C) {
	store := newMockStore(c)
	defer func() {
		c.Assert(store.Close(), IsNil)
	}()
	s.prepareMeta(c, store)

	c.Assert(kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)

		// tbl1.par0
		bundle, err := placement.NewPartitionBundle(t, s.tbl1.Partition.Definitions[0])
		c.Assert(err, IsNil)
		s.checkPartitionBundle(c, s.tbl1.Partition.Definitions[0], bundle)

		// tbl1.par1
		bundle, err = placement.NewPartitionBundle(t, s.tbl1.Partition.Definitions[1])
		c.Assert(err, IsNil)
		s.checkPartitionBundle(c, s.tbl1.Partition.Definitions[1], bundle)

		return nil
	}), IsNil)
}

func (s *testMetaBundleSuite) TestNewPartitionListBundles(c *C) {
	store := newMockStore(c)
	defer func() {
		c.Assert(store.Close(), IsNil)
	}()
	s.prepareMeta(c, store)

	c.Assert(kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)

		bundles, err := placement.NewPartitionListBundles(t, s.tbl1.Partition.Definitions)
		c.Assert(err, IsNil)
		c.Assert(len(bundles), Equals, 1)
		s.checkPartitionBundle(c, s.tbl1.Partition.Definitions[1], bundles[0])

		bundles, err = placement.NewPartitionListBundles(t, []model.PartitionDefinition{})
		c.Assert(err, IsNil)
		c.Assert(len(bundles), Equals, 0)

		bundles, err = placement.NewPartitionListBundles(t, []model.PartitionDefinition{
			s.tbl1.Partition.Definitions[0],
			s.tbl1.Partition.Definitions[2],
		})
		c.Assert(err, IsNil)
		c.Assert(len(bundles), Equals, 0)

		return nil
	}), IsNil)
}

func (s *testMetaBundleSuite) TestNewFullTableBundles(c *C) {
	store := newMockStore(c)
	defer func() {
		c.Assert(store.Close(), IsNil)
	}()
	s.prepareMeta(c, store)

	c.Assert(kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)

		bundles, err := placement.NewFullTableBundles(t, s.tbl1)
		c.Assert(err, IsNil)
		c.Assert(len(bundles), Equals, 2)
		s.checkTableBundle(c, s.tbl1, bundles[0])
		s.checkPartitionBundle(c, s.tbl1.Partition.Definitions[1], bundles[1])

		bundles, err = placement.NewFullTableBundles(t, s.tbl2)
		c.Assert(err, IsNil)
		c.Assert(len(bundles), Equals, 1)
		s.checkPartitionBundle(c, s.tbl2.Partition.Definitions[0], bundles[0])

		bundles, err = placement.NewFullTableBundles(t, s.tbl3)
		c.Assert(err, IsNil)
		c.Assert(len(bundles), Equals, 1)
		s.checkTableBundle(c, s.tbl3, bundles[0])

		bundles, err = placement.NewFullTableBundles(t, s.tbl4)
		c.Assert(err, IsNil)
		c.Assert(len(bundles), Equals, 0)

		return nil
	}), IsNil)
}

func (s *testMetaBundleSuite) checkTwoJSONObjectEquals(c *C, expected interface{}, got interface{}) {
	expectedJSON, err := json.Marshal(expected)
	c.Assert(err, IsNil)
	expectedStr := string(expectedJSON)

	gotJSON, err := json.Marshal(got)
	c.Assert(err, IsNil)
	gotStr := string(gotJSON)

	c.Assert(gotStr, Equals, expectedStr)
}

func (s *testMetaBundleSuite) checkTableBundle(c *C, tbl *model.TableInfo, got *placement.Bundle) {
	if tbl.PlacementPolicyRef == nil {
		c.Assert(got, IsNil)
		return
	}

	expected := &placement.Bundle{
		ID:       fmt.Sprintf("TiDB_DDL_%d", tbl.ID),
		Index:    placement.RuleIndexTable,
		Override: true,
		Rules:    s.expectedRules(c, tbl.PlacementPolicyRef),
	}

	for idx, rule := range expected.Rules {
		rule.GroupID = expected.ID
		rule.Index = placement.RuleIndexTable
		rule.ID = fmt.Sprintf("table_rule_%d_%d", tbl.ID, idx)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tbl.ID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tbl.ID+1)))
	}

	if tbl.Partition != nil {
		for _, par := range tbl.Partition.Definitions {
			rules := s.expectedRules(c, tbl.PlacementPolicyRef)
			for idx, rule := range rules {
				rule.GroupID = expected.ID
				rule.Index = placement.RuleIndexPartition
				rule.ID = fmt.Sprintf("partition_rule_%d_%d", par.ID, idx)
				rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(par.ID)))
				rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(par.ID+1)))
				expected.Rules = append(expected.Rules, rule)
			}
		}
	}

	s.checkTwoJSONObjectEquals(c, expected, got)
}

func (s *testMetaBundleSuite) checkPartitionBundle(c *C, def model.PartitionDefinition, got *placement.Bundle) {
	if def.PlacementPolicyRef == nil {
		c.Assert(got, IsNil)
		return
	}

	expected := &placement.Bundle{
		ID:       fmt.Sprintf("TiDB_DDL_%d", def.ID),
		Index:    placement.RuleIndexPartition,
		Override: true,
		Rules:    s.expectedRules(c, def.PlacementPolicyRef),
	}

	for idx, rule := range expected.Rules {
		rule.GroupID = expected.ID
		rule.Index = placement.RuleIndexTable
		rule.ID = fmt.Sprintf("partition_rule_%d_%d", def.ID, idx)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(def.ID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(def.ID+1)))
	}

	s.checkTwoJSONObjectEquals(c, expected, got)
}

func (s *testMetaBundleSuite) expectedRules(c *C, ref *model.PolicyRefInfo) []*placement.Rule {
	if ref == nil {
		return []*placement.Rule{}
	}

	var policy *model.PolicyInfo
	switch ref.ID {
	case s.policy1.ID:
		policy = s.policy1
	case s.policy2.ID:
		policy = s.policy2
	case s.policy3.ID:
		policy = s.policy3
	default:
		c.FailNow()
	}
	c.Assert(ref.Name, Equals, policy.Name)
	settings := policy.PlacementSettings

	bundle, err := placement.NewBundleFromOptions(settings)
	c.Assert(err, IsNil)
	return bundle.Rules
}

func newMockStore(c *C) kv.Storage {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	return store
}
