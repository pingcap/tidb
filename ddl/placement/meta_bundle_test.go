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
	"testing"

	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

type metaBundleSuite struct {
	policy1 *model.PolicyInfo
	policy2 *model.PolicyInfo
	policy3 *model.PolicyInfo
	tbl1    *model.TableInfo
	tbl2    *model.TableInfo
	tbl3    *model.TableInfo
	tbl4    *model.TableInfo
}

func createMetaBundleSuite() *metaBundleSuite {
	s := new(metaBundleSuite)
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
	return s
}

func (s *metaBundleSuite) prepareMeta(t *testing.T, store kv.Storage) {
	err := kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		require.NoError(t, m.CreatePolicy(s.policy1))
		require.NoError(t, m.CreatePolicy(s.policy2))
		require.NoError(t, m.CreatePolicy(s.policy3))
		return nil
	})
	require.NoError(t, err)
}

func TestNewTableBundle(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	s := createMetaBundleSuite()
	s.prepareMeta(t, store)
	require.NoError(t, kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)

		// tbl1
		bundle, err := placement.NewTableBundle(m, s.tbl1)
		require.NoError(t, err)
		s.checkTableBundle(t, s.tbl1, bundle)

		// tbl2
		bundle, err = placement.NewTableBundle(m, s.tbl2)
		require.NoError(t, err)
		s.checkTableBundle(t, s.tbl2, bundle)

		// tbl3
		bundle, err = placement.NewTableBundle(m, s.tbl3)
		require.NoError(t, err)
		s.checkTableBundle(t, s.tbl3, bundle)

		// tbl4
		bundle, err = placement.NewTableBundle(m, s.tbl4)
		require.NoError(t, err)
		s.checkTableBundle(t, s.tbl4, bundle)

		return nil
	}))
}

func TestNewPartitionBundle(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	s := createMetaBundleSuite()
	s.prepareMeta(t, store)

	require.NoError(t, kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)

		// tbl1.par0
		bundle, err := placement.NewPartitionBundle(m, s.tbl1.Partition.Definitions[0])
		require.NoError(t, err)
		s.checkPartitionBundle(t, s.tbl1.Partition.Definitions[0], bundle)

		// tbl1.par1
		bundle, err = placement.NewPartitionBundle(m, s.tbl1.Partition.Definitions[1])
		require.NoError(t, err)
		s.checkPartitionBundle(t, s.tbl1.Partition.Definitions[1], bundle)

		return nil
	}))
}

func TestNewPartitionListBundles(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	s := createMetaBundleSuite()
	s.prepareMeta(t, store)

	require.NoError(t, kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)

		bundles, err := placement.NewPartitionListBundles(m, s.tbl1.Partition.Definitions)
		require.NoError(t, err)
		require.Len(t, bundles, 1)
		s.checkPartitionBundle(t, s.tbl1.Partition.Definitions[1], bundles[0])

		bundles, err = placement.NewPartitionListBundles(m, []model.PartitionDefinition{})
		require.NoError(t, err)
		require.Len(t, bundles, 0)

		bundles, err = placement.NewPartitionListBundles(m, []model.PartitionDefinition{
			s.tbl1.Partition.Definitions[0],
			s.tbl1.Partition.Definitions[2],
		})
		require.NoError(t, err)
		require.Len(t, bundles, 0)

		return nil
	}))
}

func TestNewFullTableBundles(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	s := createMetaBundleSuite()
	s.prepareMeta(t, store)

	require.NoError(t, kv.RunInNewTxn(context.TODO(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)

		bundles, err := placement.NewFullTableBundles(m, s.tbl1)
		require.NoError(t, err)
		require.Len(t, bundles, 2)
		s.checkTableBundle(t, s.tbl1, bundles[0])
		s.checkPartitionBundle(t, s.tbl1.Partition.Definitions[1], bundles[1])

		bundles, err = placement.NewFullTableBundles(m, s.tbl2)
		require.NoError(t, err)
		require.Len(t, bundles, 1)
		s.checkPartitionBundle(t, s.tbl2.Partition.Definitions[0], bundles[0])

		bundles, err = placement.NewFullTableBundles(m, s.tbl3)
		require.NoError(t, err)
		require.Len(t, bundles, 1)
		s.checkTableBundle(t, s.tbl3, bundles[0])

		bundles, err = placement.NewFullTableBundles(m, s.tbl4)
		require.NoError(t, err)
		require.Len(t, bundles, 0)

		return nil
	}))
}

func (s *metaBundleSuite) checkTwoJSONObjectEquals(t *testing.T, expected interface{}, got interface{}) {
	expectedJSON, err := json.Marshal(expected)
	require.NoError(t, err)
	expectedStr := string(expectedJSON)

	gotJSON, err := json.Marshal(got)
	require.NoError(t, err)
	gotStr := string(gotJSON)

	require.Equal(t, expectedStr, gotStr)
}

func (s *metaBundleSuite) checkTableBundle(t *testing.T, tbl *model.TableInfo, got *placement.Bundle) {
	if tbl.PlacementPolicyRef == nil {
		require.Nil(t, got)
		return
	}

	expected := &placement.Bundle{
		ID:       fmt.Sprintf("TiDB_DDL_%d", tbl.ID),
		Index:    placement.RuleIndexTable,
		Override: true,
		Rules:    s.expectedRules(t, tbl.PlacementPolicyRef),
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
			rules := s.expectedRules(t, tbl.PlacementPolicyRef)
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

	s.checkTwoJSONObjectEquals(t, expected, got)
}

func (s *metaBundleSuite) checkPartitionBundle(t *testing.T, def model.PartitionDefinition, got *placement.Bundle) {
	if def.PlacementPolicyRef == nil {
		require.Nil(t, got)
		return
	}

	expected := &placement.Bundle{
		ID:       fmt.Sprintf("TiDB_DDL_%d", def.ID),
		Index:    placement.RuleIndexPartition,
		Override: true,
		Rules:    s.expectedRules(t, def.PlacementPolicyRef),
	}

	for idx, rule := range expected.Rules {
		rule.GroupID = expected.ID
		rule.Index = placement.RuleIndexTable
		rule.ID = fmt.Sprintf("partition_rule_%d_%d", def.ID, idx)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(def.ID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(def.ID+1)))
	}

	s.checkTwoJSONObjectEquals(t, expected, got)
}

func (s *metaBundleSuite) expectedRules(t *testing.T, ref *model.PolicyRefInfo) []*placement.Rule {
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
		t.FailNow()
	}

	require.Equal(t, policy.Name, ref.Name)
	settings := policy.PlacementSettings

	bundle, err := placement.NewBundleFromOptions(settings)
	require.NoError(t, err)

	return bundle.Rules
}
