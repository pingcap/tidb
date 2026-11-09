package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestParsePhysicalIDFromRuleID(t *testing.T) {
	tests := []struct {
		name     string
		ruleID   string
		expected int64
		hasErr   bool
	}{
		{
			name:     "v1 format",
			ruleID:   infosync.MakeRuleID(123),
			expected: 123,
		},
		{
			name:     "v2 format with keyspace",
			ruleID:   "keyspace-1-" + infosync.MakeRuleID(456),
			expected: 456,
		},
		{
			name:   "no table prefix",
			ruleID: "something-else",
			hasErr: true,
		},
		{
			name:   "missing -r suffix",
			ruleID: "table-123",
			hasErr: true,
		},
		{
			name:   "empty string",
			ruleID: "",
			hasErr: true,
		},
		{
			name:     "large ID",
			ruleID:   infosync.MakeRuleID(9223372036854775807),
			expected: 9223372036854775807,
		},
		{
			name:     "zero ID",
			ruleID:   infosync.MakeRuleID(0),
			expected: 0,
		},
		{
			name:   "non-numeric ID",
			ruleID: "table-abc-r",
			hasErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := parsePhysicalIDFromRuleID(tt.ruleID)
			if tt.hasErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, id)
			}
		})
	}
}

func TestShouldDeleteTiFlashRule(t *testing.T) {
	tests := []struct {
		name       string
		schema     infoschema.InfoSchema
		physicalID int64
		expected   bool
	}{
		{
			name:       "live table without tiflash replica should delete",
			schema:     infoschema.MockInfoSchema([]*model.TableInfo{newTestTiFlashTableInfo(100, "t_no_replica", nil, nil)}),
			physicalID: 100,
			expected:   true,
		},
		{
			name:       "live table with tiflash replica should keep",
			schema:     infoschema.MockInfoSchema([]*model.TableInfo{newTestTiFlashTableInfo(101, "t_with_replica", &model.TiFlashReplicaInfo{Count: 2}, nil)}),
			physicalID: 101,
			expected:   false,
		},
		{
			name: "live partition without tiflash replica should delete",
			schema: infoschema.MockInfoSchema([]*model.TableInfo{newTestTiFlashTableInfo(
				102,
				"pt_no_replica",
				nil,
				[]model.PartitionDefinition{{ID: 1001, Name: pmodel.NewCIStr("p0")}},
			)}),
			physicalID: 1001,
			expected:   true,
		},
		{
			name: "live partition with tiflash replica should keep",
			schema: infoschema.MockInfoSchema([]*model.TableInfo{newTestTiFlashTableInfo(
				103,
				"pt_with_replica",
				&model.TiFlashReplicaInfo{Count: 1},
				[]model.PartitionDefinition{{ID: 1002, Name: pmodel.NewCIStr("p0")}},
			)}),
			physicalID: 1002,
			expected:   false,
		},
		{
			name:       "missing physical id should keep for gc path",
			schema:     infoschema.MockInfoSchema(nil),
			physicalID: 1234567,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, shouldDeleteTiFlashRule(context.Background(), tt.schema, tt.physicalID))
		})
	}
}

func TestSyncTiFlashPlacementRules(t *testing.T) {
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() {
		infosync.SetMockTiFlash(nil)
		tiflash.Lock()
		if tiflash.StatusServer != nil {
			tiflash.StatusServer.Close()
		}
		tiflash.Unlock()
	})

	liveReplica := newTestTiFlashTableInfo(300, "t_live", &model.TiFlashReplicaInfo{
		Count:          2,
		LocationLabels: []string{"zone"},
	}, nil)
	liveNoReplica := newTestTiFlashTableInfo(301, "t_drop_rule", nil, nil)

	existingNoReplicaRule := infosync.MakeNewRule(301, 1, nil)
	require.NoError(t, tiflash.HandleSetPlacementRule(&existingNoReplicaRule))
	missingPhysicalRule := infosync.MakeNewRule(399, 1, nil)
	require.NoError(t, tiflash.HandleSetPlacementRule(&missingPhysicalRule))

	schema := infoschema.MockInfoSchema([]*model.TableInfo{liveReplica, liveNoReplica})
	require.NoError(t, syncTiFlashPlacementRules(context.Background(), schema))

	rule, ok := tiflash.GetPlacementRule(infosync.MakeRuleID(300))
	require.True(t, ok)
	require.Equal(t, 2, rule.Count)
	require.Equal(t, []string{"zone"}, rule.LocationLabels)

	_, ok = tiflash.GetPlacementRule(infosync.MakeRuleID(301))
	require.False(t, ok)

	rule, ok = tiflash.GetPlacementRule(infosync.MakeRuleID(399))
	require.True(t, ok)
	require.Equal(t, 1, rule.Count)
}

func TestSyncTiFlashPlacementRulesUpdatesLocationLabels(t *testing.T) {
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() {
		infosync.SetMockTiFlash(nil)
		tiflash.Lock()
		if tiflash.StatusServer != nil {
			tiflash.StatusServer.Close()
		}
		tiflash.Unlock()
	})

	tableReplica := newTestTiFlashTableInfo(400, "t_labels", &model.TiFlashReplicaInfo{
		Count:          2,
		LocationLabels: []string{"zone", "rack"},
	}, nil)
	partitionReplica := newTestTiFlashTableInfo(401, "pt_labels", &model.TiFlashReplicaInfo{
		Count:          2,
		LocationLabels: []string{"zone", "rack"},
	}, []model.PartitionDefinition{
		{ID: 4001, Name: pmodel.NewCIStr("p0")},
		{ID: 4002, Name: pmodel.NewCIStr("p1")},
	})

	staleTableRule := infosync.MakeNewRule(400, 2, []string{"host"})
	require.NoError(t, tiflash.HandleSetPlacementRule(&staleTableRule))
	stalePartitionRule0 := infosync.MakeNewRule(4001, 2, []string{"host"})
	require.NoError(t, tiflash.HandleSetPlacementRule(&stalePartitionRule0))
	stalePartitionRule1 := infosync.MakeNewRule(4002, 2, []string{"host"})
	require.NoError(t, tiflash.HandleSetPlacementRule(&stalePartitionRule1))

	schema := infoschema.MockInfoSchema([]*model.TableInfo{tableReplica, partitionReplica})
	require.NoError(t, syncTiFlashPlacementRules(context.Background(), schema))

	for _, id := range []int64{400, 4001, 4002} {
		rule, ok := tiflash.GetPlacementRule(infosync.MakeRuleID(id))
		require.True(t, ok)
		require.Equal(t, 2, rule.Count)
		require.Equal(t, []string{"zone", "rack"}, rule.LocationLabels)
	}
}

func newTestTiFlashTableInfo(
	id int64,
	name string,
	replica *model.TiFlashReplicaInfo,
	partitions []model.PartitionDefinition,
) *model.TableInfo {
	tblInfo := &model.TableInfo{
		ID:    id,
		Name:  pmodel.NewCIStr(name),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{{
			State:     model.StatePublic,
			Offset:    0,
			Name:      pmodel.NewCIStr("c"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
			ID:        1,
		}},
		TiFlashReplica: replica,
	}
	if len(partitions) > 0 {
		tblInfo.Partition = &model.PartitionInfo{
			Enable:      true,
			Definitions: partitions,
		}
	}
	return tblInfo
}
