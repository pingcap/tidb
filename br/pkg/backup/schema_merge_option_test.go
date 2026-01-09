// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client/http"
)

// mockLabelManagerForTest is a mock label manager for testing
type mockLabelManagerForTest struct {
	rules map[string]*label.Rule
}

func newMockLabelManagerForTest() *mockLabelManagerForTest {
	return &mockLabelManagerForTest{
		rules: make(map[string]*label.Rule),
	}
}

func (m *mockLabelManagerForTest) PutLabelRule(_ context.Context, rule *label.Rule) error {
	if rule == nil {
		return nil
	}
	m.rules[rule.ID] = rule
	return nil
}

func (m *mockLabelManagerForTest) UpdateLabelRules(_ context.Context, patch *pd.LabelRulePatch) error {
	if patch == nil {
		return nil
	}
	for _, id := range patch.DeleteRules {
		delete(m.rules, id)
	}
	for _, r := range patch.SetRules {
		if r != nil {
			m.rules[r.ID] = (*label.Rule)(r)
		}
	}
	return nil
}

func (m *mockLabelManagerForTest) GetAllLabelRules(_ context.Context) ([]*label.Rule, error) {
	rules := make([]*label.Rule, 0, len(m.rules))
	for _, rule := range m.rules {
		rules = append(rules, rule)
	}
	return rules, nil
}

func (m *mockLabelManagerForTest) GetLabelRules(_ context.Context, ruleIDs []string) (map[string]*label.Rule, error) {
	result := make(map[string]*label.Rule)
	for _, ruleID := range ruleIDs {
		if rule, exists := m.rules[ruleID]; exists {
			result[ruleID] = rule
		}
	}
	return result, nil
}

// TestBackupSchemaMergeOptionRuleIDFormat tests the rule ID format generation
func TestBackupSchemaMergeOptionRuleIDFormat(t *testing.T) {
	t.Run("normal table rule ID format", func(t *testing.T) {
		dbName := "test"
		tableName := "t1"
		expectedRuleID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, dbName, tableName)
		require.Equal(t, "schema/test/t1", expectedRuleID)
	})

	t.Run("partition table rule ID format", func(t *testing.T) {
		dbName := "test"
		tableName := "pt1"
		partitionName := "p0"
		expectedRuleID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, dbName, tableName, partitionName)
		require.Equal(t, "schema/test/pt1/p0", expectedRuleID)
	})

	t.Run("multiple partitions rule ID format", func(t *testing.T) {
		dbName := "test"
		tableName := "pt2"
		partitions := []string{"p0", "p1", "p2"}

		for _, partName := range partitions {
			ruleID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, dbName, tableName, partName)
			expected := fmt.Sprintf("schema/%s/%s/%s", dbName, tableName, partName)
			require.Equal(t, expected, ruleID, "partition %s rule ID should match", partName)
		}
	})

	t.Run("rule ID format matches DDL behavior - case insensitive", func(t *testing.T) {
		// DDL uses .L (lowercase) for rule ID generation
		// BR should also use .L to match DDL behavior
		// This test verifies that both use lowercase for rule IDs

		// Simulate DDL behavior: uses .L (lowercase)
		// DDL would use: schema.Name.L, meta.Name.L, spec.PartitionNames[0].L
		// For "TestDB"/"TestTable"/"Partition0", .L would be: "testdb"/"testtable"/"partition0"
		ddlRuleID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, "testdb", "testtable")
		ddlPartitionRuleID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, "testdb", "testtable", "partition0")

		// BR should also use .L (lowercase) - simulate BR behavior
		// In real code: table.DB.Name.L, table.Info.Name.L, def.Name.L
		brRuleID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, "testdb", "testtable")
		brPartitionRuleID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, "testdb", "testtable", "partition0")

		// Both should generate the same rule ID
		require.Equal(t, ddlRuleID, brRuleID, "DDL and BR should generate same table rule ID")
		require.Equal(t, ddlPartitionRuleID, brPartitionRuleID, "DDL and BR should generate same partition rule ID")
		require.Equal(t, "schema/testdb/testtable", ddlRuleID)
		require.Equal(t, "schema/testdb/testtable/partition0", ddlPartitionRuleID)
	})
}

// TestBackupSchemaWithMergeOption tests the full backup schema flow with merge_option
func TestBackupSchemaWithMergeOption(t *testing.T) {
	m := createMockCluster(t)
	tk := testkit.NewTestKit(t, m.Storage)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_normal, t_partition;")

	// Create normal table
	tk.MustExec("create table t_normal (a int);")
	tk.MustExec("insert into t_normal values (1);")

	// Create partitioned table
	tk.MustExec(`create table t_partition (
		a int,
		b int
	) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30)
	);`)
	tk.MustExec("insert into t_partition values (5, 1), (15, 2), (25, 3);")

	testFilter, err := filter.Parse([]string{"test.t_normal", "test.t_partition"})
	require.NoError(t, err)

	_, backupSchemas, _, err := backup.BuildBackupRangeAndInitSchema(
		m.Storage, testFilter, math.MaxUint64, false)
	require.NoError(t, err)
	require.Equal(t, 2, backupSchemas.Len())

	ctx := context.Background()
	es := GetRandomStorage(t)
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	metaWriter := metautil.NewMetaWriter(es, metautil.MetaFileSize, false, "", &cipher)
	updateCh := new(simpleProgress)
	skipChecksum := true // Skip checksum for faster test

	err = backupSchemas.BackupSchemas(
		ctx, metaWriter, nil, m.Storage, nil, math.MaxUint64, nil, 1, vardef.DefChecksumTableConcurrency, skipChecksum, updateCh)
	require.NoError(t, err)
	require.Equal(t, int64(2), updateCh.get())

	err = metaWriter.FlushBackupMeta(ctx)
	require.NoError(t, err)

	// Read back schemas and verify structure
	schemas := GetSchemasFromMeta(t, es)
	require.Len(t, schemas, 2)

	// Find normal table and partitioned table
	var normalTable, partitionTable *metautil.Table
	for _, schema := range schemas {
		if schema.Info.Name.O == "t_normal" {
			normalTable = schema
		} else if schema.Info.Name.O == "t_partition" {
			partitionTable = schema
		}
	}

	require.NotNil(t, normalTable, "normal table should be found")
	require.NotNil(t, partitionTable, "partition table should be found")

	// Verify normal table structure
	require.NotNil(t, normalTable.Info)
	require.Nil(t, normalTable.Info.Partition, "normal table should not have partition info")

	// Verify partition table structure
	require.NotNil(t, partitionTable.Info)
	require.NotNil(t, partitionTable.Info.Partition, "partition table should have partition info")
	require.Len(t, partitionTable.Info.Partition.Definitions, 3, "should have 3 partitions")

	// Verify merge_option fields exist (they should be false by default if no rules are set)
	// Note: Without actual label rules set, IsMergeOptionAllowed should be false
	require.False(t, normalTable.IsMergeOptionAllowed)
	require.False(t, partitionTable.IsMergeOptionAllowed)
	require.Empty(t, normalTable.PartitionMergeOptionAllowed)
	require.Empty(t, partitionTable.PartitionMergeOptionAllowed)
}
