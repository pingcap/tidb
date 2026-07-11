// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
)

func TestCheckRuleWithKeyspaceID(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("keyspace-prefixed label rule IDs are parsed only in nextgen")
	}

	newRule := func(id string) *label.Rule {
		return &label.Rule{
			ID:       id,
			RuleType: "key-range",
			Labels:   []pd.RegionLabel{{Key: "key", Value: "value"}},
			Data: []any{
				map[string]any{
					"start_key": "aa",
					"end_key":   "bb",
				},
			},
		}
	}

	tests := []struct {
		name          string
		ruleID        string
		wantDB        string
		wantTable     string
		wantPartition string
		wantErr       bool
	}{
		{
			name:    "invalid non-keyspace prefix",
			ruleID:  "foo/test/t1",
			wantErr: true,
		},
		{
			name:    "missing table name",
			ruleID:  fmt.Sprintf("%s/test", label.IDPrefix),
			wantErr: true,
		},
		{
			name:      "non-keyspace table",
			ruleID:    fmt.Sprintf("%s/test/t1", label.IDPrefix),
			wantDB:    "test",
			wantTable: "t1",
		},
		{
			name:    "invalid keyspace inner prefix",
			ruleID:  fmt.Sprintf("%s/42/foo/test/t2", label.KeyspacePrefix),
			wantErr: true,
		},
		{
			name:    "missing keyspace table name",
			ruleID:  fmt.Sprintf("%s/42/%s/test", label.KeyspacePrefix, label.IDPrefix),
			wantErr: true,
		},
		{
			name:      "keyspace table",
			ruleID:    fmt.Sprintf("%s/42/%s/test/t2", label.KeyspacePrefix, label.IDPrefix),
			wantDB:    "test",
			wantTable: "t2",
		},
		{
			name:          "keyspace partition",
			ruleID:        fmt.Sprintf("%s/42/%s/test/t3/p0", label.KeyspacePrefix, label.IDPrefix),
			wantDB:        "test",
			wantTable:     "t3",
			wantPartition: "p0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbName, tableName, partitionName, err := checkRule(newRule(tc.ruleID))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantDB, dbName)
			require.Equal(t, tc.wantTable, tableName)
			require.Equal(t, tc.wantPartition, partitionName)
		})
	}

	codecV2, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: 42})
	require.NoError(t, err)
	rule := &label.Rule{Labels: []pd.RegionLabel{{Key: "merge_option", Value: "allow"}}}
	rule.Reset(codecV2, "test", "t4", "", 123)
	data := rule.Data.([]any)[0].(map[string]string)
	rule.Data = []any{map[string]any{
		"start_key": data["start_key"],
		"end_key":   data["end_key"],
	}}
	tableID, err := decodeTableIDFromRule(rule)
	require.NoError(t, err)
	require.Equal(t, int64(123), tableID)
}
