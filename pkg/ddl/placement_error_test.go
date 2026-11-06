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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	stderrors "errors"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client/http"
)

func TestEnhancePlacementRuleError(t *testing.T) {
	pdMsg := "[PD:placement:ErrRuleContent]invalid rule content, rule 'table_rule_100_2' from rule group 'TiDB_DDL_100' can not match any store"
	err := infosync.ErrHTTPServiceError.FastGen("%s", pdMsg+"\n")
	bundle := &placement.Bundle{
		ID: "TiDB_DDL_100",
		Rules: []*pd.Rule{
			{
				GroupID: "TiDB_DDL_100",
				ID:      "table_rule_100_2",
				Role:    pd.Voter,
				Count:   1,
				LabelConstraints: []pd.LabelConstraint{
					{Key: "region", Op: pd.In, Values: []string{"us-west"}},
					{Key: "engine", Op: pd.NotIn, Values: []string{"tiflash"}},
				},
			},
		},
	}

	enhancedErr := placementRuleErrorWithContext(err, placementRuleErrorContext{
		SchemaName: "test",
		TableName:  "t",
		PolicyName: "mydeploy",
		Bundles:    []*placement.Bundle{bundle},
	})

	require.True(t, infosync.ErrHTTPServiceError.Equal(enhancedErr))
	terrorErr, ok := errors.Cause(enhancedErr).(*errors.Error)
	require.True(t, ok)
	sqlErr := terror.ToSQLError(terrorErr)
	require.Equal(t, uint16(errno.ErrHTTPServiceError), sqlErr.Code)
	require.Contains(t, sqlErr.Message, pdMsg)
	require.Contains(t, sqlErr.Message, "table=`test`.`t`")
	require.Contains(t, sqlErr.Message, "policy=`mydeploy`")
	require.Contains(t, sqlErr.Message, "rule_group=`TiDB_DDL_100`")
	require.Contains(t, sqlErr.Message, "rule=`table_rule_100_2`")
	require.Contains(t, sqlErr.Message, "role=voter")
	require.Contains(t, sqlErr.Message, "count=1")
	require.Contains(t, sqlErr.Message, "label_constraints=[region in (us-west), engine notIn (tiflash)]")
	require.Contains(t, sqlErr.Message, "No store matches the failed rule constraints")
	require.Contains(t, sqlErr.Message, "SHOW PLACEMENT LABELS")
}

func TestEnhancePlacementRuleErrorWithoutNoStoreHint(t *testing.T) {
	pdMsg := "[PD:placement:ErrRuleContent]invalid rule content, rule 'table_rule_100_2' from rule group 'TiDB_DDL_100' is invalid"
	err := infosync.ErrHTTPServiceError.FastGen("%s", pdMsg)
	bundle := &placement.Bundle{
		ID: "TiDB_DDL_100",
		Rules: []*pd.Rule{
			{GroupID: "TiDB_DDL_100", ID: "table_rule_100_2", Role: pd.Leader, Count: 1},
		},
	}

	enhancedErr := placementRuleErrorWithContext(err, placementRuleErrorContext{
		SchemaName: "test",
		TableName:  "t",
		PolicyName: "mydeploy",
		Bundles:    []*placement.Bundle{bundle},
	})

	require.True(t, infosync.ErrHTTPServiceError.Equal(enhancedErr))
	terrorErr, ok := errors.Cause(enhancedErr).(*errors.Error)
	require.True(t, ok)
	require.Contains(t, terrorErr.GetMsg(), "TiDB placement context")
	require.NotContains(t, terrorErr.GetMsg(), "No store matches the failed rule constraints")
}

func TestEnhancePlacementRuleErrorRuleNotFound(t *testing.T) {
	pdMsg := "[PD:placement:ErrRuleContent]invalid rule content, rule 'table_rule_100_2' from rule group 'TiDB_DDL_100' can not match any store"
	err := infosync.ErrHTTPServiceError.FastGen("%s", pdMsg)

	enhancedErr := placementRuleErrorWithContext(err, placementRuleErrorContext{
		SchemaName: "test",
		TableName:  "t",
		PolicyName: "mydeploy",
		Bundles: []*placement.Bundle{{
			ID:    "TiDB_DDL_100",
			Rules: []*pd.Rule{{GroupID: "TiDB_DDL_100", ID: "table_rule_100_1", Role: pd.Voter, Count: 1}},
		}},
	})

	require.True(t, infosync.ErrHTTPServiceError.Equal(enhancedErr))
	terrorErr, ok := errors.Cause(enhancedErr).(*errors.Error)
	require.True(t, ok)
	require.Contains(t, terrorErr.GetMsg(), "rule detail not found in current TiDB bundle")
	require.Contains(t, terrorErr.GetMsg(), "rule=`table_rule_100_2`")
}

func TestEnhancePlacementRuleErrorKeepOtherErrors(t *testing.T) {
	err := stderrors.New("not pd http service error")
	require.Equal(t, err, placementRuleErrorWithContext(err, placementRuleErrorContext{}))

	pdErr := infosync.ErrHTTPServiceError.FastGen("%s", "pd http service error without rule id")
	require.Equal(t, pdErr, placementRuleErrorWithContext(pdErr, placementRuleErrorContext{}))
}
