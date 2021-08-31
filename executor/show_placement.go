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

package executor

import (
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/util/placementpolicy"
	"github.com/pingcap/tidb/util/sqlexec"
)

func (e *ShowExec) fetchShowPlacementLabels(ctx context.Context) error {
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(ctx, "SELECT DISTINCT LABEL FROM %n.%n", "INFORMATION_SCHEMA", infoschema.TableTiKVStoreStatus)
	if err != nil {
		return errors.Trace(err)
	}

	rows, _, err := exec.ExecRestrictedStmt(ctx, stmt)
	if err != nil {
		return errors.Trace(err)
	}

	b := &placementpolicy.ShowPlacementLabelsResultBuilder{}
	for _, row := range rows {
		bj := row.GetJSON(0)
		if err := b.AppendStoreLabels(bj); err != nil {
			return err
		}
	}

	result, err := b.BuildRows()
	if err != nil {
		return err
	}

	for _, row := range result {
		e.appendRow(row)
	}

	return nil
}

func (e *ShowExec) fetchShowPlacement(_ context.Context) error {
	err := e.fetchAllPlacementPolicies()
	if err != nil {
		return err
	}

	return nil
}

func (e *ShowExec) fetchAllPlacementPolicies() error {
	policies := e.is.AllPlacementPolicies()
	sort.Slice(policies, func(i, j int) bool { return policies[i].Name.O < policies[j].Name.O })
	for _, policy := range policies {
		name := policy.Name
		settings := policy.PlacementSettings
		e.appendRow([]interface{}{"POLICY " + name.String(), settings.String(), "SCHEDULED"})
	}

	return nil
}
