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
	gjson "encoding/json"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/sqlexec"
)

type showPlacementLabelsResultBuilder struct {
	labelKey2values map[string]interface{}
}

func (b *showPlacementLabelsResultBuilder) AppendStoreLabels(bj json.BinaryJSON) error {
	if b.labelKey2values == nil {
		b.labelKey2values = make(map[string]interface{})
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return errors.Trace(err)
	}

	if string(data) == "null" {
		return nil
	}

	if bj.TypeCode != json.TypeCodeArray {
		return errors.New("only array or null type is allowed")
	}

	labels := make([]*helper.StoreLabel, 0, bj.GetElemCount())
	err = gjson.Unmarshal(data, &labels)
	if err != nil {
		return errors.Trace(err)
	}

	for _, label := range labels {
		if values, ok := b.labelKey2values[label.Key]; ok {
			values.(map[string]interface{})[label.Value] = true
		} else {
			b.labelKey2values[label.Key] = map[string]interface{}{label.Value: true}
		}
	}

	return nil
}

func (b *showPlacementLabelsResultBuilder) BuildRows() ([][]interface{}, error) {
	rows := make([][]interface{}, 0, len(b.labelKey2values))
	for _, key := range b.sortMapKeys(b.labelKey2values) {
		values := b.sortMapKeys(b.labelKey2values[key].(map[string]interface{}))
		d, err := gjson.Marshal(values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		valuesJSON := json.BinaryJSON{}
		err = valuesJSON.UnmarshalJSON(d)
		if err != nil {
			return nil, errors.Trace(err)
		}

		rows = append(rows, []interface{}{key, valuesJSON})
	}

	return rows, nil
}

func (b *showPlacementLabelsResultBuilder) sortMapKeys(m map[string]interface{}) []string {
	sorted := make([]string, 0, len(m))
	for key := range m {
		sorted = append(sorted, key)
	}

	sort.Strings(sorted)
	return sorted
}

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

	b := &showPlacementLabelsResultBuilder{}
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
		e.appendRow([]interface{}{name.String(), settings.String(), "SCHEDULED"})
	}

	return nil
}
