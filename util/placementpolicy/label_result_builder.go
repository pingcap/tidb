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

package placementpolicy

import (
	"sort"

	gjson "encoding/json"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/types/json"
)

// CheckPolicyLabelsResultBuilder is used to fetch the `show placement labels` result.
type CheckPolicyLabelsResultBuilder struct {
	labelKey2ValueList map[string][]string
}

// AppendShowLabels is used to fill the LabelKey and valuesList into the builder.
func (b *CheckPolicyLabelsResultBuilder) AppendShowLabels(key string, valueList json.BinaryJSON) error {
	if b.labelKey2ValueList == nil {
		b.labelKey2ValueList = make(map[string][]string)
	}

	data, err := valueList.MarshalJSON()
	if err != nil {
		return errors.Trace(err)
	}

	if string(data) == "null" {
		return nil
	}

	if valueList.TypeCode != json.TypeCodeArray {
		return errors.New("only array or null type is allowed")
	}
	labels := make([]string, 0, valueList.GetElemCount())
	err = gjson.Unmarshal(data, &labels)
	if err != nil {
		return errors.Trace(err)
	}
	b.labelKey2ValueList[key] = labels
	return nil
}

// CheckLabels is used to check whether the target label is existing in the builder.
func (b *CheckPolicyLabelsResultBuilder) CheckLabels(targetsMap map[string]struct{}) error {
	for target := range targetsMap {
		var founded bool
		for label := range b.labelKey2ValueList {
			if label == target {
				founded = true
				break
			}
		}
		if founded {
			continue
		}
		return errors.Errorf("Placement label %s doesn't exist in the storage", target)
	}
	return nil
}

// ShowPlacementLabelsResultBuilder is used to fetch the `tikv_store_status` result.
type ShowPlacementLabelsResultBuilder struct {
	labelKey2values map[string]interface{}
}

// AppendStoreLabels is used to fill the labelKey:labelValue json into builder.
func (b *ShowPlacementLabelsResultBuilder) AppendStoreLabels(bj json.BinaryJSON) error {
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

// BuildRows is used to retrieve the rows.
func (b *ShowPlacementLabelsResultBuilder) BuildRows() ([][]interface{}, error) {
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

func (b *ShowPlacementLabelsResultBuilder) sortMapKeys(m map[string]interface{}) []string {
	sorted := make([]string, 0, len(m))
	for key := range m {
		sorted = append(sorted, key)
	}

	sort.Strings(sorted)
	return sorted
}
