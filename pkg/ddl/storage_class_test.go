// Copyright 2025 PingCAP, Inc.
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

package ddl_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBuildStorageClassSettingsFromJSON(t *testing.T) {
	cases := []struct {
		input    string
		okParseEngineAttribute bool
		okBuildStorageClass       bool
		expected string
	}{
		{
			input:    "",
			okParseEngineAttribute: true,
			okBuildStorageClass:       true,
			expected: `{"defs":[{"tier":"STANDARD", "names_in":null, "less_than":null, "values_in":null}]}`,
		},
		{
			input:    `{"storage_class": "IA"}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       true,
			expected: `{"defs":[{"tier":"IA", "names_in":null, "less_than":null, "values_in":null}]}`,
		},
		{
			input:    `{"storage_class": "ia"}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       true,
			expected: `{"defs":[{"tier":"IA", "names_in":null, "less_than":null, "values_in":null}]}`,
		},
		{
			input:    `{"storage_class": "STANDARD"}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       true,
			expected: `{"defs":[{"tier":"STANDARD", "names_in":null, "less_than":null, "values_in":null}]}`,
		},
		{
			input:    `{"storage_class": "standard"}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       true,
			expected: `{"defs":[{"tier":"STANDARD", "names_in":null, "less_than":null, "values_in":null}]}`,
		},
		{
			input:    `{"storage_class": {"tier": "STANDARD", "names_in": ["p0", "p1"]}}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       true,
			expected: `{"defs":[{"tier":"STANDARD", "names_in":["p0", "p1"], "less_than":null, "values_in":null}]}`,
		},
		{
			input: `{"storage_class": [
						{"tier": "STANDARD", "names_in": ["p0", "p1"]},
						{"tier": "IA", "names_in": ["p2", "p3"]}
					]}`,
			okParseEngineAttribute: true,
			okBuildStorageClass: true,
			expected: `{"defs":[
							{"tier":"STANDARD", "names_in":["p0", "p1"], "less_than":null, "values_in":null},
							{"tier":"IA", "names_in":["p2", "p3"], "less_than":null, "values_in":null}
						]}`,
		},
		{
			input:    `{"storage_class": "INVALID"}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       false,
			expected: ``,
		},
		{
			input:    `{"storage_class": "IA", "extra_field": "value"}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       true,
			expected: `{"defs":[{"tier":"IA", "names_in":null, "less_than":null, "values_in":null}]}`,
		},
		{
			// Unknown fields in the storage class definition is not allowed.
			input:    `{"storage_class": {"tier": "IA", "extra_field": "value"}}`,
			okParseEngineAttribute: true,
			okBuildStorageClass:       false,
			expected: ``,
		},
		{
			// Error if names_in is duplicated.
			input: `{"storage_class": [
						{"tier": "STANDARD", "names_in": ["p0", "p1"]},
						{"tier": "IA", "names_in": ["p0", "p3"]}
					]}`,
			okParseEngineAttribute: true,
			okBuildStorageClass: false,
			expected: ``,
		},
		{
			// Error if JSON is invalid.
			input: `IA`,
			okParseEngineAttribute: false,
			okBuildStorageClass: false,
			expected: ``,
		},
	}

	for _, cs := range cases {
		attr, err := model.ParseEngineAttributeFromString(cs.input)
		if !cs.okParseEngineAttribute {
			require.Error(t, err, "input: %s", cs.input)
			continue
		}
		require.NoError(t, err, "input: %s", cs.input)
		require.NotNil(t, attr, "input: %s", cs.input)
		settings, err := ddl.BuildStorageClassSettingsFromJSON(attr.StorageClass)
		if !cs.okBuildStorageClass {
			require.Error(t, err, "input: %s", cs.input)
			continue
		}
		require.NoError(t, err, "input: %s", cs.input)
		output, err := json.Marshal(settings)
		require.NoError(t, err, "input: %s", cs.input)
		require.JSONEq(t, cs.expected, string(output), "input: %s", cs.input)
	}
}
