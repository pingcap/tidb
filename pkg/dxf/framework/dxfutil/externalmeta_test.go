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

package dxfutil

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
)

func TestMarshalFields(t *testing.T) {
	type Example struct {
		X string
		Y int `json:"y"`
	}

	testCases := []struct {
		name            string
		instInternal    any
		instExternal    any
		expectedMarshal string
		expectedOmit    string
	}{
		{
			name: "non-public",
			instInternal: struct {
				a int
			}{a: 42},
			instExternal: struct {
				a int `external:"true"`
			}{a: 42},
			expectedMarshal: `{}`,
			expectedOmit:    `{}`,
		},
		{
			name: "-",
			instInternal: struct {
				A string `json:"-"`
			}{A: "42"},
			instExternal: struct {
				A string `json:"-" external:"true"`
			}{A: "42"},
			expectedMarshal: `{}`,
			expectedOmit:    `{}`,
		},
		{
			name: "omitempty",
			instInternal: struct {
				A string `json:"a,omitempty"`
			}{A: ""},
			instExternal: struct {
				A string `json:"a,omitempty" external:"true"`
			}{A: ""},
			expectedMarshal: `{}`,
			expectedOmit:    `{}`,
		},
		{
			name: "int",
			instInternal: struct {
				A int
			}{A: 42},
			instExternal: struct {
				A int `external:"true"`
			}{A: 42},
			expectedMarshal: `{"A":42}`,
			expectedOmit:    `{}`,
		},
		{
			name: "rename",
			instInternal: struct {
				A string `json:"a"`
			}{A: "42"},
			instExternal: struct {
				A string `json:"a" external:"true"`
			}{A: "42"},
			expectedMarshal: `{"a":"42"}`,
			expectedOmit:    `{}`,
		},
		{
			name: "embed",
			instInternal: struct {
				Example
			}{Example: Example{X: "42", Y: 42}},
			instExternal: struct {
				Example `external:"true"`
			}{Example: Example{X: "42", Y: 42}},
			expectedMarshal: `{"X":"42","y":42}`,
			expectedOmit:    `{}`,
		},
		{
			name: "nested",
			instInternal: struct {
				Example Example
			}{Example: Example{X: "42", Y: 42}},
			instExternal: struct {
				Example Example `external:"true"`
			}{Example: Example{X: "42", Y: 42}},
			expectedMarshal: `{"Example":{"X":"42","y":42}}`,
			expectedOmit:    `{}`,
		},
		{
			name: "inline",
			instInternal: struct {
				Example `json:",inline"`
			}{Example: Example{X: "42", Y: 42}},
			instExternal: struct {
				Example `json:",inline" external:"true"`
			}{Example: Example{X: "42", Y: 42}},
			expectedMarshal: `{"X":"42","y":42}`,
			expectedOmit:    `{}`,
		},
		{
			name: "slice",
			instInternal: struct {
				A []string
			}{A: []string{"42"}},
			instExternal: struct {
				A []string `external:"true"`
			}{A: []string{"42"}},
			expectedMarshal: `{"A":["42"]}`,
			expectedOmit:    `{}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := marshalInternalFields(tc.instInternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMarshal, string(data))

			data, err = marshalExternalFields(tc.instInternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedOmit, string(data))

			data, err = marshalInternalFields(tc.instExternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedOmit, string(data))

			data, err = marshalExternalFields(tc.instExternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMarshal, string(data))
		})
	}
}

func TestReadWriteJSON(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()

	type testStruct struct {
		BaseExternalMeta
		X int
		Y string `external:"true"`
	}
	ts := &testStruct{
		X: 42,
		Y: "test",
	}

	data, err := ts.BaseExternalMeta.Marshal(ts)
	require.NoError(t, err)
	js, err := json.Marshal(ts)
	require.NoError(t, err)
	require.Equal(t, data, js)

	ts.BaseExternalMeta.ExternalPath = "/test"
	err = ts.WriteJSONToExternalStorage(ctx, store, ts)
	require.NoError(t, err)
	data, err = ts.BaseExternalMeta.Marshal(ts)
	require.NoError(t, err)

	var ts1 testStruct
	err = ts1.ReadJSONFromExternalStorage(ctx, store, &ts1)
	require.NoError(t, err)
	require.NotEqual(t, ts, ts1)

	var ts2 testStruct
	err = json.Unmarshal(data, &ts2)
	require.NoError(t, err)
	require.NotEqual(t, ts, ts2)

	err = ts2.ReadJSONFromExternalStorage(ctx, store, &ts2)
	require.NoError(t, err)
	require.Equal(t, *ts, ts2)
}

func TestExternalMetaPath(t *testing.T) {
	require.Equal(t, "1/plan/merge-sort/1/meta.json", PlanMetaPath(1, "merge-sort", 1))
	require.Equal(t, "2/plan/ingest/3/meta.json", PlanMetaPath(2, "ingest", 3))
	require.Equal(t, "1/plan/prepared/meta.json", PreparedMetaPath(1))
	require.Equal(t, "2/plan/prepared/meta.json", PreparedMetaPath(2))

	require.Equal(t, "1/1/meta.json", SubtaskMetaPath(1, 1))
	require.Equal(t, "2/3/meta.json", SubtaskMetaPath(2, 3))
}
