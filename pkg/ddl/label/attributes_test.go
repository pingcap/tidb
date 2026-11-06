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

package label

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client/http"
)

func TestNewLabel(t *testing.T) {
	type TestCase struct {
		name  string
		input string
		label pd.RegionLabel
	}

	tests := []TestCase{
		{
			name:  "normal",
			input: "merge_option=allow",
			label: pd.RegionLabel{
				Key:   "merge_option",
				Value: "allow",
			},
		},
		{
			name:  "normal with space",
			input: " merge_option=allow ",
			label: pd.RegionLabel{
				Key:   "merge_option",
				Value: "allow",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			label, err := NewLabel(test.input)
			require.NoError(t, err)
			require.Equal(t, test.label, label)
		})
	}
}

func TestRestoreLabel(t *testing.T) {
	type TestCase struct {
		name   string
		input  pd.RegionLabel
		output string
	}

	input, err := NewLabel("merge_option=allow")
	require.NoError(t, err)

	input1, err := NewLabel(" merge_option=allow  ")
	require.NoError(t, err)

	tests := []TestCase{
		{
			name:   "normal",
			input:  input,
			output: "merge_option=allow",
		},
		{
			name:   "normal with spaces",
			input:  input1,
			output: "merge_option=allow",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := RestoreRegionLabel(&test.input)
			require.Equal(t, test.output, output)
		})
	}
}

func TestNewLabels(t *testing.T) {
	labels, err := NewLabels(nil)
	require.NoError(t, err)
	require.Len(t, labels, 0)

	labels, err = NewLabels([]string{})
	require.NoError(t, err)
	require.Len(t, labels, 0)

	labels, err = NewLabels([]string{"merge_option=allow"})
	require.NoError(t, err)
	require.Len(t, labels, 1)
	require.Equal(t, "merge_option", labels[0].Key)
	require.Equal(t, "allow", labels[0].Value)

	// test multiple attributes
	labels, err = NewLabels([]string{"merge_option=allow", "key=value"})
	require.NoError(t, err)
	require.Len(t, labels, 2)
	require.Equal(t, "merge_option", labels[0].Key)
	require.Equal(t, "allow", labels[0].Value)
	require.Equal(t, "key", labels[1].Key)
	require.Equal(t, "value", labels[1].Value)

	// test duplicated attributes
	labels, err = NewLabels([]string{"merge_option=allow", "merge_option=allow"})
	require.NoError(t, err)
	require.Len(t, labels, 1)
	require.Equal(t, "merge_option", labels[0].Key)
	require.Equal(t, "allow", labels[0].Value)

	_, err = NewLabels([]string{"region_policy=exclusive", "region_policy=exclusive"})
	require.ErrorContains(t, err, "duplicated attribute 'region_policy'")
}

func TestAddLabels(t *testing.T) {
	type TestCase struct {
		name   string
		labels []pd.RegionLabel
		label  pd.RegionLabel
		err    bool
	}

	labels, err := NewLabels([]string{"merge_option=allow"})
	require.NoError(t, err)
	label, err := NewLabel("somethingelse=true")
	require.NoError(t, err)
	l1, err := NewLabels([]string{"key=value"})
	require.NoError(t, err)
	l2, err := NewLabel("key=value")
	require.NoError(t, err)
	l3, err := NewLabels([]string{"key=value1"})
	require.NoError(t, err)

	tests := []TestCase{
		{
			"normal",
			labels,
			label,
			false,
		},
		{
			"duplicated attributes, skip",
			l1, l2,
			false,
		},
		{
			"duplicated attributes, skip",
			append(labels, pd.RegionLabel{
				Key:   "merge_option",
				Value: "allow",
			}),
			label,
			false,
		},
		{
			"conflict attributes",
			l3,
			l2,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err = Add(&test.labels, test.label)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.label, test.labels[len(test.labels)-1])
			}
		})
	}
}

func TestRestoreLabels(t *testing.T) {
	type TestCase struct {
		name   string
		input  []pd.RegionLabel
		output string
	}

	input1, err := NewLabel("merge_option=allow")
	require.NoError(t, err)
	input2, err := NewLabel("key=value")
	require.NoError(t, err)
	input3, err := NewLabel("db=d1")
	require.NoError(t, err)
	input4, err := NewLabel("table=t1")
	require.NoError(t, err)
	input5, err := NewLabel("partition=p1")
	require.NoError(t, err)
	input6, err := NewLabel("keyspace=42")
	require.NoError(t, err)

	tests := []TestCase{
		{
			"normal1",
			[]pd.RegionLabel{},
			"",
		},
		{
			"normal2",
			[]pd.RegionLabel{input1, input2},
			`"merge_option=allow","key=value"`,
		},
		{
			"normal3",
			[]pd.RegionLabel{input3, input4, input5},
			"",
		},
		{
			"normal4",
			[]pd.RegionLabel{input1, input2, input3},
			`"merge_option=allow","key=value"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := RestoreRegionLabels(&test.input)
			require.Equal(t, test.output, output)
		})
	}

	if kerneltype.IsNextGen() {
		output := RestoreRegionLabels(&[]pd.RegionLabel{input1, input6})
		require.Equal(t, `"merge_option=allow"`, output)
		output = RestoreRegionLabels(&[]pd.RegionLabel{input6, input1})
		require.Equal(t, `"merge_option=allow"`, output)
	} else {
		output := RestoreRegionLabels(&[]pd.RegionLabel{input1, input6})
		require.Equal(t, `"merge_option=allow","keyspace=42"`, output)
		output = RestoreRegionLabels(&[]pd.RegionLabel{input6, input1})
		require.Equal(t, `"keyspace=42","merge_option=allow"`, output)
	}
}

func TestValidateRegionPolicy(t *testing.T) {
	tests := []struct {
		name          string
		input         []string
		appendLabel   *pd.RegionLabel
		expectedLabel string
		err           string
	}{
		{
			name:          "valid exclusive",
			input:         []string{"region_policy=exclusive"},
			expectedLabel: "region_policy=exclusive",
		},
		{
			name:          "normalize to lower case",
			input:         []string{"region_policy=EXCLUSIVE"},
			expectedLabel: "region_policy=exclusive",
		},
		{
			name:        "duplicate in label slice",
			input:       []string{"region_policy=exclusive"},
			appendLabel: &pd.RegionLabel{Key: regionPolicyKey, Value: regionPolicyExclusive},
			err:         "duplicated attribute 'region_policy'",
		},
		{
			name:  "invalid shared",
			input: []string{"region_policy=shared"},
			err:   "invalid region policy attribute 'region_policy=shared'",
		},
		{
			name:  "invalid random",
			input: []string{"region_policy=random"},
			err:   "invalid region policy attribute 'region_policy=random'",
		},
		{
			name:  "unrelated attributes unchanged",
			input: []string{"merge_option=deny"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			labels, err := NewLabels(test.input)
			require.NoError(t, err)
			if test.appendLabel != nil {
				labels = append(labels, *test.appendLabel)
			}

			err = ValidateRegionPolicy(labels)
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
				return
			}

			require.NoError(t, err)
			if test.expectedLabel != "" {
				require.Equal(t, test.expectedLabel, RestoreRegionLabel(&labels[0]))
			}
		})
	}
}
