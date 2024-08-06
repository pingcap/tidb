// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestParseVectorIndexDefFromComment(t *testing.T) {
	tests := []struct {
		comment     string
		expected    *VectorIndexInfo
		expectedErr error
	}{
		{
			comment: "hnsw(distance=l2)",
			expected: &VectorIndexInfo{
				Kind:           VectorIndexKindHNSW,
				DistanceMetric: DistanceMetricL2,
			},
			expectedErr: nil,
		},
		{
			comment: "HNSW(distance=Cosine)",
			expected: &VectorIndexInfo{
				Kind:           VectorIndexKindHNSW,
				DistanceMetric: DistanceMetricCosine,
			},
			expectedErr: nil,
		},
		{
			comment: "HNSW( distance = Cosine )",
			expected: &VectorIndexInfo{
				Kind:           VectorIndexKindHNSW,
				DistanceMetric: DistanceMetricCosine,
			},
			expectedErr: nil,
		},
		{
			comment: "HNSW(distance=Cosine, )",
			expected: &VectorIndexInfo{
				Kind:           VectorIndexKindHNSW,
				DistanceMetric: DistanceMetricCosine,
			},
			expectedErr: nil,
		},
		{
			comment:     "hnsw(distance=invalid)",
			expected:    nil,
			expectedErr: errors.New("unsupported HNSW distance metric 'invalid', available values: l2, cosine"),
		},
		{
			comment:     "hnsw(distance=l2,Distance=cosine)",
			expected:    nil,
			expectedErr: errors.New("duplicate HNSW index config 'distance'"),
		},
		{
			comment:     "hnsw(distance=l2, Distance=COSINE)",
			expected:    nil,
			expectedErr: errors.New("duplicate HNSW index config 'distance'"),
		},
		{
			comment:     "hnsw(a)",
			expected:    nil,
			expectedErr: errors.New("invalid HNSW index config 'a', expect 'key=value' format"),
		},
		{
			comment:     "hnsw(a=b)",
			expected:    nil,
			expectedErr: errors.New("unsupported HNSW index config 'a'"),
		},
		{
			comment: "hnsw()",
			expected: &VectorIndexInfo{
				Kind:           VectorIndexKindHNSW,
				DistanceMetric: DistanceMetricL2,
			},
			expectedErr: nil,
		},
	}

	for _, test := range tests {
		indexInfo, err := ParseVectorIndexDefFromComment(test.comment)
		require.Equal(t, test.expected, indexInfo)
		if test.expectedErr != nil {
			require.EqualError(t, err, test.expectedErr.Error())
		} else {
			require.NoError(t, err)
		}
	}
}
