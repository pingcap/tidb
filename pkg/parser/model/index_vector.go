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
	"regexp"
	"strings"

	"github.com/pingcap/errors"
)

// DistanceMetric is the distance metric used by the vector index.
type DistanceMetric string

const (
	// Note: tipb.VectorDistanceMetric's enum names must be aligned with these constant values.

	// DistanceMetricL1 is L1 distance.
	DistanceMetricL1 DistanceMetric = "L1"
	// DistanceMetricL2 is L2 distance.
	DistanceMetricL2 DistanceMetric = "L2"
	// DistanceMetricCosine is cosine distance.
	DistanceMetricCosine DistanceMetric = "COSINE"
	// DistanceMetricInnerProduct is inner product.
	DistanceMetricInnerProduct DistanceMetric = "INNER_PRODUCT"
)

// VectorIndexKind is the kind of vector index.
type VectorIndexKind string

const (
	// Note: tipb.VectorIndexKind's enum names must be aligned with these constant values.

	// VectorIndexKindHNSW is HNSW index.
	VectorIndexKindHNSW VectorIndexKind = "HNSW"
)

// VectorIndexInfo is the information of vector index of a column.
type VectorIndexInfo struct {
	// Kind is the kind of vector index. Currently only HNSW is supported.
	Kind VectorIndexKind `json:"kind"`

	// Dimension is the dimension of the vector.
	Dimension uint64 `json:"dimension"` // Set to 0 when initially parsed from comment. Will be assigned to flen later.

	// DistanceMetric is the distance metric used by the index.
	DistanceMetric DistanceMetric `json:"distance_metric"`
}

var regexVectorIndexDef = regexp.MustCompile(`^hnsw\(([^\)]*)\)$`)

// ParseVectorIndexDefFromComment parses a VectorIndexInfo from the column comment
func ParseVectorIndexDefFromComment(comment string) (*VectorIndexInfo, error) {
	comment = strings.ToLower(strings.TrimSpace(comment))

	// Match comments like hnsw(....)
	matches := regexVectorIndexDef.FindStringSubmatch(comment)
	if len(matches) != 2 {
		return nil, nil
	}

	parameters := strings.FieldsFunc(matches[1], func(r rune) bool {
		return r == ','
	})

	parametersMap := make(map[string]string)

	for _, parameter := range parameters {
		if strings.TrimSpace(parameter) == "" {
			continue
		}
		parts := strings.SplitN(parameter, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("invalid HNSW index config '%s', expect 'key=value' format", parameter)
		}

		key, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if _, ok := parametersMap[key]; ok {
			return nil, errors.Errorf("duplicate HNSW index config '%s'", key)
		}
		parametersMap[key] = value
	}

	indexInfo := &VectorIndexInfo{
		Kind:      VectorIndexKindHNSW,
		Dimension: 0, // Unknown
		// Use L2 distance by default
		DistanceMetric: DistanceMetricL2,
	}

	for key, value := range parametersMap {
		switch key {
		case "distance":
			switch value {
			// l1 is not supported by TiFlash yet.
			// case "l1":
			// 	indexInfo.DistanceMetric = DistanceMetricL1
			case "l2":
				indexInfo.DistanceMetric = DistanceMetricL2
			case "cosine":
				indexInfo.DistanceMetric = DistanceMetricCosine
			// inner product is not supported by TiDB optimizer yet.
			// case "inner_product":
			// 	indexInfo.DistanceMetric = DistanceMetricInnerProduct
			default:
				return nil, errors.Errorf("unsupported HNSW distance metric '%s', available values: l2, cosine", value)
			}
		default:
			return nil, errors.Errorf("unsupported HNSW index config '%s'", key)
		}
	}

	return indexInfo, nil
}
