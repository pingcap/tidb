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

package resourcegrouptag

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tipb/go-tipb"
)

// EncodeResourceGroupTag encodes sql digest and plan digest into resource group tag.
func EncodeResourceGroupTag(sqlDigest, planDigest *parser.Digest) []byte {
	if sqlDigest == nil && planDigest == nil {
		return nil
	}

	tag := &tipb.ResourceGroupTag{}
	if sqlDigest != nil {
		tag.SqlDigest = sqlDigest.Bytes()
	}
	if planDigest != nil {
		tag.PlanDigest = planDigest.Bytes()
	}
	b, err := tag.Marshal()
	if err != nil {
		return nil
	}
	return b
}

// DecodeResourceGroupTag decodes a resource group tag and return the sql digest.
func DecodeResourceGroupTag(data []byte) (sqlDigest []byte, err error) {
	if len(data) == 0 {
		return nil, nil
	}
	tag := &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(data)
	if err != nil {
		return nil, errors.Errorf("invalid resource group tag data %x", data)
	}
	return tag.SqlDigest, nil
}
