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
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/tablecodec/rowindexcodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// EncodeResourceGroupTag encodes sql digest and plan digest into resource group tag.
func EncodeResourceGroupTag(sqlDigest, planDigest *parser.Digest, label tipb.ResourceGroupTagLabel) []byte {
	if sqlDigest == nil && planDigest == nil {
		return nil
	}

	tag := &tipb.ResourceGroupTag{Label: &label}
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

// GetResourceGroupLabelByKey determines the tipb.ResourceGroupTagLabel of key.
func GetResourceGroupLabelByKey(key []byte) tipb.ResourceGroupTagLabel {
	switch rowindexcodec.GetKeyKind(key) {
	case rowindexcodec.KeyKindRow:
		return tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow
	case rowindexcodec.KeyKindIndex:
		return tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex
	default:
		return tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown
	}
}

// GetFirstKeyFromRequest gets the first Key of the request from tikvrpc.Request.
func GetFirstKeyFromRequest(req *tikvrpc.Request) (firstKey []byte) {
	if req == nil {
		return
	}
	switch req.Req.(type) {
	case *kvrpcpb.GetRequest:
		r := req.Req.(*kvrpcpb.GetRequest)
		if r != nil {
			firstKey = r.Key
		}
	case *kvrpcpb.BatchGetRequest:
		r := req.Req.(*kvrpcpb.BatchGetRequest)
		if r != nil && len(r.Keys) > 0 {
			firstKey = r.Keys[0]
		}
	case *kvrpcpb.ScanRequest:
		r := req.Req.(*kvrpcpb.ScanRequest)
		if r != nil {
			firstKey = r.StartKey
		}
	case *kvrpcpb.PrewriteRequest:
		r := req.Req.(*kvrpcpb.PrewriteRequest)
		if r != nil && len(r.Mutations) > 0 {
			if mutation := r.Mutations[0]; mutation != nil {
				firstKey = mutation.Key
			}
		}
	case *kvrpcpb.CommitRequest:
		r := req.Req.(*kvrpcpb.CommitRequest)
		if r != nil && len(r.Keys) > 0 {
			firstKey = r.Keys[0]
		}
	case *kvrpcpb.BatchRollbackRequest:
		r := req.Req.(*kvrpcpb.BatchRollbackRequest)
		if r != nil && len(r.Keys) > 0 {
			firstKey = r.Keys[0]
		}
	case *coprocessor.Request:
		r := req.Req.(*coprocessor.Request)
		if r != nil && len(r.Ranges) > 0 {
			if keyRange := r.Ranges[0]; keyRange != nil {
				firstKey = keyRange.Start
			}
		}
	case *coprocessor.BatchRequest:
		r := req.Req.(*coprocessor.BatchRequest)
		if r != nil && len(r.Regions) > 0 {
			if region := r.Regions[0]; region != nil {
				if len(region.Ranges) > 0 {
					if keyRange := region.Ranges[0]; keyRange != nil {
						firstKey = keyRange.Start
					}
				}
			}
		}
	}
	return
}
