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
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/dbterror"
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

// GetResourceGroupLabelByKey determines the label of the resource group based on the content of the key.
func GetResourceGroupLabelByKey(key []byte) tipb.ResourceGroupTagLabel {
	if len(key) == 0 {
		return tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown
	}
	_, _, isRow, err := decodeKeyHead(key)
	if err != nil {
		return tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown
	}
	if isRow {
		return tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow
	}
	return tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex
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
	}
	return
}

// variables needed by decodeKeyHead.
//
// **These variables are copied from tablecodec package to avoid circular dependency.**
var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
	errInvalidKey   = dbterror.ClassXEval.NewStd(errno.ErrInvalidKey)
)

// decodeKeyHead decodes the key's head and gets the tableID, indexID. isRecordKey is true when is a record key.
//
// **This function is copied from tablecodec.DecodeKeyHead to avoid circular dependency.**
func decodeKeyHead(key []byte) (tableID int64, indexID int64, isRecordKey bool, err error) {
	isRecordKey = false
	k := key
	if !bytes.HasPrefix(key, tablePrefix) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(tablePrefix):]
	key, tableID, err = decodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if bytes.HasPrefix(key, recordPrefixSep) {
		isRecordKey = true
		return
	}
	if !bytes.HasPrefix(key, indexPrefixSep) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(indexPrefixSep):]

	key, indexID, err = decodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

// decodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
//
// **This function is copied from codec.DecodeInt to avoid circular dependency.**
func decodeInt(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := decodeCmpUintToInt(u)
	b = b[8:]
	return b, v, nil
}

const signMask uint64 = 0x8000000000000000

// decodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
//
// **This function is copied from codec.DecodeCmpUintToInt to avoid circular dependency.**
func decodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}
