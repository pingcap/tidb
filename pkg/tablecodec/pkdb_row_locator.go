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

package tablecodec

import (
	"bytes"
	"encoding/hex"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// ErrInvalidTiDBRowID is returned when a TIDB_ROW_ID token is malformed or
// cannot be used as a locator boundary for its target table.
var ErrInvalidTiDBRowID = dbterror.ClassExpression.NewStd(errno.ErrInvalidTiDBRowID)

// EncodeTiDBRowIDKey encodes a physical table ID and an inner row handle as a
// complete record key. A partition handle is accepted only when its embedded
// partition ID agrees with physicalID; the inner handle is always encoded.
func EncodeTiDBRowIDKey(physicalID int64, handle kv.Handle) (kv.Key, error) {
	if physicalID <= 0 || handle == nil {
		return nil, ErrInvalidTiDBRowID
	}
	if partitionHandle, ok := handle.(kv.PartitionHandle); ok {
		if partitionHandle.PartitionID != physicalID || partitionHandle.Handle == nil {
			return nil, ErrInvalidTiDBRowID
		}
		handle = partitionHandle.Handle
	}
	return EncodeRowKeyWithHandle(physicalID, handle), nil
}

// EncodeTiDBRowID returns the canonical lowercase hexadecimal TIDB_ROW_ID
// token for a physical table ID and an inner row handle.
func EncodeTiDBRowID(physicalID int64, handle kv.Handle) (string, error) {
	key, err := EncodeTiDBRowIDKey(physicalID, handle)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(key), nil
}

// DecodeTiDBRowID parses and canonically validates a TIDB_ROW_ID token. It
// accepts upper- or lowercase hexadecimal input and returns lowercase output.
func DecodeTiDBRowID(token string) (
	physicalID int64,
	handle kv.Handle,
	rawRecordKey kv.Key,
	canonicalToken string,
	err error,
) {
	rawRecordKey, err = hex.DecodeString(token)
	if err != nil || len(rawRecordKey) == 0 {
		return 0, nil, nil, "", ErrInvalidTiDBRowID
	}

	physicalID, handle, err = DecodeRecordKey(rawRecordKey)
	if err != nil || physicalID <= 0 || handle == nil {
		return 0, nil, nil, "", ErrInvalidTiDBRowID
	}
	if !handle.IsInt() && !isCanonicalCommonHandle(CutRowKeyPrefix(rawRecordKey)) {
		return 0, nil, nil, "", ErrInvalidTiDBRowID
	}

	reencoded, err := EncodeTiDBRowIDKey(physicalID, handle)
	if err != nil || !bytes.Equal(rawRecordKey, reencoded) {
		return 0, nil, nil, "", ErrInvalidTiDBRowID
	}
	return physicalID, handle, rawRecordKey, hex.EncodeToString(rawRecordKey), nil
}

func isCanonicalCommonHandle(encoded []byte) bool {
	if len(encoded) == 0 {
		return false
	}

	columnCount := 0
	for len(encoded) > 0 {
		// Common-handle primary-key columns cannot be NULL. Rejecting the nil
		// flag also closes the zero-padding accepted by kv.NewCommonHandle.
		if encoded[0] == 0 {
			return false
		}
		_, remain, err := codec.CutOne(encoded)
		if err != nil || len(remain) >= len(encoded) {
			return false
		}
		encoded = remain
		columnCount++
	}
	return columnCount > 0
}
