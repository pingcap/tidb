// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

var (
	errInvalidKey       = dbterror.ClassXEval.NewStd(errno.ErrInvalidKey)
	errInvalidRecordKey = dbterror.ClassXEval.NewStd(errno.ErrInvalidRecordKey)
	errInvalidIndexKey  = dbterror.ClassXEval.NewStd(errno.ErrInvalidIndexKey)
)

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
	metaPrefix      = []byte{'m'}
)

const (
	idLen     = 8
	prefixLen = 1 + idLen /*tableID*/ + 2
	// RecordRowKeyLen is public for calculating average row size.
	RecordRowKeyLen       = prefixLen + idLen /*handle*/
	tablePrefixLength     = 1
	recordPrefixSepLength = 2
	metaPrefixLength      = 1
	// MaxOldEncodeValueLen is the maximum len of the old encoding of index value.
	MaxOldEncodeValueLen = 9

	// CommonHandleFlag is the flag used to decode the common handle in an unique index value.
	CommonHandleFlag byte = 127
	// PartitionIDFlag is the flag used to decode the partition ID.
	// Used in both global index values and global index keys (for V1+ non-unique indexes).
	// In keys: PartitionIDFlag + partition_id (8 bytes) + inner_handle_encoded (IntHandle)
	// In values: PartitionIDFlag + partition_id (8 bytes)
	PartitionIDFlag byte = 126
	// IndexVersionFlag is the flag used to decode the index's version info.
	IndexVersionFlag byte = 125
	// RestoreDataFlag is the flag that RestoreData begin with.
	// See rowcodec.Encoder.Encode and rowcodec.row.toBytes
	RestoreDataFlag byte = rowcodec.CodecVer
)

// TableSplitKeyLen is the length of key 't{table_id}' which is used for table split.
const TableSplitKeyLen = 1 + idLen

func init() {
	// help kv package to refer the tablecodec package to resolve the kv.Key functions.
	kv.DecodeTableIDFunc = func(key kv.Key) int64 {
		//preCheck, avoid the noise error log.
		if hasTablePrefix(key) && len(key) >= TableSplitKeyLen {
			return DecodeTableID(key)
		}
		return 0
	}
}
