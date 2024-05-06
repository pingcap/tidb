// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keyspace

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"

	// KeyspaceMetaConfigGCManagementType is the name of the key for GC management type in keyspace meta config.
	KeyspaceMetaConfigGCManagementType = "gc_management_type"
	// KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC is a type of GC management in keyspace meta config,
	// it means this keyspace will calculate GC safe point by its own.
	KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC = "keyspace_level_gc"
	// KeyspaceMetaConfigGCManagementTypeGlobalGC is a type of GC management in keyspace meta config, it means this keyspace will use GC safe point by global GC(default).
	KeyspaceMetaConfigGCManagementTypeGlobalGC = "global_gc"

	// maxKeyspaceID is the maximum keyspace id that can be created, no keyspace can be created greater than this value.
	maxKeyspaceID = 0xffffff

	// API V2 key bounds:
	// See https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md for more keyspace and API V2 details.

	// KeyspaceTxnModePrefix is txn data prefix of keyspace.
	KeyspaceTxnModePrefix byte = 'x'
	// MaxKeyspaceRightBoundaryPrefix is the max keyspace txn right boundary.
	// Because the keyspace ranges are all under the 'x' prefix, so MaxKeyspaceRightBoundaryPrefix = 'y'
	MaxKeyspaceRightBoundaryPrefix byte = 'y'
	// RawKVLeftBoundaryPrefix is RawKV left boundary.
	RawKVLeftBoundaryPrefix byte = 'r'
	// RawKVRightBoundaryPrefix is RawKV right boundary.
	RawKVRightBoundaryPrefix byte = 's'
)

// CodecV1 represents api v1 codec.
var CodecV1 = tikv.NewCodecV1(tikv.ModeTxn)

// MakeKeyspaceEtcdNamespace return the keyspace prefix path for etcd namespace
func MakeKeyspaceEtcdNamespace(c tikv.Codec) string {
	if c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d", c.GetKeyspaceID())
}

// MakeKeyspaceEtcdNamespaceSlash return the keyspace prefix path for etcd namespace, and end with a slash.
func MakeKeyspaceEtcdNamespaceSlash(c tikv.Codec) string {
	if c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d/", c.GetKeyspaceID())
}

// GetKeyspaceNameBySettings is used to get Keyspace name setting.
func GetKeyspaceNameBySettings() (keyspaceName string) {
	keyspaceName = config.GetGlobalKeyspaceName()
	return keyspaceName
}

// IsKeyspaceNameEmpty is used to determine whether keyspaceName is set.
func IsKeyspaceNameEmpty(keyspaceName string) bool {
	return keyspaceName == ""
}

// WrapZapcoreWithKeyspace is used to wrap zapcore.Core.
func WrapZapcoreWithKeyspace() zap.Option {
	return zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		keyspaceName := GetKeyspaceNameBySettings()
		if !IsKeyspaceNameEmpty(keyspaceName) {
			core = core.With([]zap.Field{zap.String("keyspaceName", keyspaceName)})
		}
		return core
	})
}

// IsKeyspaceUseKeyspaceLevelGC return true if keyspace meta config set "gc_management_type" = "keyspace_level_gc" explicitly.
func IsKeyspaceUseKeyspaceLevelGC(keyspaceMeta *keyspacepb.KeyspaceMeta) bool {
	if keyspaceMeta == nil {
		return false
	}
	if val, ok := keyspaceMeta.Config[KeyspaceMetaConfigGCManagementType]; ok {
		return val == KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC
	}
	return false
}

// IsKeyspaceMetaNotNilAndUseGlobalGC return whether the specified keyspace meta use global GC.
func IsKeyspaceMetaNotNilAndUseGlobalGC(keyspaceMeta *keyspacepb.KeyspaceMeta) bool {
	if keyspaceMeta == nil {
		return false
	}
	if val, ok := keyspaceMeta.Config[KeyspaceMetaConfigGCManagementType]; ok {
		return val == KeyspaceMetaConfigGCManagementTypeGlobalGC
	}
	return true
}

// GetKeyspaceTxnLeftBound return the specified keyspace txn left boundary.
func GetKeyspaceTxnLeftBound(keyspaceID uint32) []byte {
	keyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, keyspaceID)

	// The first byte is keyspaceTxnModePrefix, and the next three bytes are converted from uint32
	txnLeftBound := codec.EncodeBytes(nil, append([]byte{KeyspaceTxnModePrefix}, keyspaceIDBytes[1:]...))
	return txnLeftBound
}

// GetKeyspaceTxnRange return the specified keyspace txn left boundary and txn right boundary.
func GetKeyspaceTxnRange(keyspaceID uint32) ([]byte, []byte) {
	// Get keyspace txn left boundary
	txnLeftBound := GetKeyspaceTxnLeftBound(keyspaceID)

	var txnRightBound []byte
	if keyspaceID == maxKeyspaceID {
		// Directly set the right boundary of maxKeyspaceID to be {MaxKeyspaceRightBoundaryPrefix}
		maxKeyspaceIDTxnRightBound := []byte{MaxKeyspaceRightBoundaryPrefix}
		txnRightBound = codec.EncodeBytes(nil, maxKeyspaceIDTxnRightBound[:])
	} else {
		// The right boundary of the specified keyspace is the left boundary of keyspaceID + 1.
		txnRightBound = GetKeyspaceTxnLeftBound(keyspaceID + 1)
	}

	return txnLeftBound, txnRightBound
}
