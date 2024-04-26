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
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"

	// KeyspaceMetaConfigGCManagementType is gc management type in keyspace meta config.
	KeyspaceMetaConfigGCManagementType = "gc_management_type"
	// KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC is a type of GC management in keyspace meta config,
	// it means this keyspace will calculate GC safe point by its own.
	KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC = "keyspace_level_gc"
	// KeyspaceMetaConfigGCManagementTypeGlobalGC is a type of GC management in keyspace meta config, it means this keyspace will use GC safe point by global GC(default).
	KeyspaceMetaConfigGCManagementTypeGlobalGC = "global_gc"

	// maxKeyspaceID is the maximum keyspace id that can be created, no keyspace can be created greater than this value.
	maxKeyspaceID = 0xffffff

	// keyspaceTxnModePrefix is txn data prefix of keyspace.
	keyspaceTxnModePrefix byte = 'x'
)

// CodecV1 represents api v1 codec.
var CodecV1 = tikv.NewCodecV1(tikv.ModeTxn)

// globalKeyspaceMeta is the keyspace meta of the current TiDB, if TiDB without set "keyspace-name" then globalKeyspaceMeta.Load() == nil.
var globalKeyspaceMeta atomic.Pointer[keyspacepb.KeyspaceMeta]

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

// IsCurrentKeyspaceUseKeyspaceLevelGC return true if globalKeyspaceMeta not nil and globalKeyspaceMeta config has "gc_management_type" = "keyspace_level_gc".
func IsCurrentKeyspaceUseKeyspaceLevelGC() bool {
	return IsKeyspaceUseKeyspaceLevelGC(GetCurrentKeyspaceMeta())
}

// IsKeyspaceUseKeyspaceLevelGC return true if keyspace meta config has "gc_management_type" = "keyspace_level_gc".
func IsKeyspaceUseKeyspaceLevelGC(keyspaceMeta *keyspacepb.KeyspaceMeta) bool {
	if keyspaceMeta == nil {
		return false
	}
	if val, ok := keyspaceMeta.Config[KeyspaceMetaConfigGCManagementType]; ok {
		return val == KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC
	}
	return false
}

// IsCurrentKeyspaceUseGlobalGC return true if TiDB set 'keyspace-name' and use global gc.
func IsCurrentKeyspaceUseGlobalGC() bool {
	if GetCurrentKeyspaceMeta() == nil {
		return true
	}
	if val, ok := GetCurrentKeyspaceMeta().Config[KeyspaceMetaConfigGCManagementType]; ok {
		return val == KeyspaceMetaConfigGCManagementTypeGlobalGC
	}
	return true
}

// GetKeyspaceTxnLeftBound return the keyspace txn left boundary.
func GetKeyspaceTxnLeftBound(keyspaceID uint32) []byte {
	keyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, keyspaceID)

	// The first byte is keyspaceTxnModePrefix, and the next three bytes are converted from uint32
	txnLeftBound := codec.EncodeBytes(nil, append([]byte{keyspaceTxnModePrefix}, keyspaceIDBytes[1:]...))
	return txnLeftBound
}

// GetKeyspaceTxnRange return the keyspace txn left boundary and txn right boundary.
func GetKeyspaceTxnRange(keyspaceID uint32) ([]byte, []byte) {
	// Get keyspace txn left boundary
	txnLeftBound := GetKeyspaceTxnLeftBound(keyspaceID)

	var txnRightBound []byte
	if keyspaceID == maxKeyspaceID {
		// Directly set the right boundary of maxKeyspaceID to be {keyspaceTxnModePrefix + 1, 0, 0, 0}
		maxKeyspaceIDTxnRightBound := [4]byte{keyspaceTxnModePrefix + 1, 0, 0, 0}
		txnRightBound = codec.EncodeBytes(nil, maxKeyspaceIDTxnRightBound[:])
	} else {
		// The right boundary of the specified keyspace is the left boundary of keyspaceID + 1.
		txnRightBound = GetKeyspaceTxnLeftBound(keyspaceID + 1)
	}

	return txnLeftBound, txnRightBound
}

// InitGlobalKeyspaceMeta is used to get the keyspace meta from PD during TiDB startup and set global keyspace meta.
func InitGlobalKeyspaceMeta(pdClient *tikv.CodecPDClient, keyspaceName string) error {
	keyspaceMeta, err := GetKeyspaceMeta(pdClient, keyspaceName)
	setCurrentKeyspaceMeta(keyspaceMeta)
	return err
}

// GetKeyspaceMeta return keyspace meta of the given keyspace name.
func GetKeyspaceMeta(pdCli pd.Client, keyspaceName string) (*keyspacepb.KeyspaceMeta, error) {
	// Load Keyspace meta with retry.
	var keyspaceMeta *keyspacepb.KeyspaceMeta
	err := util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (bool, error) {
		var errInner error
		keyspaceMeta, errInner = pdCli.LoadKeyspace(context.TODO(), keyspaceName)
		// Retry when pd not bootstrapped or if keyspace not exists.
		if IsNotBootstrappedError(errInner) || IsKeyspaceNotExistError(errInner) {
			return true, errInner
		}
		// Do not retry when success or encountered unexpected error.
		return false, errInner
	})
	if err != nil {
		return nil, err
	}

	return keyspaceMeta, nil
}

// IsNotBootstrappedError returns true if the error is pd not bootstrapped error.
func IsNotBootstrappedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
}

// IsKeyspaceNotExistError returns true the error is caused by keyspace not exists.
func IsKeyspaceNotExistError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_ENTRY_NOT_FOUND.String())
}

// GetCurrentKeyspaceMeta return global keyspace meta if TiDB set "keyspace-name"
func GetCurrentKeyspaceMeta() *keyspacepb.KeyspaceMeta {
	v := globalKeyspaceMeta.Load()
	if v == nil {
		return nil
	}
	return v
}

func setCurrentKeyspaceMeta(ks *keyspacepb.KeyspaceMeta) {
	globalKeyspaceMeta.Store(ks)
}
