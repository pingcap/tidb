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
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"
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
