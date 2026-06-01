// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metaservice

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// GlobalGroupID is global meta service group id,
	// it stores some global rules and label, region and other information.
	GlobalGroupID = "0"
	// KeyspaceMetaGroupIDKey is a keyspace meta config key name,
	// the value of this key is meta service group id for this keyspace.
	KeyspaceMetaGroupIDKey = "meta_service_group_id"
	// KeyspaceMetaGroupAddrsKey is a keyspace meta config key name,
	// the value of this key is meta service group addrs for this key.
	KeyspaceMetaGroupAddrsKey = "meta_service_group_addrs"
)

// ErrGroupNotMatch exported for test.
var ErrGroupNotMatch = errors.New("it is unexpected for the keyspace to have a group ID but no group addresses")

// ErrNilKeyspaceMeta indicates the caller passed a nil keyspace meta to GetKeyspaceMetaServiceGroup.
var ErrNilKeyspaceMeta = errors.New("GetKeyspaceMetaServiceGroup: keyspace meta is nil")

// Info includes the global meta service address and the TiDB meta service group info.
type Info struct {
	PDAddrs                []string
	GlobalMetaServiceAddrs []string
	KeyspaceMetaGroup      *KeyspaceMetaServiceGroup
}

// KeyspaceMetaServiceGroup includes keyspace meta service group info.
type KeyspaceMetaServiceGroup struct {
	GroupID                  string
	KeyspaceMetaServiceAddrs []string
}

// GetKeyspaceMetaServiceGroup return keyspace meta service group.
func GetKeyspaceMetaServiceGroup(keyspaceMeta *keyspacepb.KeyspaceMeta, globalMetaAddrs []string) (*KeyspaceMetaServiceGroup, error) {
	if keyspaceMeta == nil {
		return nil, ErrNilKeyspaceMeta
	}
	var keyspaceMetaServiceGroup *KeyspaceMetaServiceGroup
	if val, ok := keyspaceMeta.Config[KeyspaceMetaGroupIDKey]; ok {
		groupID := val
		if addrsStr, addrsOk := keyspaceMeta.Config[KeyspaceMetaGroupAddrsKey]; addrsOk {
			rawAddrs := strings.Split(strings.TrimSpace(addrsStr), ",")
			addrs := make([]string, 0, len(rawAddrs))
			for _, addr := range rawAddrs {
				addr = strings.TrimSpace(addr)
				if addr == "" {
					continue
				}
				addrs = append(addrs, addr)
			}
			if len(addrs) == 0 {
				return nil, ErrGroupNotMatch
			}
			keyspaceMetaServiceGroup = &KeyspaceMetaServiceGroup{
				GroupID:                  groupID,
				KeyspaceMetaServiceAddrs: addrs,
			}
			log.Info("get keyspace meta service group info", zap.Any("group-info", keyspaceMetaServiceGroup))
			return keyspaceMetaServiceGroup, nil
		}
		return nil, ErrGroupNotMatch
	}

	// If keyspace don't have KeyspaceMetaGroupIDKey, then set keyspace meta service as global meta service.
	keyspaceMetaServiceGroup = &KeyspaceMetaServiceGroup{
		GroupID:                  GlobalGroupID,
		KeyspaceMetaServiceAddrs: globalMetaAddrs,
	}
	log.Info("get default keyspace meta service group info ", zap.Any("group-info", keyspaceMetaServiceGroup))
	return keyspaceMetaServiceGroup, nil
}

// GetMetaServiceInfo return meta service info.
func GetMetaServiceInfo(keyspaceMeta *keyspacepb.KeyspaceMeta, globalMetaAddrs []string, pdAddrs []string) (*Info, error) {
	// If non-keyspace then return global meta service or not enable meta service group.
	if keyspaceMeta == nil {
		keyspaceMetaServiceGroup := &KeyspaceMetaServiceGroup{
			GroupID:                  GlobalGroupID,
			KeyspaceMetaServiceAddrs: globalMetaAddrs,
		}
		metaInfo := &Info{
			PDAddrs:                pdAddrs,
			GlobalMetaServiceAddrs: globalMetaAddrs,
			KeyspaceMetaGroup:      keyspaceMetaServiceGroup,
		}
		log.Info("return meta service group info", zap.Any("meta-service-info", metaInfo))
		return metaInfo, nil
	}

	keyspaceServiceGroup, err := GetKeyspaceMetaServiceGroup(keyspaceMeta, globalMetaAddrs)
	if err != nil {
		return nil, err
	}
	metaInfo := &Info{
		PDAddrs:                pdAddrs,
		GlobalMetaServiceAddrs: globalMetaAddrs,
		KeyspaceMetaGroup:      keyspaceServiceGroup,
	}
	log.Info("return keyspace meta service group info", zap.Any("meta-service-info", metaInfo))
	return metaInfo, nil
}

// ServiceClient is used to request meta service.
type ServiceClient interface {
	// GetPDAddrs is used to get pd addrs(host:port).
	GetPDAddrs() ([]string, error)
	// GetPDLeaderAddrs is used to get meta service leader addrs.
	GetPDLeaderAddrs(ctx context.Context) (string, error)
	// GetPDHttpAddrs is used to get PD http addrs.
	GetPDHttpAddrs() ([]string, error)
}
