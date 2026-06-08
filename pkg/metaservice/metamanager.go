// Copyright 2026 PingCAP, Inc.
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
	"regexp"
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
	// GroupIDKey is a keyspace meta config key name,
	// the value of this key is meta service group id for this keyspace.
	GroupIDKey = "meta_service_group_id"
	// GroupAddrsKey is a keyspace meta config key name,
	// the value of this key is meta service group addrs for this key.
	GroupAddrsKey = "meta_service_group_addrs"
)

// ErrGroupNotMatch exported for test.
var ErrGroupNotMatch = errors.New("it is unexpected for the keyspace to have a group ID but no group addresses")

// ErrNilKeyspaceMeta indicates the caller passed a nil keyspace meta to GetGroup.
var ErrNilKeyspaceMeta = errors.New("GetGroup: keyspace meta is nil")

// ErrInvalidGroupID indicates the keyspace meta contains an invalid meta service group ID.
var ErrInvalidGroupID = errors.New("invalid meta service group id: it must contain at least one letter and only contain letters, digits, '-' or '_'")

// groupIDPattern validates keyspace meta service group IDs configured in keyspace meta.
// Rules enforced by the pattern:
//   - only ASCII letters, digits, '-' and '_' are allowed;
//   - at least one letter is required, so pure numeric IDs (for example "123") are rejected.
//
// Note: the default global group ID "0" is not validated here; it is used when GroupIDKey is absent.
var groupIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]*[A-Za-z][A-Za-z0-9_-]*$`)

func validateGroupID(groupID string) error {
	if !groupIDPattern.MatchString(groupID) {
		return ErrInvalidGroupID
	}
	return nil
}

// Info includes the PD addresses and the TiDB meta service group info.
type Info struct {
	PDAddrs []string
	Group   *Group
}

// Group includes keyspace meta service group info.
type Group struct {
	GroupID string
	Addrs   []string
}

// GetGroup returns the keyspace meta service group.
func GetGroup(keyspaceMeta *keyspacepb.KeyspaceMeta, pdAddrs []string) (*Group, error) {
	if keyspaceMeta == nil {
		return nil, ErrNilKeyspaceMeta
	}
	var group *Group
	// TODO: Refactor meta service group storage format by moving it from config to dedicated fields in keyspace meta.
	if val, ok := keyspaceMeta.Config[GroupIDKey]; ok {
		groupID := val
		if err := validateGroupID(groupID); err != nil {
			return nil, err
		}
		addrsStr, addrsOk := keyspaceMeta.Config[GroupAddrsKey]
		if !addrsOk {
			return nil, ErrGroupNotMatch
		}
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
		group = &Group{
			GroupID: groupID,
			Addrs:   addrs,
		}
		log.Info("get keyspace meta service group info", zap.Any("group-info", group))
		return group, nil
	}

	// If keyspace doesn't have GroupIDKey, use the global meta service group.
	group = &Group{
		GroupID: GlobalGroupID,
		Addrs:   pdAddrs,
	}
	log.Info("get default keyspace meta service group info ", zap.Any("group-info", group))
	return group, nil
}

// GetInfo return meta service info.
func GetInfo(keyspaceMeta *keyspacepb.KeyspaceMeta, pdAddrs []string) (*Info, error) {
	// If non-keyspace then return global meta service or not enable meta service group.
	if keyspaceMeta == nil {
		keyspaceMetaServiceGroup := &Group{
			GroupID: GlobalGroupID,
			Addrs:   pdAddrs,
		}
		metaInfo := &Info{
			PDAddrs: pdAddrs,
			Group:   keyspaceMetaServiceGroup,
		}
		log.Info("return meta service group info", zap.Any("meta-service-info", metaInfo))
		return metaInfo, nil
	}

	keyspaceServiceGroup, err := GetGroup(keyspaceMeta, pdAddrs)
	if err != nil {
		return nil, err
	}
	metaInfo := &Info{
		PDAddrs: pdAddrs,
		Group:   keyspaceServiceGroup,
	}
	log.Info("return keyspace meta service group info", zap.Any("meta-service-info", metaInfo))
	return metaInfo, nil
}

// ServiceClient is used to request meta service.
type ServiceClient interface {
	// GetPDAddrs is used to get pd addrs(host:port).
	GetPDAddrs(ctx context.Context) ([]string, error)
	// GetPDHttpAddrs is used to get PD http addrs.
	GetPDHttpAddrs(ctx context.Context) ([]string, error)
}
