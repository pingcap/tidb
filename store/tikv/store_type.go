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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"fmt"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// AccessMode uses to index stores for different region cache access requirements.
type AccessMode int

const (
	// TiKVOnly indicates stores list that use for TiKv access(include both leader request and follower read).
	TiKVOnly AccessMode = iota
	// TiFlashOnly indicates stores list that use for TiFlash request.
	TiFlashOnly
	// NumAccessMode reserved to keep max access mode value.
	NumAccessMode
)

func (a AccessMode) String() string {
	switch a {
	case TiKVOnly:
		return "TiKvOnly"
	case TiFlashOnly:
		return "TiFlashOnly"
	default:
		return fmt.Sprintf("%d", a)
	}
}

// Constants to determine engine type.
// They should be synced with PD.
const (
	engineLabelKey     = "engine"
	engineLabelTiFlash = "tiflash"
)

// GetStoreTypeByMeta gets store type by store meta pb.
func GetStoreTypeByMeta(store *metapb.Store) tikvrpc.EndpointType {
	for _, label := range store.Labels {
		if label.Key == engineLabelKey && label.Value == engineLabelTiFlash {
			return tikvrpc.TiFlash
		}
	}
	return tikvrpc.TiKV
}
