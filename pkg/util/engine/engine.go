// Copyright 2022 PingCAP, Inc.
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

package engine

import (
	"slices"

	"github.com/pingcap/kvproto/pkg/metapb"
	pdhttp "github.com/tikv/pd/client/http"
)

// IsTiFlash check whether the store is based on tiflash engine.
func IsTiFlash(store *metapb.Store) bool {
	for _, label := range store.Labels {
		if label.Key == "engine" && (label.Value == "tiflash_compute" || label.Value == "tiflash") {
			return true
		}
	}
	return false
}

// IsTiFlashHTTPResp tests whether the store is based on tiflash engine from a PD
// HTTP response.
func IsTiFlashHTTPResp(store *pdhttp.MetaStore) bool {
	for _, label := range store.Labels {
		if label.Key == "engine" && (label.Value == "tiflash_compute" || label.Value == "tiflash") {
			return true
		}
	}
	return false
}

// IsTiFlashWriteNodeHTTPResp tests whether the store is a tiflash write node from a PD HTTP response.
func IsTiFlashWriteNodeHTTPResp(store *pdhttp.MetaStore) bool {
	for _, label := range store.Labels {
		if label.Key == "engine" && label.Value == "tiflash" {
			return true
		}
	}
	return false
}

// IsReplicator check whether the store is a replicator store.
func IsReplicator(store *metapb.Store) bool {
	return slices.ContainsFunc(store.Labels, func(label *metapb.StoreLabel) bool {
		return label.Key == "engine" && label.Value == "replicator"
	})
}

// IsReplicatorHTTPResp tests whether the store is a replicator store from a PD HTTP response.
func IsReplicatorHTTPResp(store *pdhttp.MetaStore) bool {
	return slices.ContainsFunc(store.Labels, func(label pdhttp.StoreLabel) bool {
		return label.Key == "engine" && label.Value == "replicator"
	})
}
