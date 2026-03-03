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
	"github.com/pingcap/kvproto/pkg/metapb"
	pdhttp "github.com/tikv/pd/client/http"
)

// Under Classic kernel, TiFlash nodes have the label {"engine":"tiflash"}.
// Under NextGen kernel,
// - TiFlash write nodes have the label {"engine":"tiflash", "engine_role":"write"},
// - TiFlash compute nodes have the label {"engine":"tiflash_compute", "exclusive":"no-data"}, and no Region is stored on them.

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

// IsTiFlashWriteHTTPResp tests whether the store is either
// - a TiFlash node under Classic kernel
// - a TiFlash write node under NextGen kernel
// from a PD HTTP response.
// This function will return false for TiFlash compute node under NextGen kernel.
func IsTiFlashWriteHTTPResp(store *pdhttp.MetaStore) bool {
	for _, label := range store.Labels {
		if label.Key == "engine" && label.Value == "tiflash" {
			return true
		}
	}
	return false
}
