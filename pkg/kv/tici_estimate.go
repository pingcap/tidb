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

package kv

import (
	"context"
	"time"

	"github.com/pingcap/tipb/go-tipb"
)

// TiCIEstimateCountRequest describes a store-level TiCI estimate-count request.
type TiCIEstimateCountRequest struct {
	TableID        int64
	IndexID        int64
	StartTS        uint64
	FTSQueryInfo   *tipb.FTSQueryInfo
	KeyRanges      *KeyRanges
	TimeZoneName   string
	TimeZoneOffset int64
}

// TiCIEstimateCountProvider is implemented by stores that can estimate TiCI row counts.
type TiCIEstimateCountProvider interface {
	EstimateTiCICount(ctx context.Context, req *TiCIEstimateCountRequest, timeout time.Duration) (uint64, error)
}
