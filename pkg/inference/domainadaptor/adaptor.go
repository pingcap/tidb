// Copyright 2026 PingCAP, Inc.
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

// Package domainadaptor provides access to Domain-owned inference resources
// without importing the domain package and creating a dependency cycle.
package domainadaptor

import (
	"github.com/pingcap/tidb/pkg/inference"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type domainContext interface {
	GetDomain() any
}

type domainProxy interface {
	GetEmbedFn() *inference.EmbedFn
}

// GetEmbedFn returns the Domain-owned embedding function associated with sctx.
// Unit tests without a Domain use a test-only process-wide fallback.
func GetEmbedFn(sctx domainContext) *inference.EmbedFn {
	if sctx != nil {
		if proxy, ok := sctx.GetDomain().(domainProxy); ok {
			if embedFn := proxy.GetEmbedFn(); embedFn != nil {
				return embedFn
			}
		}
	}
	if intest.InTest {
		return inference.DefaultEmbedFn()
	}
	return nil
}
