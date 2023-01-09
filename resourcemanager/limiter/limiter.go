// Copyright 2022 PingCAP, Inc.
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

package limiter

import (
	"github.com/pingcap/tidb/resourcemanager/util"
)

// Limiter is a limiter interface
type Limiter interface {
	Limit(component util.Component, p util.GorotinuePool) bool
}

// NewLimiter is to create a default collection of limiter.
func NewLimiter() []Limiter {
	return []Limiter{
		NewBBRLimiter(80),
	}
}
