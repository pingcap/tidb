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

package ddl

import (
	"testing"

	"github.com/ngaut/pools"
)

func TestDDLWorkerPool(t *testing.T) {
	f := func() func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newWorker(nil, addIdxWorker, nil, nil, nil)
			return wk, nil
		}
	}
	pool := newDDLWorkerPool(pools.NewResourcePool(f(), 1, 2, 0), jobTypeReorg)
	pool.close()
	pool.put(nil)
}
