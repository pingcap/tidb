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

package resourcemanage

import "github.com/pingcap/tidb/resourcemanage/util"

func downclock(pool *util.PoolContainer) {
	capa := pool.Pool.Cap()
	pool.Pool.Tune(capa + 1)
}

func overclock(pool *util.PoolContainer) {
	capa := pool.Pool.Cap()
	pool.Pool.Tune(capa - 1)
}
