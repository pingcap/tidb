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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"testing"

	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/klauspost/compress/zstd.(*blockDec).startDecoder"),
		goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).compactionError"),
		goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).mCompaction"),
		goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).mpoolDrain"),
		goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).tCompaction"),
		goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb/util.(*BufferPool).drain"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}
