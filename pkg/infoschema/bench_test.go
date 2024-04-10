// Copyright 2024 PingCAP, Inc.
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

package infoschema_test

import (
	"context"
	"flag"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	tableCnt = flag.Int("table-cnt", 100, "table count")
	version  = flag.Int("version", 2, "infoschema version")
	port     = flag.String("port", "10080", "port of metric server")
)

// test overhead of infoschema
//
// GOOS=linux GOARCH=amd64 go test -tags intest -c -o bench.test ./pkg/infoschema
//
// bench.test -test.v -run ^$ -test.bench=BenchmarkInfoschemaOverhead --with-tikv "upstream-pd:2379?disableGC=true"
func BenchmarkInfoschemaOverhead(b *testing.B) {
	wg := testkit.MockTiDBStatusPort(context.Background(), b, *port)

	var d driver.TiKVDriver
	var err error
	store, err := d.Open("tikv://" + *testkit.WithTiKV)
	require.NoError(b, err)

	re := internal.CreateAutoIDRequirementWithStore(b, store)
	defer func() {
		err := re.Store().Close()
		require.NoError(b, err)
	}()

	if *version == 2 {
		variable.SchemaCacheSize.Store(1000000)
	}

	tc := &infoschemaTestContext{
		t:    b,
		re:   re,
		ctx:  kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL),
		data: infoschema.NewData(),
	}
	startTime := time.Now()
	for j := 0; j < *tableCnt; j++ {
		tc.runCreateTable("test" + strconv.Itoa(j))
	}
	logutil.BgLogger().Info("all table created", zap.Duration("cost time", time.Since(startTime)))
	// TODO: add more scenes.
	wg.Wait()
}
