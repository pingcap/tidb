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

//go:build !codes

package testkit

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/domain"
	statisticsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/metricsutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// RegionProperityClient is to get region properties.
type RegionProperityClient struct {
	tikv.Client
	mu struct {
		sync.Mutex
		failedOnce bool
		count      int64
	}
}

// SendRequest is to mock send request.
func (c *RegionProperityClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdDebugGetRegionProperties {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.count++
		// Mock failure once.
		if !c.mu.failedOnce {
			c.mu.failedOnce = true
			return &tikvrpc.Response{}, nil
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

var _ sqlexec.RecordSet = &rowsRecordSet{}

type rowsRecordSet struct {
	fields []*resolve.ResultField
	rows   []chunk.Row

	idx int

	// this error is stored here to return in the future
	err error
}

func (r *rowsRecordSet) Fields() []*resolve.ResultField {
	return r.fields
}

func (r *rowsRecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	if r.err != nil {
		return r.err
	}

	req.Reset()
	for r.idx < len(r.rows) {
		if req.IsFull() {
			return nil
		}
		req.AppendRow(r.rows[r.idx])
		r.idx++
	}
	return nil
}

func (r *rowsRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(r.fields))
	for _, field := range r.fields {
		fields = append(fields, &field.Column.FieldType)
	}
	if alloc != nil {
		return alloc.Alloc(fields, 0, 1024)
	}
	return chunk.New(fields, 1024, 1024)
}

func (r *rowsRecordSet) Close() error {
	// do nothing
	return nil
}

// buildRowsRecordSet builds a `rowsRecordSet` from any `RecordSet` by draining all rows from it.
// It's used to store the result temporarily. After building a new RecordSet, the original rs (in the argument) can be
// closed safely.
func buildRowsRecordSet(ctx context.Context, rs sqlexec.RecordSet) sqlexec.RecordSet {
	rows, err := session.GetRows4Test(ctx, nil, rs)
	if err != nil {
		return &rowsRecordSet{
			fields: rs.Fields(),
			err:    err,
		}
	}

	return &rowsRecordSet{
		fields: rs.Fields(),
		rows:   rows,
		idx:    0,
	}
}

// MockTiDBStatusPort mock the TiDB server status port to have metrics.
func MockTiDBStatusPort(ctx context.Context, b *testing.B, port string) *util.WaitGroupWrapper {
	var wg util.WaitGroupWrapper
	err := metricsutil.RegisterMetrics()
	terror.MustNil(err)
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	statusListener, err := net.Listen("tcp", "0.0.0.0:"+port)
	require.NoError(b, err)
	statusServer := &http.Server{Handler: serverMux}
	wg.RunWithLog(func() {
		if err := statusServer.Serve(statusListener); err != nil {
			b.Logf("status server serve failed: %v", err)
		}
	})
	wg.RunWithLog(func() {
		<-ctx.Done()
		_ = statusServer.Close()
	})

	return &wg
}

// LoadTableStats loads table stats from json file.
func LoadTableStats(fileName string, dom *domain.Domain) error {
	statsPath := filepath.Join("testdata", fileName)
	bytes, err := os.ReadFile(statsPath)
	if err != nil {
		return err
	}
	statsTbl := &statisticsutil.JSONTable{}
	err = json.Unmarshal(bytes, statsTbl)
	if err != nil {
		return err
	}
	statsHandle := dom.StatsHandle()
	err = statsHandle.LoadStatsFromJSON(context.Background(), dom.InfoSchema(), statsTbl, 0)
	if err != nil {
		return err
	}
	return nil
}

// MockGCSavePoint mocks a GC save point. It's used in tests that need to set TiDB snapshot.
func (tk *TestKit) MockGCSavePoint() {
	safePoint := "20160102-15:04:05 -0700"
	tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%s', '') ON DUPLICATE KEY UPDATE variable_value = '%s', comment=''`, safePoint, safePoint))
}
