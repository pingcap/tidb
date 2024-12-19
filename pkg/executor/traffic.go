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

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// The keys for the mocked data that stored in context. They are only used for test.
type tiproxyAddrKeyType struct{}

var tiproxyAddrKey tiproxyAddrKeyType

type trafficJob struct {
	Instance  string `json:"-"` // not passed from TiProxy
	Type      string `json:"type"`
	Status    string `json:"status"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time,omitempty"`
	Progress  string `json:"progress"`
	Err       string `json:"error,omitempty"`
}

const (
	startTimeKey = "start-time"
)

// TrafficCaptureExec sends capture traffic requests to TiProxy.
type TrafficCaptureExec struct {
	exec.BaseExecutor
	Args map[string]string
}

// Next implements the Executor Next interface.
func (e *TrafficCaptureExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	e.Args[startTimeKey] = time.Now().Format(time.RFC3339)
	form := getForm(e.Args)
	_, err := request(ctx, e.BaseExecutor, strings.NewReader(form), http.MethodPost, "api/traffic/capture")
	return err
}

// TrafficReplayExec sends replay traffic requests to TiProxy.
type TrafficReplayExec struct {
	exec.BaseExecutor
	Args map[string]string
}

// Next implements the Executor Next interface.
func (e *TrafficReplayExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	e.Args[startTimeKey] = time.Now().Format(time.RFC3339)
	form := getForm(e.Args)
	_, err := request(ctx, e.BaseExecutor, strings.NewReader(form), http.MethodPost, "api/traffic/replay")
	return err
}

// TrafficCancelExec sends cancel traffic job requests to TiProxy.
type TrafficCancelExec struct {
	exec.BaseExecutor
}

// Next implements the Executor Next interface.
func (e *TrafficCancelExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	_, err := request(ctx, e.BaseExecutor, nil, http.MethodPost, "api/traffic/cancel")
	return err
}

// TrafficShowExec sends show traffic job requests to TiProxy.
type TrafficShowExec struct {
	exec.BaseExecutor
	jobs   []trafficJob
	cursor int
}

// Open implements the Executor Open interface.
func (e *TrafficShowExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	resps, err := request(ctx, e.BaseExecutor, nil, http.MethodGet, "api/traffic/show")
	if err != nil {
		return err
	}
	allJobs := make([]trafficJob, 0, len(resps))
	for addr, resp := range resps {
		var jobs []trafficJob
		if err := json.Unmarshal([]byte(resp), &jobs); err != nil {
			logutil.Logger(ctx).Error("unmarshal traffic job failed", zap.String("addr", addr), zap.String("jobs", resp), zap.Error(err))
			return err
		}
		for i := range len(jobs) {
			jobs[i].Instance = addr
		}
		allJobs = append(allJobs, jobs...)
	}
	sort.Slice(allJobs, func(i, j int) bool {
		if allJobs[i].StartTime > allJobs[j].StartTime {
			return true
		} else if allJobs[i].StartTime < allJobs[j].StartTime {
			return false
		}
		return allJobs[i].Instance < allJobs[j].Instance
	})
	e.jobs = allJobs
	return nil
}

// Next implements the Executor Next interface.
func (e *TrafficShowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	batchSize := min(e.MaxChunkSize(), len(e.jobs)-e.cursor)
	req.GrowAndReset(batchSize)
	for i := 0; i < batchSize; i++ {
		job := e.jobs[e.cursor]
		e.cursor++
		req.AppendTime(0, parseTime(ctx, e.BaseExecutor, job.StartTime))
		req.AppendTime(1, parseTime(ctx, e.BaseExecutor, job.EndTime))
		req.AppendString(2, job.Instance)
		req.AppendString(3, job.Type)
		req.AppendString(4, job.Progress)
		req.AppendString(5, job.Status)
		req.AppendString(6, job.Err)
	}
	return nil
}

func request(ctx context.Context, exec exec.BaseExecutor, reader io.Reader, method, path string) (map[string]string, error) {
	addrs, err := getTiProxyAddrs(ctx)
	if err != nil {
		return nil, err
	}
	resps := make(map[string]string, len(addrs))
	for _, addr := range addrs {
		resp, requestErr := requestOne(method, addr, path, reader)
		if requestErr != nil {
			// Let's send requests to all the instances even if some fail.
			exec.Ctx().GetSessionVars().StmtCtx.AppendError(requestErr)
			logutil.Logger(ctx).Error("traffic request to tiproxy failed", zap.String("method", method),
				zap.String("path", path), zap.String("addr", addr), zap.String("resp", resp), zap.Error(requestErr))
			if err == nil {
				err = requestErr
			}
		} else {
			resps[addr] = resp
		}
	}
	if err == nil {
		logutil.Logger(ctx).Info("traffic request to tiproxy succeeds", zap.Any("addrs", addrs), zap.String("path", path))
	}
	return resps, err
}

func getTiProxyAddrs(ctx context.Context) ([]string, error) {
	var tiproxyNodes map[string]*infosync.TiProxyServerInfo
	var err error
	if v := ctx.Value(tiproxyAddrKey); v != nil {
		tiproxyNodes = v.(map[string]*infosync.TiProxyServerInfo)
	} else {
		tiproxyNodes, err = infosync.GetTiProxyServerInfo(ctx)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(tiproxyNodes) == 0 {
		return nil, errors.Errorf("no tiproxy server found")
	}
	servers := make([]string, 0, len(tiproxyNodes))
	for _, node := range tiproxyNodes {
		servers = append(servers, net.JoinHostPort(node.IP, node.StatusPort))
	}
	return servers, nil
}

func requestOne(method, addr, path string, rd io.Reader) (string, error) {
	url := fmt.Sprintf("%s://%s/%s", util.InternalHTTPSchema(), addr, path)
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return "", errors.Trace(err)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer func() {
		terror.Log(resp.Body.Close())
	}()
	resb, err := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
		return string(resb), err
	default:
		return string(resb), errors.Errorf("request %s failed: %s", url, resp.Status)
	}
}

func getForm(m map[string]string) string {
	form := url.Values{}
	for key, value := range m {
		form.Add(key, value)
	}
	return form.Encode()
}

func parseTime(ctx context.Context, exec exec.BaseExecutor, timeStr string) types.Time {
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		exec.Ctx().GetSessionVars().StmtCtx.AppendError(err)
		logutil.Logger(ctx).Error("parse time failed", zap.String("time", timeStr), zap.Error(err))
	}
	return types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.MaxFsp)
}
