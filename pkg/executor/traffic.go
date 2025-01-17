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
	"maps"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
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
type trafficPathKeyType struct{}

var tiproxyAddrKey tiproxyAddrKeyType
var trafficPathKey trafficPathKeyType

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
	outputKey    = "output"
	inputKey     = "input"

	capturePath = "/api/traffic/capture"
	replayPath  = "/api/traffic/replay"
	cancelPath  = "/api/traffic/cancel"
	showPath    = "/api/traffic/show"

	sharedStorageTimeout = 10 * time.Second
	filePrefix           = "tiproxy-"
)

// TrafficCaptureExec sends capture traffic requests to TiProxy.
type TrafficCaptureExec struct {
	exec.BaseExecutor
	Args map[string]string
}

// Next implements the Executor Next interface.
func (e *TrafficCaptureExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	e.Args[startTimeKey] = time.Now().Format(time.RFC3339)
	addrs, err := getTiProxyAddrs(ctx)
	if err != nil {
		return errors.Wrapf(err, "get tiproxy addresses failed")
	}
	// For shared storage, append a suffix to the output path for each TiProxy so that they won't write to the same path.
	readers, err := formReader4Capture(e.Args, len(addrs))
	if err != nil {
		return err
	}
	_, err = request(ctx, addrs, readers, http.MethodPost, capturePath)
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
	addrs, err := getTiProxyAddrs(ctx)
	if err != nil {
		return errors.Wrapf(err, "get tiproxy addresses failed")
	}
	// For shared storage, read the sub-direcotires from the input path and assign each sub-directory to a TiProxy instance.
	formCtx, cancel := context.WithTimeout(ctx, sharedStorageTimeout)
	readers, err := formReader4Replay(formCtx, e.Args, len(addrs))
	cancel()
	if err != nil {
		return err
	}
	readerNum, tiproxyNum := len(readers), len(addrs)
	if readerNum > tiproxyNum {
		logutil.Logger(ctx).Error("tiproxy instances number is less than input paths number", zap.Int("tiproxy number", tiproxyNum),
			zap.Int("path number", readerNum))
		return errors.Errorf("tiproxy instances number (%d) is less than input paths number (%d)", tiproxyNum, readerNum)
	} else if readerNum < tiproxyNum {
		addrs = addrs[:readerNum]
		err = errors.Errorf("tiproxy instances number (%d) is greater than input paths number (%d), some instances won't replay", tiproxyNum, readerNum)
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		logutil.Logger(ctx).Warn("tiproxy instances number is greater than input paths number, some instances won't replay",
			zap.Int("tiproxy number", tiproxyNum), zap.Int("path number", readerNum))
	}
	_, err = request(ctx, addrs, readers, http.MethodPost, replayPath)
	return err
}

// TrafficCancelExec sends cancel traffic job requests to TiProxy.
type TrafficCancelExec struct {
	exec.BaseExecutor
}

// Next implements the Executor Next interface.
func (*TrafficCancelExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	addrs, err := getTiProxyAddrs(ctx)
	if err != nil {
		return errors.Wrapf(err, "get tiproxy addresses failed")
	}
	_, err = request(ctx, addrs, nil, http.MethodPost, cancelPath)
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
	addrs, err := getTiProxyAddrs(ctx)
	if err != nil {
		return errors.Wrapf(err, "get tiproxy addresses failed")
	}
	resps, err := request(ctx, addrs, nil, http.MethodGet, showPath)
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

func request(ctx context.Context, addrs []string, readers []io.Reader, method, path string) (map[string]string, error) {
	resps := make(map[string]string, len(addrs))
	for i, addr := range addrs {
		var reader io.Reader
		if readers != nil && i < len(readers) {
			reader = readers[i]
		}
		resp, err := requestOne(method, addr, path, reader)
		if err != nil {
			logutil.Logger(ctx).Error("traffic request to tiproxy failed", zap.String("path", path), zap.String("addr", addr),
				zap.String("resp", resp), zap.Error(err))
			return resps, errors.Wrapf(err, "request to tiproxy '%s' failed", addr)
		}
		resps[addr] = resp
	}
	logutil.Logger(ctx).Info("traffic request to tiproxy succeeds", zap.Strings("addrs", addrs), zap.String("path", path))
	return resps, nil
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
	url := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), addr, path)
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

func formReader4Capture(args map[string]string, tiproxyNum int) ([]io.Reader, error) {
	output, ok := args[outputKey]
	if !ok || len(output) == 0 {
		return nil, errors.New("the output path for capture must be specified")
	}
	u, err := url.Parse(output)
	if err != nil {
		return nil, errors.Wrapf(err, "parse output path failed")
	}
	readers := make([]io.Reader, tiproxyNum)
	if storage.IsLocal(u) {
		form := getForm(args)
		for i := 0; i < tiproxyNum; i++ {
			readers[i] = strings.NewReader(form)
		}
	} else {
		for i := 0; i < tiproxyNum; i++ {
			m := maps.Clone(args)
			m[outputKey] = u.JoinPath(fmt.Sprintf("%s%d", filePrefix, i)).String()
			form := getForm(m)
			readers[i] = strings.NewReader(form)
		}
	}
	return readers, nil
}

func formReader4Replay(ctx context.Context, args map[string]string, tiproxyNum int) ([]io.Reader, error) {
	input, ok := args[inputKey]
	if !ok || len(input) == 0 {
		return nil, errors.New("the input path for replay must be specified")
	}
	u, err := storage.ParseRawURL(input)
	if err != nil {
		return nil, errors.Wrapf(err, "parse input path failed")
	}
	if storage.IsLocal(u) {
		readers := make([]io.Reader, tiproxyNum)
		form := getForm(args)
		for i := 0; i < tiproxyNum; i++ {
			readers[i] = strings.NewReader(form)
		}
		return readers, nil
	}

	names := make([]string, 0, tiproxyNum)
	if mockNames := ctx.Value(trafficPathKey); mockNames != nil {
		names = mockNames.([]string)
	} else {
		backend, err := storage.ParseBackendFromURL(u, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "parse backend from the input path failed")
		}
		store, err := storage.NewWithDefaultOpt(ctx, backend)
		if err != nil {
			return nil, errors.Wrapf(err, "create storage for input failed")
		}
		defer store.Close()
		err = store.WalkDir(ctx, &storage.WalkOption{
			ObjPrefix: filePrefix,
		}, func(name string, _ int64) error {
			names = append(names, name)
			return nil
		})
		if err != nil {
			return nil, errors.Wrapf(err, "walk input path failed")
		}
	}
	if len(names) == 0 {
		return nil, errors.New("no replay files found in the input path")
	}
	readers := make([]io.Reader, 0, len(names))
	for _, name := range names {
		m := maps.Clone(args)
		m[inputKey] = u.JoinPath(name).String()
		form := getForm(m)
		readers = append(readers, strings.NewReader(form))
	}
	return readers, nil
}
