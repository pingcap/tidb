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

// Contexts for HTTP requests communicating with a real HTTP server are essential,
// however, when the subject is a mocked server, it would probably be redundant.
//nolint:noctx

package lightning

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/web"
	"github.com/stretchr/testify/require"
)

// initProgressOnce is used to ensure init progress once to avoid data race.
var initProgressOnce sync.Once

type lightningServerSuite struct {
	lightning *Lightning
	taskCfgCh chan *config.Config
	taskRunCh chan struct{}
}

func createSuite(t *testing.T) (s *lightningServerSuite, clean func()) {
	initProgressOnce.Do(web.EnableCurrentProgress)

	cfg := config.NewGlobalConfig()
	cfg.TiDB.Host = "test.invalid"
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "test.invalid:2379"
	cfg.App.ServerMode = true
	cfg.App.StatusAddr = "127.0.0.1:0"
	cfg.Mydumper.SourceDir = "file://."
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = t.TempDir()

	s = new(lightningServerSuite)
	s.lightning = New(cfg)
	s.taskRunCh = make(chan struct{}, 1)
	s.taskCfgCh = make(chan *config.Config)
	s.lightning.ctx = context.WithValue(s.lightning.ctx, taskRunNotifyKey, s.taskRunCh)
	s.lightning.ctx = context.WithValue(s.lightning.ctx, taskCfgRecorderKey, s.taskCfgCh)
	_ = s.lightning.GoServe()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/SkipRunTask", "return"))
	clean = func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/SkipRunTask"))
		s.lightning.Stop()
	}

	return
}

func TestRunServer(t *testing.T) {
	s, clean := createSuite(t)
	defer clean()

	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	resp, err := http.Post(url, "application/toml", strings.NewReader("????"))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	var data map[string]string
	err = json.NewDecoder(resp.Body).Decode(&data)
	require.NoError(t, err)
	require.Contains(t, data, "error")
	require.Equal(t, "server-mode not enabled", data["error"])
	require.NoError(t, resp.Body.Close())

	go func() {
		_ = s.lightning.RunServer()
	}()
	time.Sleep(100 * time.Millisecond)

	req, err := http.NewRequest(http.MethodPut, url, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Allow"), http.MethodPost)
	require.NoError(t, resp.Body.Close())

	resp, err = http.Post(url, "application/toml", strings.NewReader("????"))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&data)
	require.NoError(t, err)
	require.Contains(t, data, "error")
	require.Regexp(t, "^cannot parse task", data["error"])
	require.NoError(t, resp.Body.Close())

	resp, err = http.Post(url, "application/toml", strings.NewReader("[mydumper.csv]\nseparator = 'fooo'\ndelimiter= 'foo'"))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&data)
	require.NoError(t, err)
	require.Contains(t, data, "error")
	require.Regexp(t, "^invalid task configuration:", data["error"])
	require.NoError(t, resp.Body.Close())

	for i := 0; i < 20; i++ {
		resp, err = http.Post(url, "application/toml", strings.NewReader(fmt.Sprintf(`
			[mydumper]
			data-source-dir = 'file://demo-path-%d'
			[mydumper.csv]
			separator = '/'
		`, i)))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var result map[string]int
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
		require.Contains(t, result, "id")

		select {
		case taskCfg := <-s.taskCfgCh:
			require.Equal(t, "test.invalid", taskCfg.TiDB.Host)
			require.Equal(t, fmt.Sprintf("file://demo-path-%d", i), taskCfg.Mydumper.SourceDir)
			require.Equal(t, "/", taskCfg.Mydumper.CSV.Separator)
		case <-time.After(5 * time.Second):
			t.Fatalf("task is not queued after 5 seconds (i = %d)", i)
		}
	}
}

func TestGetDeleteTask(t *testing.T) {
	s, clean := createSuite(t)
	defer clean()

	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	type getAllResultType struct {
		Current int64
		Queue   []int64
	}

	getAllTasks := func() (result getAllResultType) {
		resp, err := http.Get(url)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
		return
	}

	postTask := func(i int) int64 {
		resp, err := http.Post(url, "application/toml", strings.NewReader(fmt.Sprintf(`
			[mydumper]
			data-source-dir = 'file://demo-path-%d'
		`, i)))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var result struct{ ID int64 }
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
		return result.ID
	}

	go func() {
		_ = s.lightning.RunServer()
	}()
	time.Sleep(500 * time.Millisecond)

	// Check `GET /tasks` without any active tasks
	require.Equal(t, getAllResultType{
		Current: 0,
		Queue:   []int64{},
	}, getAllTasks())

	first := postTask(1)
	second := postTask(2)
	third := postTask(3)

	require.NotEqual(t, 123456, first)
	require.NotEqual(t, 123456, second)
	require.NotEqual(t, 123456, third)

	// Check `GET /tasks` returns all tasks currently running

	<-s.taskRunCh
	require.Equal(t, getAllResultType{
		Current: first,
		Queue:   []int64{second, third},
	}, getAllTasks())

	// Check `GET /tasks/abcdef` returns error

	resp, err := http.Get(url + "/abcdef")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Check `GET /tasks/123456` returns not found

	resp, err = http.Get(url + "/123456")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Check `GET /tasks/1` returns the desired cfg

	var resCfg config.Config

	resp, err = http.Get(fmt.Sprintf("%s/%d", url, second))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&resCfg)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, "file://demo-path-2", resCfg.Mydumper.SourceDir)

	resp, err = http.Get(fmt.Sprintf("%s/%d", url, first))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&resCfg)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, "file://demo-path-1", resCfg.Mydumper.SourceDir)

	// Check `DELETE /tasks` returns error.

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Check `DELETE /tasks/` returns error.

	req.URL.Path = "/tasks/"
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Check `DELETE /tasks/(not a number)` returns error.

	req.URL.Path = "/tasks/abcdef"
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Check `DELETE /tasks/123456` returns not found

	req.URL.Path = "/tasks/123456"
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Cancel a queued task, then verify the task list.

	req.URL.Path = fmt.Sprintf("/tasks/%d", second)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	require.Equal(t, getAllResultType{
		Current: first,
		Queue:   []int64{third},
	}, getAllTasks())

	// Cancel a running task, then verify the task list.

	req.URL.Path = fmt.Sprintf("/tasks/%d", first)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	<-s.taskRunCh
	require.Equal(t, getAllResultType{
		Current: third,
		Queue:   []int64{},
	}, getAllTasks())
}

func TestHTTPAPIOutsideServerMode(t *testing.T) {
	s, clean := createSuite(t)
	defer clean()

	s.lightning.globalCfg.App.ServerMode = false

	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	errCh := make(chan error)
	cfg := config.NewConfig()
	cfg.TiDB.DistSQLScanConcurrency = 4
	err := cfg.LoadFromGlobal(s.lightning.globalCfg)
	require.NoError(t, err)
	go func() {
		errCh <- s.lightning.RunOnceWithOptions(s.lightning.ctx, cfg)
	}()
	time.Sleep(600 * time.Millisecond)

	var curTask struct {
		Current int64
		Queue   []int64
	}

	// `GET /tasks` should work fine.
	resp, err := http.Get(url)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&curTask)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.NotEqual(t, int64(0), curTask.Current)
	require.Len(t, curTask.Queue, 0)

	// `POST /tasks` should return 501
	resp, err = http.Post(url, "application/toml", strings.NewReader("??????"))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// `GET /tasks/(current)` should work fine.
	resp, err = http.Get(fmt.Sprintf("%s/%d", url, curTask.Current))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// `GET /tasks/123456` should return 404
	resp, err = http.Get(url + "/123456")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// `PATCH /tasks/(current)/front` should return 501
	req, err := http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/%d/front", url, curTask.Current), nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// `DELETE /tasks/123456` should return 404
	req.Method = http.MethodDelete
	req.URL.Path = "/tasks/123456"
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// `DELETE /tasks/(current)` should return 200
	req.URL.Path = fmt.Sprintf("/tasks/%d", curTask.Current)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	// ... and the task should be canceled now.
	require.Equal(t, context.Canceled, <-errCh)
}
