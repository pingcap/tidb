// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Contexts for HTTP requests communicating with a real HTTP server are essential,
// however, when the subject is a mocked server, it would probably be redundant.
//nolint:noctx

// lightningSuite.SetUpTest sets up global logger but the gocheck framework calls this method
// multi times, hence data race may happen. However, the operation setting up the global logger is idempotent.
// Hence in real life the race is harmless. Disable this when race enabled till this get fixed.
// +build !race

package lightning

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-units"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
)

type lightningSuite struct{}

var _ = Suite(&lightningSuite{})

func TestLightning(t *testing.T) {
	TestingT(t)
}

func (s *lightningSuite) TestInitEnv(c *C) {
	cfg := &config.GlobalConfig{
		App: config.GlobalLightning{StatusAddr: ":45678"},
	}
	err := initEnv(cfg)
	c.Assert(err, IsNil)
	cfg.App.StatusAddr = ""
	cfg.App.Config.File = "."
	err = initEnv(cfg)
	c.Assert(err, ErrorMatches, "can't use directory as log file name")
}

func (s *lightningSuite) TestRun(c *C) {
	globalConfig := config.NewGlobalConfig()
	globalConfig.TiDB.Host = "test.invalid"
	globalConfig.TiDB.Port = 4000
	globalConfig.TiDB.PdAddr = "test.invalid:2379"
	globalConfig.Mydumper.SourceDir = "not-exists"
	globalConfig.TikvImporter.Backend = config.BackendLocal
	globalConfig.TikvImporter.SortedKVDir = c.MkDir()
	lightning := New(globalConfig)
	cfg := config.NewConfig()
	err := cfg.LoadFromGlobal(globalConfig)
	c.Assert(err, IsNil)
	err = lightning.RunOnce(context.Background(), cfg, nil)
	c.Assert(err, ErrorMatches, ".*mydumper dir does not exist")

	path, _ := filepath.Abs(".")
	ctx := context.Background()
	invalidGlue := glue.NewExternalTiDBGlue(nil, 0)
	err = lightning.run(ctx, &config.Config{
		Mydumper: config.MydumperRuntime{
			SourceDir:        "file://" + filepath.ToSlash(path),
			Filter:           []string{"*.*"},
			DefaultFileRules: true,
		},
		Checkpoint: config.Checkpoint{
			Enable: true,
			Driver: "invalid",
		},
	}, invalidGlue)
	c.Assert(err, ErrorMatches, "open checkpoint db failed: Unknown checkpoint driver invalid")

	err = lightning.run(ctx, &config.Config{
		Mydumper: config.MydumperRuntime{
			SourceDir: ".",
			Filter:    []string{"*.*"},
		},
		Checkpoint: config.Checkpoint{
			Enable: true,
			Driver: "file",
			DSN:    "any-file",
		},
	}, invalidGlue)
	c.Assert(err, NotNil)
}

var _ = Suite(&lightningServerSuite{})

type lightningServerSuite struct {
	lightning *Lightning
	taskCfgCh chan *config.Config
}

func (s *lightningServerSuite) SetUpTest(c *C) {
	cfg := config.NewGlobalConfig()
	cfg.TiDB.Host = "test.invalid"
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "test.invalid:2379"
	cfg.App.ServerMode = true
	cfg.App.StatusAddr = "127.0.0.1:0"
	cfg.Mydumper.SourceDir = "file://."
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.SortedKVDir = c.MkDir()

	s.lightning = New(cfg)
	s.taskCfgCh = make(chan *config.Config)
	s.lightning.ctx = context.WithValue(s.lightning.ctx, &taskCfgRecorderKey, s.taskCfgCh)
	_ = s.lightning.GoServe()

	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/SkipRunTask", "return")
}

func (s *lightningServerSuite) TearDownTest(c *C) {
	_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/SkipRunTask")
	s.lightning.Stop()
}

func (s *lightningServerSuite) TestRunServer(c *C) {
	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	resp, err := http.Post(url, "application/toml", strings.NewReader("????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotImplemented)
	var data map[string]string
	err = json.NewDecoder(resp.Body).Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data, HasKey, "error")
	c.Assert(data["error"], Equals, "server-mode not enabled")
	resp.Body.Close()

	go func() {
		_ = s.lightning.RunServer()
	}()
	time.Sleep(100 * time.Millisecond)

	req, err := http.NewRequest(http.MethodPut, url, nil)
	c.Assert(err, IsNil)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusMethodNotAllowed)
	c.Assert(resp.Header.Get("Allow"), Matches, ".*"+http.MethodPost+".*")
	resp.Body.Close()

	resp, err = http.Post(url, "application/toml", strings.NewReader("????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	err = json.NewDecoder(resp.Body).Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data, HasKey, "error")
	c.Assert(data["error"], Matches, "cannot parse task.*")
	resp.Body.Close()

	resp, err = http.Post(url, "application/toml", strings.NewReader("[mydumper.csv]\nseparator = 'fooo'\ndelimiter= 'foo'"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	err = json.NewDecoder(resp.Body).Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data, HasKey, "error")
	c.Assert(data["error"], Matches, "invalid task configuration:.*")
	resp.Body.Close()

	for i := 0; i < 20; i++ {
		resp, err = http.Post(url, "application/toml", strings.NewReader(fmt.Sprintf(`
			[mydumper]
			data-source-dir = 'file://demo-path-%d'
			[mydumper.csv]
			separator = '/'
		`, i)))
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		var result map[string]int
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(result, HasKey, "id")

		select {
		case taskCfg := <-s.taskCfgCh:
			c.Assert(taskCfg.TiDB.Host, Equals, "test.invalid")
			c.Assert(taskCfg.Mydumper.SourceDir, Equals, fmt.Sprintf("file://demo-path-%d", i))
			c.Assert(taskCfg.Mydumper.CSV.Separator, Equals, "/")
		case <-time.After(5 * time.Second):
			c.Fatalf("task is not queued after 5 seconds (i = %d)", i)
		}
	}
}

func (s *lightningServerSuite) TestGetDeleteTask(c *C) {
	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	type getAllResultType struct {
		Current int64
		Queue   []int64
	}

	getAllTasks := func() (result getAllResultType) {
		resp, err := http.Get(url)
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		c.Assert(err, IsNil)
		return
	}

	postTask := func(i int) int64 {
		resp, err := http.Post(url, "application/toml", strings.NewReader(fmt.Sprintf(`
			[mydumper]
			data-source-dir = 'file://demo-path-%d'
		`, i)))
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		var result struct{ ID int64 }
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		c.Assert(err, IsNil)
		return result.ID
	}

	go func() {
		_ = s.lightning.RunServer()
	}()
	time.Sleep(100 * time.Millisecond)

	// Check `GET /tasks` without any active tasks

	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: 0,
		Queue:   []int64{},
	})

	first := postTask(1)
	second := postTask(2)
	third := postTask(3)

	c.Assert(first, Not(Equals), 123456)
	c.Assert(second, Not(Equals), 123456)
	c.Assert(third, Not(Equals), 123456)

	// Check `GET /tasks` returns all tasks currently running

	time.Sleep(100 * time.Millisecond)
	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: first,
		Queue:   []int64{second, third},
	})

	// Check `GET /tasks/abcdef` returns error

	resp, err := http.Get(url + "/abcdef")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `GET /tasks/123456` returns not found

	resp, err = http.Get(url + "/123456")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	resp.Body.Close()

	// Check `GET /tasks/1` returns the desired cfg

	var resCfg config.Config

	resp, err = http.Get(fmt.Sprintf("%s/%d", url, second))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	err = json.NewDecoder(resp.Body).Decode(&resCfg)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resCfg.Mydumper.SourceDir, Equals, "file://demo-path-2")

	resp, err = http.Get(fmt.Sprintf("%s/%d", url, first))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	err = json.NewDecoder(resp.Body).Decode(&resCfg)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resCfg.Mydumper.SourceDir, Equals, "file://demo-path-1")

	// Check `DELETE /tasks` returns error.

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	c.Assert(err, IsNil)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `DELETE /tasks/` returns error.

	req.URL.Path = "/tasks/"
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `DELETE /tasks/(not a number)` returns error.

	req.URL.Path = "/tasks/abcdef"
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()

	// Check `DELETE /tasks/123456` returns not found

	req.URL.Path = "/tasks/123456"
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	resp.Body.Close()

	// Cancel a queued task, then verify the task list.

	req.URL.Path = fmt.Sprintf("/tasks/%d", second)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp.Body.Close()

	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: first,
		Queue:   []int64{third},
	})

	// Cancel a running task, then verify the task list.

	req.URL.Path = fmt.Sprintf("/tasks/%d", first)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp.Body.Close()

	time.Sleep(100 * time.Millisecond)
	c.Assert(getAllTasks(), DeepEquals, getAllResultType{
		Current: third,
		Queue:   []int64{},
	})
}

func (s *lightningServerSuite) TestHTTPAPIOutsideServerMode(c *C) {
	s.lightning.globalCfg.App.ServerMode = false

	url := "http://" + s.lightning.serverAddr.String() + "/tasks"

	errCh := make(chan error)
	cfg := config.NewConfig()
	err := cfg.LoadFromGlobal(s.lightning.globalCfg)
	c.Assert(err, IsNil)
	go func() {
		errCh <- s.lightning.RunOnce(s.lightning.ctx, cfg, nil)
	}()
	time.Sleep(100 * time.Millisecond)

	var curTask struct {
		Current int64
		Queue   []int64
	}

	// `GET /tasks` should work fine.
	resp, err := http.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	err = json.NewDecoder(resp.Body).Decode(&curTask)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(curTask.Current, Not(Equals), int64(0))
	c.Assert(curTask.Queue, HasLen, 0)

	// `POST /tasks` should return 501
	resp, err = http.Post(url, "application/toml", strings.NewReader("??????"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotImplemented)
	resp.Body.Close()

	// `GET /tasks/(current)` should work fine.
	resp, err = http.Get(fmt.Sprintf("%s/%d", url, curTask.Current))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp.Body.Close()

	// `GET /tasks/123456` should return 404
	resp, err = http.Get(url + "/123456")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	resp.Body.Close()

	// `PATCH /tasks/(current)/front` should return 501
	req, err := http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/%d/front", url, curTask.Current), nil)
	c.Assert(err, IsNil)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotImplemented)
	resp.Body.Close()

	// `DELETE /tasks/123456` should return 404
	req.Method = http.MethodDelete
	req.URL.Path = "/tasks/123456"
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	resp.Body.Close()

	// `DELETE /tasks/(current)` should return 200
	req.URL.Path = fmt.Sprintf("/tasks/%d", curTask.Current)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(resp.Body.Close(), IsNil)
	// ... and the task should be canceled now.
	c.Assert(<-errCh, Equals, context.Canceled)
}

func (s *lightningServerSuite) TestCheckSystemRequirement(c *C) {
	if runtime.GOOS == "windows" {
		c.Skip("Local-backend is not supported on Windows")
		return
	}

	cfg := config.NewConfig()
	cfg.App.CheckRequirements = true
	cfg.App.TableConcurrency = 4
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.EngineMemCacheSize = 512 * units.MiB

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Tables: []*mydump.MDTableMeta{
				{
					TotalSize: 500 << 20,
				},
				{
					TotalSize: 150_000 << 20,
				},
			},
		},
		{
			Tables: []*mydump.MDTableMeta{
				{
					TotalSize: 150_800 << 20,
				},
				{
					TotalSize: 35 << 20,
				},
				{
					TotalSize: 100_000 << 20,
				},
			},
		},
		{
			Tables: []*mydump.MDTableMeta{
				{
					TotalSize: 240 << 20,
				},
				{
					TotalSize: 124_000 << 20,
				},
			},
		},
	}

	// with max open files 1024, the max table size will be: 65536MB
	err := failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue", "return(2049)")
	c.Assert(err, IsNil)
	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/SetRlimitError", "return(true)")
	c.Assert(err, IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/SetRlimitError")
	}()
	// with this dbMetas, the estimated fds will be 2050, so should return error
	err = checkSystemRequirement(cfg, dbMetas)
	c.Assert(err, NotNil)

	err = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue")
	c.Assert(err, IsNil)

	// the min rlimit should be bigger than the default min value (16384)
	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue", "return(8200)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/GetRlimitValue")
	}()
	c.Assert(err, IsNil)
	err = checkSystemRequirement(cfg, dbMetas)
	c.Assert(err, IsNil)
}

func (s *lightningServerSuite) TestCheckSchemaConflict(c *C) {
	cfg := config.NewConfig()
	cfg.Checkpoint.Schema = "cp"
	cfg.Checkpoint.Driver = config.CheckpointDriverMySQL

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "test",
			Tables: []*mydump.MDTableMeta{
				{
					Name: checkpoints.CheckpointTableNameTable,
				},
				{
					Name: checkpoints.CheckpointTableNameEngine,
				},
			},
		},
		{
			Name: "cp",
			Tables: []*mydump.MDTableMeta{
				{
					Name: "test",
				},
			},
		},
	}
	err := checkSchemaConflict(cfg, dbMetas)
	c.Assert(err, IsNil)

	dbMetas = append(dbMetas, &mydump.MDDatabaseMeta{
		Name: "cp",
		Tables: []*mydump.MDTableMeta{
			{
				Name: checkpoints.CheckpointTableNameChunk,
			},
			{
				Name: "test123",
			},
		},
	})
	err = checkSchemaConflict(cfg, dbMetas)
	c.Assert(err, NotNil)

	cfg.Checkpoint.Enable = false
	err = checkSchemaConflict(cfg, dbMetas)
	c.Assert(err, IsNil)

	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.Driver = config.CheckpointDriverFile
	err = checkSchemaConflict(cfg, dbMetas)
	c.Assert(err, IsNil)
}
