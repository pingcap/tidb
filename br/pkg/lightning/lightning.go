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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lightning

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/importer"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/restore"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
	"github.com/pingcap/tidb/br/pkg/lightning/web"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shurcooL/httpgzip"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Lightning struct {
	globalCfg *config.GlobalConfig
	globalTLS *common.TLS
	// taskCfgs is the list of task configurations enqueued in the server mode
	taskCfgs   *config.List
	ctx        context.Context
	shutdown   context.CancelFunc // for whole lightning context
	server     http.Server
	serverAddr net.Addr
	serverLock sync.Mutex
	status     restore.LightningStatus

	cancelLock sync.Mutex
	curTask    *config.Config
	cancel     context.CancelFunc // for per task context, which maybe different from lightning context
}

func initEnv(cfg *config.GlobalConfig) error {
	if cfg.App.Config.File == "" {
		return nil
	}
	return log.InitLogger(&cfg.App.Config, cfg.TiDB.LogLevel)
}

func New(globalCfg *config.GlobalConfig) *Lightning {
	if err := initEnv(globalCfg); err != nil {
		fmt.Println("Failed to initialize environment:", err)
		os.Exit(1)
	}

	tls, err := common.NewTLS(globalCfg.Security.CAPath, globalCfg.Security.CertPath, globalCfg.Security.KeyPath, globalCfg.App.StatusAddr)
	if err != nil {
		log.L().Fatal("failed to load TLS certificates", zap.Error(err))
	}

	redact.InitRedact(globalCfg.Security.RedactInfoLog)

	ctx, shutdown := context.WithCancel(context.Background())
	return &Lightning{
		globalCfg: globalCfg,
		globalTLS: tls,
		ctx:       ctx,
		shutdown:  shutdown,
	}
}

func (l *Lightning) GoServe() error {
	handleSigUsr1(func() {
		l.serverLock.Lock()
		statusAddr := l.globalCfg.App.StatusAddr
		shouldStartServer := len(statusAddr) == 0
		if shouldStartServer {
			l.globalCfg.App.StatusAddr = ":"
		}
		l.serverLock.Unlock()

		if shouldStartServer {
			// open a random port and start the server if SIGUSR1 is received.
			if err := l.goServe(":", os.Stderr); err != nil {
				log.L().Warn("failed to start HTTP server", log.ShortError(err))
			}
		} else {
			// just prints the server address if it is already started.
			log.L().Info("already started HTTP server", zap.Stringer("address", l.serverAddr))
		}
	})

	l.serverLock.Lock()
	statusAddr := l.globalCfg.App.StatusAddr
	l.serverLock.Unlock()

	if len(statusAddr) == 0 {
		return nil
	}
	return l.goServe(statusAddr, io.Discard)
}

func (l *Lightning) goServe(statusAddr string, realAddrWriter io.Writer) error {
	mux := http.NewServeMux()
	mux.Handle("/", http.RedirectHandler("/web/", http.StatusFound))
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	handleTasks := http.StripPrefix("/tasks", http.HandlerFunc(l.handleTask))
	mux.Handle("/tasks", handleTasks)
	mux.Handle("/tasks/", handleTasks)
	mux.HandleFunc("/progress/task", handleProgressTask)
	mux.HandleFunc("/progress/table", handleProgressTable)
	mux.HandleFunc("/pause", handlePause)
	mux.HandleFunc("/resume", handleResume)
	mux.HandleFunc("/loglevel", handleLogLevel)

	mux.Handle("/web/", http.StripPrefix("/web", httpgzip.FileServer(web.Res, httpgzip.FileServerOptions{
		IndexHTML: true,
		ServeError: func(w http.ResponseWriter, req *http.Request, err error) {
			if os.IsNotExist(err) && !strings.Contains(req.URL.Path, ".") {
				http.Redirect(w, req, "/web/", http.StatusFound)
			} else {
				httpgzip.NonSpecific(w, req, err)
			}
		},
	})))

	listener, err := net.Listen("tcp", statusAddr)
	if err != nil {
		return err
	}
	l.serverAddr = listener.Addr()
	log.L().Info("starting HTTP server", zap.Stringer("address", l.serverAddr))
	fmt.Fprintln(realAddrWriter, "started HTTP server on", l.serverAddr)
	l.server.Handler = mux
	listener = l.globalTLS.WrapListener(listener)

	go func() {
		err := l.server.Serve(listener)
		log.L().Info("stopped HTTP server", log.ShortError(err))
	}()
	return nil
}

// RunOnce is used by binary lightning and host when using lightning as a library.
// - for binary lightning, taskCtx could be context.Background which means taskCtx wouldn't be canceled directly by its
//   cancel function, but only by Lightning.Stop or HTTP DELETE using l.cancel. and glue could be nil to let lightning
//   use a default glue later.
// - for lightning as a library, taskCtx could be a meaningful context that get canceled outside, and glue could be a
//   caller implemented glue.
func (l *Lightning) RunOnce(taskCtx context.Context, taskCfg *config.Config, glue glue.Glue) error {
	if err := taskCfg.Adjust(taskCtx); err != nil {
		return err
	}

	taskCfg.TaskID = time.Now().UnixNano()
	failpoint.Inject("SetTaskID", func(val failpoint.Value) {
		taskCfg.TaskID = int64(val.(int))
	})

	return l.run(taskCtx, taskCfg, glue)
}

func (l *Lightning) RunServer() error {
	l.serverLock.Lock()
	l.taskCfgs = config.NewConfigList()
	l.serverLock.Unlock()
	log.L().Info(
		"Lightning server is running, post to /tasks to start an import task",
		zap.Stringer("address", l.serverAddr),
	)

	for {
		task, err := l.taskCfgs.Pop(l.ctx)
		if err != nil {
			return err
		}
		err = l.run(context.Background(), task, nil)
		if err != nil && !common.IsContextCanceledError(err) {
			restore.DeliverPauser.Pause() // force pause the progress on error
			log.L().Error("tidb lightning encountered error", zap.Error(err))
		}
	}
}

var (
	taskRunNotifyKey   = "taskRunNotifyKey"
	taskCfgRecorderKey = "taskCfgRecorderKey"
)

func (l *Lightning) run(taskCtx context.Context, taskCfg *config.Config, g glue.Glue) (err error) {
	build.LogInfo(build.Lightning)
	log.L().Info("cfg", zap.Stringer("cfg", taskCfg))

	utils.LogEnvVariables()

	ctx, cancel := context.WithCancel(taskCtx)
	l.cancelLock.Lock()
	l.cancel = cancel
	l.curTask = taskCfg
	l.cancelLock.Unlock()
	web.BroadcastStartTask()

	defer func() {
		cancel()
		l.cancelLock.Lock()
		l.cancel = nil
		l.cancelLock.Unlock()
		web.BroadcastEndTask(err)
	}()

	failpoint.Inject("SkipRunTask", func() {
		if notifyCh, ok := l.ctx.Value(taskRunNotifyKey).(chan struct{}); ok {
			select {
			case notifyCh <- struct{}{}:
			default:
			}
		}
		if recorder, ok := l.ctx.Value(taskCfgRecorderKey).(chan *config.Config); ok {
			select {
			case recorder <- taskCfg:
			case <-ctx.Done():
				failpoint.Return(ctx.Err())
			}
		}
		failpoint.Return(nil)
	})

	if err := taskCfg.TiDB.Security.RegisterMySQL(); err != nil {
		return common.ErrInvalidTLSConfig.Wrap(err)
	}
	defer func() {
		// deregister TLS config with name "cluster"
		if taskCfg.TiDB.Security == nil {
			return
		}
		taskCfg.TiDB.Security.DeregisterMySQL()
	}()

	// initiation of default glue should be after RegisterMySQL, which is ready to be called after taskCfg.Adjust
	// and also put it here could avoid injecting another two SkipRunTask failpoint to caller
	if g == nil {
		db, err := restore.DBFromConfig(ctx, taskCfg.TiDB)
		if err != nil {
			return common.ErrDBConnect.Wrap(err)
		}
		g = glue.NewExternalTiDBGlue(db, taskCfg.TiDB.SQLMode)
	}

	u, err := storage.ParseBackend(taskCfg.Mydumper.SourceDir, nil)
	if err != nil {
		return common.NormalizeError(err)
	}
	s, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return common.NormalizeError(err)
	}

	// return expectedErr means at least meet one file
	expectedErr := errors.New("Stop Iter")
	walkErr := s.WalkDir(ctx, &storage.WalkOption{ListCount: 1}, func(string, int64) error {
		// return an error when meet the first regular file to break the walk loop
		return expectedErr
	})
	if !errors.ErrorEqual(walkErr, expectedErr) {
		if walkErr == nil {
			return common.ErrEmptySourceDir.GenWithStackByArgs(taskCfg.Mydumper.SourceDir)
		}
		return common.NormalizeOrWrapErr(common.ErrStorageUnknown, walkErr)
	}

	loadTask := log.L().Begin(zap.InfoLevel, "load data source")
	var mdl *mydump.MDLoader
	mdl, err = mydump.NewMyDumpLoaderWithStore(ctx, taskCfg, s)
	loadTask.End(zap.ErrorLevel, err)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkSystemRequirement(taskCfg, mdl.GetDatabases())
	if err != nil {
		log.L().Error("check system requirements failed", zap.Error(err))
		return common.ErrSystemRequirementNotMet.Wrap(err).GenWithStackByArgs()
	}
	// check table schema conflicts
	err = checkSchemaConflict(taskCfg, mdl.GetDatabases())
	if err != nil {
		log.L().Error("checkpoint schema conflicts with data files", zap.Error(err))
		return errors.Trace(err)
	}

	dbMetas := mdl.GetDatabases()
	web.BroadcastInitProgress(dbMetas)

	var procedure *restore.Controller

	procedure, err = restore.NewRestoreController(ctx, dbMetas, taskCfg, &l.status, s, g)
	if err != nil {
		log.L().Error("restore failed", log.ShortError(err))
		return errors.Trace(err)
	}
	defer procedure.Close()

	err = procedure.Run(ctx)
	return errors.Trace(err)
}

func (l *Lightning) Stop() {
	l.cancelLock.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	l.cancelLock.Unlock()
	if err := l.server.Shutdown(l.ctx); err != nil {
		log.L().Warn("failed to shutdown HTTP server", log.ShortError(err))
	}
	l.shutdown()
}

// Status return the sum size of file which has been imported to TiKV and the total size of source file.
func (l *Lightning) Status() (finished int64, total int64) {
	finished = l.status.FinishedFileSize.Load()
	total = l.status.TotalFileSize.Load()
	return
}

func writeJSONError(w http.ResponseWriter, code int, prefix string, err error) {
	type errorResponse struct {
		Error string `json:"error"`
	}

	w.WriteHeader(code)

	if err != nil {
		prefix += ": " + err.Error()
	}
	_ = json.NewEncoder(w).Encode(errorResponse{Error: prefix})
}

func parseTaskID(req *http.Request) (int64, string, error) {
	path := strings.TrimPrefix(req.URL.Path, "/")
	taskIDString := path
	verb := ""
	if i := strings.IndexByte(path, '/'); i >= 0 {
		taskIDString = path[:i]
		verb = path[i+1:]
	}

	taskID, err := strconv.ParseInt(taskIDString, 10, 64)
	if err != nil {
		return 0, "", err
	}

	return taskID, verb, nil
}

func (l *Lightning) handleTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodGet:
		taskID, _, err := parseTaskID(req)
		// golint tells us to refactor this with switch stmt.
		// However switch stmt doesn't support init-statements,
		// hence if we follow it things might be worse.
		// Anyway, this chain of if-else isn't unacceptable.
		//nolint:gocritic
		if e, ok := err.(*strconv.NumError); ok && e.Num == "" {
			l.handleGetTask(w)
		} else if err == nil {
			l.handleGetOneTask(w, req, taskID)
		} else {
			writeJSONError(w, http.StatusBadRequest, "invalid task ID", err)
		}
	case http.MethodPost:
		l.handlePostTask(w, req)
	case http.MethodDelete:
		l.handleDeleteOneTask(w, req)
	case http.MethodPatch:
		l.handlePatchOneTask(w, req)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost+", "+http.MethodDelete+", "+http.MethodPatch)
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET, POST, DELETE and PATCH are allowed", nil)
	}
}

func (l *Lightning) handleGetTask(w http.ResponseWriter) {
	var response struct {
		Current   *int64  `json:"current"`
		QueuedIDs []int64 `json:"queue"`
	}
	l.serverLock.Lock()
	if l.taskCfgs != nil {
		response.QueuedIDs = l.taskCfgs.AllIDs()
	} else {
		response.QueuedIDs = []int64{}
	}
	l.serverLock.Unlock()

	l.cancelLock.Lock()
	if l.cancel != nil && l.curTask != nil {
		response.Current = new(int64)
		*response.Current = l.curTask.TaskID
	}
	l.cancelLock.Unlock()

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

func (l *Lightning) handleGetOneTask(w http.ResponseWriter, req *http.Request, taskID int64) {
	var task *config.Config

	l.cancelLock.Lock()
	if l.curTask != nil && l.curTask.TaskID == taskID {
		task = l.curTask
	}
	l.cancelLock.Unlock()

	if task == nil && l.taskCfgs != nil {
		task, _ = l.taskCfgs.Get(taskID)
	}

	if task == nil {
		writeJSONError(w, http.StatusNotFound, "task ID not found", nil)
		return
	}

	json, err := json.Marshal(task)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to serialize task", err)
		return
	}

	writeBytesCompressed(w, req, json)
}

func (l *Lightning) handlePostTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	l.serverLock.Lock()
	defer l.serverLock.Unlock()
	if l.taskCfgs == nil {
		// l.taskCfgs is non-nil only if Lightning is started with RunServer().
		// Without the server mode this pointer is default to be nil.
		writeJSONError(w, http.StatusNotImplemented, "server-mode not enabled", nil)
		return
	}

	type taskResponse struct {
		ID int64 `json:"id"`
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "cannot read request", err)
		return
	}
	log.L().Debug("received task config", zap.ByteString("content", data))

	cfg := config.NewConfig()
	if err = cfg.LoadFromGlobal(l.globalCfg); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "cannot restore from global config", err)
		return
	}
	if err = cfg.LoadFromTOML(data); err != nil {
		writeJSONError(w, http.StatusBadRequest, "cannot parse task (must be TOML)", err)
		return
	}
	if err = cfg.Adjust(l.ctx); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid task configuration", err)
		return
	}

	l.taskCfgs.Push(cfg)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(taskResponse{ID: cfg.TaskID})
}

func (l *Lightning) handleDeleteOneTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	taskID, _, err := parseTaskID(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid task ID", err)
		return
	}

	var cancel context.CancelFunc
	cancelSuccess := false

	l.cancelLock.Lock()
	if l.cancel != nil && l.curTask != nil && l.curTask.TaskID == taskID {
		cancel = l.cancel
		l.cancel = nil
	}
	l.cancelLock.Unlock()

	if cancel != nil {
		cancel()
		cancelSuccess = true
	} else if l.taskCfgs != nil {
		cancelSuccess = l.taskCfgs.Remove(taskID)
	}

	log.L().Info("canceled task", zap.Int64("taskID", taskID), zap.Bool("success", cancelSuccess))

	if cancelSuccess {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	} else {
		writeJSONError(w, http.StatusNotFound, "task ID not found", nil)
	}
}

func (l *Lightning) handlePatchOneTask(w http.ResponseWriter, req *http.Request) {
	if l.taskCfgs == nil {
		writeJSONError(w, http.StatusNotImplemented, "server-mode not enabled", nil)
		return
	}

	taskID, verb, err := parseTaskID(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid task ID", err)
		return
	}

	moveSuccess := false
	switch verb {
	case "front":
		moveSuccess = l.taskCfgs.MoveToFront(taskID)
	case "back":
		moveSuccess = l.taskCfgs.MoveToBack(taskID)
	default:
		writeJSONError(w, http.StatusBadRequest, "unknown patch action", nil)
		return
	}

	if moveSuccess {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	} else {
		writeJSONError(w, http.StatusNotFound, "task ID not found", nil)
	}
}

func writeBytesCompressed(w http.ResponseWriter, req *http.Request, b []byte) {
	if !strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		_, _ = w.Write(b)
		return
	}

	w.Header().Set("Content-Encoding", "gzip")
	w.WriteHeader(http.StatusOK)
	gw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
	_, _ = gw.Write(b)
	_ = gw.Close()
}

func handleProgressTask(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	res, err := web.MarshalTaskProgress()
	if err == nil {
		writeBytesCompressed(w, req, res)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(err.Error())
	}
}

func handleProgressTable(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tableName := req.URL.Query().Get("t")
	res, err := web.MarshalTableCheckpoints(tableName)
	if err == nil {
		writeBytesCompressed(w, req, res)
	} else {
		if errors.IsNotFound(err) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(err.Error())
	}
}

func handlePause(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodGet:
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"paused":%v}`, restore.DeliverPauser.IsPaused())

	case http.MethodPut:
		w.WriteHeader(http.StatusOK)
		restore.DeliverPauser.Pause()
		log.L().Info("progress paused")
		_, _ = w.Write([]byte("{}"))

	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut)
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET and PUT are allowed", nil)
	}
}

func handleResume(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodPut:
		w.WriteHeader(http.StatusOK)
		restore.DeliverPauser.Resume()
		log.L().Info("progress resumed")
		_, _ = w.Write([]byte("{}"))

	default:
		w.Header().Set("Allow", http.MethodPut)
		writeJSONError(w, http.StatusMethodNotAllowed, "only PUT is allowed", nil)
	}
}

func handleLogLevel(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var logLevel struct {
		Level zapcore.Level `json:"level"`
	}

	switch req.Method {
	case http.MethodGet:
		logLevel.Level = log.Level()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logLevel)

	case http.MethodPut, http.MethodPost:
		if err := json.NewDecoder(req.Body).Decode(&logLevel); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid log level", err)
			return
		}
		oldLevel := log.SetLevel(zapcore.InfoLevel)
		log.L().Info("changed log level", zap.Stringer("old", oldLevel), zap.Stringer("new", logLevel.Level))
		log.SetLevel(logLevel.Level)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))

	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut+", "+http.MethodPost)
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET, PUT and POST are allowed", nil)
	}
}

func checkSystemRequirement(cfg *config.Config, dbsMeta []*mydump.MDDatabaseMeta) error {
	// in local mode, we need to read&write a lot of L0 sst files, so we need to check system max open files limit
	if cfg.TikvImporter.Backend == config.BackendLocal {
		// estimate max open files = {top N(TableConcurrency) table sizes} / {MemoryTableSize}
		tableTotalSizes := make([]int64, 0)
		for _, dbs := range dbsMeta {
			for _, tb := range dbs.Tables {
				tableTotalSizes = append(tableTotalSizes, tb.TotalSize)
			}
		}
		sort.Slice(tableTotalSizes, func(i, j int) bool {
			return tableTotalSizes[i] > tableTotalSizes[j]
		})
		topNTotalSize := int64(0)
		for i := 0; i < len(tableTotalSizes) && i < cfg.App.TableConcurrency; i++ {
			topNTotalSize += tableTotalSizes[i]
		}

		// region-concurrency: number of LocalWriters writing SST files.
		// 2*totalSize/memCacheSize: number of Pebble MemCache files.
		maxDBFiles := topNTotalSize / int64(cfg.TikvImporter.LocalWriterMemCacheSize) * 2
		// the pebble db and all import routine need upto maxDBFiles fds for read and write.
		maxOpenDBFiles := maxDBFiles * (1 + int64(cfg.TikvImporter.RangeConcurrency))
		estimateMaxFiles := local.Rlim_t(cfg.App.RegionConcurrency) + local.Rlim_t(maxOpenDBFiles)
		if err := local.VerifyRLimit(estimateMaxFiles); err != nil {
			return err
		}
	}

	return nil
}

// checkSchemaConflict return error if checkpoint table scheme is conflict with data files
func checkSchemaConflict(cfg *config.Config, dbsMeta []*mydump.MDDatabaseMeta) error {
	if cfg.Checkpoint.Enable && cfg.Checkpoint.Driver == config.CheckpointDriverMySQL {
		for _, db := range dbsMeta {
			if db.Name == cfg.Checkpoint.Schema {
				for _, tb := range db.Tables {
					if checkpoints.IsCheckpointTable(tb.Name) {
						return common.ErrCheckpointSchemaConflict.GenWithStack("checkpoint table `%s`.`%s` conflict with data files. Please change the `checkpoint.schema` config or set `checkpoint.driver` to \"file\" instead", db.Name, tb.Name)
					}
				}
			}
		}
	}
	return nil
}
func CheckpointRemove(ctx context.Context, cfg *config.Config, tableName string) error {
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	// try to remove the metadata first.
	taskCp, err := cpdb.TaskCheckpoint(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// a empty id means this task is not inited, we needn't further check metas.
	if taskCp != nil && taskCp.TaskID != 0 {
		// try to clean up table metas if exists
		if err = CleanupMetas(ctx, cfg, tableName); err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(cpdb.RemoveCheckpoint(ctx, tableName))
}

func CleanupMetas(ctx context.Context, cfg *config.Config, tableName string) error {
	if tableName == "all" {
		tableName = ""
	}
	// try to clean up table metas if exists
	db, err := restore.DBFromConfig(ctx, cfg.TiDB)
	if err != nil {
		return errors.Trace(err)
	}

	tableMetaExist, err := common.TableExists(ctx, db, cfg.App.MetaSchemaName, restore.TableMetaTableName)
	if err != nil {
		return errors.Trace(err)
	}
	if tableMetaExist {
		metaTableName := common.UniqueTable(cfg.App.MetaSchemaName, restore.TableMetaTableName)
		if err = restore.RemoveTableMetaByTableName(ctx, db, metaTableName, tableName); err != nil {
			return errors.Trace(err)
		}
	}

	exist, err := common.TableExists(ctx, db, cfg.App.MetaSchemaName, restore.TaskMetaTableName)
	if err != nil || !exist {
		return errors.Trace(err)
	}
	return errors.Trace(restore.MaybeCleanupAllMetas(ctx, db, cfg.App.MetaSchemaName, tableMetaExist))
}

func UnsafeCloseEngine(ctx context.Context, importer backend.Backend, engine string) (*backend.ClosedEngine, error) {
	if index := strings.LastIndexByte(engine, ':'); index >= 0 {
		tableName := engine[:index]
		engineID, err := strconv.Atoi(engine[index+1:])
		if err != nil {
			return nil, errors.Trace(err)
		}
		ce, err := importer.UnsafeCloseEngine(ctx, nil, tableName, int32(engineID)) // #nosec G109
		return ce, errors.Trace(err)
	}

	engineUUID, err := uuid.Parse(engine)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ce, err := importer.UnsafeCloseEngineWithUUID(ctx, nil, "<tidb-lightning-ctl>", engineUUID)
	return ce, errors.Trace(err)
}

func CleanupEngine(ctx context.Context, cfg *config.Config, tls *common.TLS, engine string) error {
	importer, err := importer.NewImporter(ctx, tls, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
	if err != nil {
		return errors.Trace(err)
	}

	ce, err := UnsafeCloseEngine(ctx, importer, engine)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(ce.Cleanup(ctx))
}

func SwitchMode(ctx context.Context, cfg *config.Config, tls *common.TLS, mode string) error {
	var m import_sstpb.SwitchMode
	switch mode {
	case config.ImportMode:
		m = import_sstpb.SwitchMode_Import
	case config.NormalMode:
		m = import_sstpb.SwitchMode_Normal
	default:
		return errors.Errorf("invalid mode %s, must use %s or %s", mode, config.ImportMode, config.NormalMode)
	}

	return tikv.ForAllStores(
		ctx,
		tls.WithHost(cfg.TiDB.PdAddr),
		tikv.StoreStateDisconnected,
		func(c context.Context, store *tikv.Store) error {
			return tikv.SwitchMode(c, tls, store.Address, m)
		},
	)
}
