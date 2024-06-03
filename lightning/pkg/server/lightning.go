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

package server

import (
	"cmp"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/lightning/pkg/importer"
	"github.com/pingcap/tidb/lightning/pkg/web"
	_ "github.com/pingcap/tidb/pkg/expression" // get rid of `import cycle`: just init expression.RewriteAstExpr,and called at package `backend.kv`.
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/tikv"
	_ "github.com/pingcap/tidb/pkg/planner/core" // init expression.EvalSimpleAst related function
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shurcooL/httpgzip"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Lightning is the main struct of the lightning package.
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
	status     importer.LightningStatus

	promFactory  promutil.Factory
	promRegistry promutil.Registry
	metrics      *metric.Metrics

	cancelLock sync.Mutex
	curTask    *config.Config
	cancel     context.CancelFunc // for per task context, which maybe different from lightning context

	taskCanceled bool
}

func initEnv(cfg *config.GlobalConfig) error {
	if cfg.App.Config.File == "" {
		return nil
	}
	return log.InitLogger(&cfg.App.Config, cfg.TiDB.LogLevel)
}

// New creates a new Lightning instance.
func New(globalCfg *config.GlobalConfig) *Lightning {
	if err := initEnv(globalCfg); err != nil {
		fmt.Println("Failed to initialize environment:", err)
		os.Exit(1)
	}

	tls, err := common.NewTLS(
		globalCfg.Security.CAPath,
		globalCfg.Security.CertPath,
		globalCfg.Security.KeyPath,
		globalCfg.App.StatusAddr,
		globalCfg.Security.CABytes,
		globalCfg.Security.CertBytes,
		globalCfg.Security.KeyBytes,
	)
	if err != nil {
		log.L().Fatal("failed to load TLS certificates", zap.Error(err))
	}

	redact.InitRedact(globalCfg.Security.RedactInfoLog)

	promFactory := promutil.NewDefaultFactory()
	promRegistry := promutil.NewDefaultRegistry()
	ctx, shutdown := context.WithCancel(context.Background())
	return &Lightning{
		globalCfg:    globalCfg,
		globalTLS:    tls,
		ctx:          ctx,
		shutdown:     shutdown,
		promFactory:  promFactory,
		promRegistry: promRegistry,
	}
}

// GoServe starts the HTTP server in a goroutine. The server will be closed
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

// TODO: maybe handle http request using gin
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	body       string
}

func newLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

// WriteHeader implements http.ResponseWriter.
func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// Write implements http.ResponseWriter.
func (lrw *loggingResponseWriter) Write(d []byte) (int, error) {
	// keep first part of the response for logging, max 1K
	if lrw.body == "" && len(d) > 0 {
		length := len(d)
		if length > 1024 {
			length = 1024
		}
		lrw.body = string(d[:length])
	}
	return lrw.ResponseWriter.Write(d)
}

func httpHandleWrapper(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger := log.L().With(zap.String("method", r.Method), zap.Stringer("url", r.URL)).
			Begin(zapcore.InfoLevel, "process http request")

		newWriter := newLoggingResponseWriter(w)
		h.ServeHTTP(newWriter, r)

		bodyField := zap.Skip()
		if newWriter.Header().Get("Content-Encoding") != "gzip" {
			bodyField = zap.String("body", newWriter.body)
		}
		logger.End(zapcore.InfoLevel, nil, zap.Int("status", newWriter.statusCode), bodyField)
	}
}

func (l *Lightning) goServe(statusAddr string, realAddrWriter io.Writer) error {
	mux := http.NewServeMux()
	mux.Handle("/", http.RedirectHandler("/web/", http.StatusFound))

	registry := l.promRegistry
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector())
	if gatherer, ok := registry.(prometheus.Gatherer); ok {
		handler := promhttp.InstrumentMetricHandler(
			registry, promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}),
		)
		mux.Handle("/metrics", handler)
	}

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Enable failpoint http API for testing.
	failpoint.Inject("EnableTestAPI", func() {
		mux.HandleFunc("/fail/", func(w http.ResponseWriter, r *http.Request) {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/fail")
			new(failpoint.HttpHandler).ServeHTTP(w, r)
		})
	})

	handleTasks := http.StripPrefix("/tasks", http.HandlerFunc(l.handleTask))
	mux.Handle("/tasks", httpHandleWrapper(handleTasks.ServeHTTP))
	mux.Handle("/tasks/", httpHandleWrapper(handleTasks.ServeHTTP))
	mux.HandleFunc("/progress/task", httpHandleWrapper(handleProgressTask))
	mux.HandleFunc("/progress/table", httpHandleWrapper(handleProgressTable))
	mux.HandleFunc("/pause", httpHandleWrapper(handlePause))
	mux.HandleFunc("/resume", httpHandleWrapper(handleResume))
	mux.HandleFunc("/loglevel", httpHandleWrapper(handleLogLevel))

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

// RunServer is used by binary lightning to start a HTTP server to receive import tasks.
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
		o := &options{
			promFactory:  l.promFactory,
			promRegistry: l.promRegistry,
			logger:       log.L(),
		}
		err = l.run(context.Background(), task, o)
		if err != nil && !common.IsContextCanceledError(err) {
			importer.DeliverPauser.Pause() // force pause the progress on error
			log.L().Error("tidb lightning encountered error", zap.Error(err))
		}
	}
}

// RunOnceWithOptions is used by binary lightning and host when using lightning as a library.
//   - for binary lightning, taskCtx could be context.Background which means taskCtx wouldn't be canceled directly by its
//     cancel function, but only by Lightning.Stop or HTTP DELETE using l.cancel. No need to set Options
//   - for lightning as a library, taskCtx could be a meaningful context that get canceled outside, and there Options may
//     be used:
//   - WithGlue: set a caller implemented glue. Otherwise, lightning will use a default glue later.
//   - WithDumpFileStorage: caller has opened an external storage for lightning. Otherwise, lightning will open a
//     storage by config
//   - WithCheckpointStorage: caller has opened an external storage for lightning and want to save checkpoint
//     in it. Otherwise, lightning will save checkpoint by the Checkpoint.DSN in config
func (l *Lightning) RunOnceWithOptions(taskCtx context.Context, taskCfg *config.Config, opts ...Option) error {
	o := &options{
		promFactory:  l.promFactory,
		promRegistry: l.promRegistry,
		logger:       log.L(),
	}
	for _, opt := range opts {
		opt(o)
	}

	failpoint.Inject("setExtStorage", func(val failpoint.Value) {
		path := val.(string)
		b, err := storage.ParseBackend(path, nil)
		if err != nil {
			panic(err)
		}
		s, err := storage.New(context.Background(), b, &storage.ExternalStorageOptions{})
		if err != nil {
			panic(err)
		}
		o.dumpFileStorage = s
		o.checkpointStorage = s
	})
	failpoint.Inject("setCheckpointName", func(val failpoint.Value) {
		file := val.(string)
		o.checkpointName = file
	})

	if o.dumpFileStorage != nil {
		// we don't use it, set a value to pass Adjust
		taskCfg.Mydumper.SourceDir = "noop://"
	}

	if err := taskCfg.Adjust(taskCtx); err != nil {
		return err
	}

	taskCfg.TaskID = time.Now().UnixNano()
	failpoint.Inject("SetTaskID", func(val failpoint.Value) {
		taskCfg.TaskID = int64(val.(int))
	})

	failpoint.Inject("SetIOTotalBytes", func(_ failpoint.Value) {
		o.logger.Info("set io total bytes")
		taskCfg.TiDB.IOTotalBytes = atomic.NewUint64(0)
		taskCfg.TiDB.UUID = uuid.New().String()
		go func() {
			for {
				time.Sleep(time.Millisecond * 10)
				log.L().Info("IOTotalBytes", zap.Uint64("IOTotalBytes", taskCfg.TiDB.IOTotalBytes.Load()))
			}
		}()
	})
	if taskCfg.TiDB.IOTotalBytes != nil {
		o.logger.Info("found IO total bytes counter")
		mysql.RegisterDialContext(taskCfg.TiDB.UUID, func(ctx context.Context, addr string) (net.Conn, error) {
			o.logger.Debug("connection with IO bytes counter")
			d := &net.Dialer{}
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			tcpConn := conn.(*net.TCPConn)
			// try https://github.com/go-sql-driver/mysql/blob/bcc459a906419e2890a50fc2c99ea6dd927a88f2/connector.go#L56-L64
			err = tcpConn.SetKeepAlive(true)
			if err != nil {
				o.logger.Warn("set TCP keep alive failed", zap.Error(err))
			}
			return util.NewTCPConnWithIOCounter(tcpConn, taskCfg.TiDB.IOTotalBytes), nil
		})
	}

	return l.run(taskCtx, taskCfg, o)
}

var (
	taskRunNotifyKey   = "taskRunNotifyKey"
	taskCfgRecorderKey = "taskCfgRecorderKey"
)

func getKeyspaceName(db *sql.DB) (string, error) {
	if db == nil {
		return "", nil
	}

	rows, err := db.Query("show config where Type = 'tidb' and name = 'keyspace-name'")
	if err != nil {
		return "", err
	}
	//nolint: errcheck
	defer rows.Close()

	var (
		_type     string
		_instance string
		_name     string
		value     string
	)
	if rows.Next() {
		err = rows.Scan(&_type, &_instance, &_name, &value)
		if err != nil {
			return "", err
		}
	}

	return value, rows.Err()
}

func (l *Lightning) run(taskCtx context.Context, taskCfg *config.Config, o *options) (err error) {
	build.LogInfo(build.Lightning)
	o.logger.Info("cfg", zap.Stringer("cfg", taskCfg))

	logutil.LogEnvVariables()

	if split.WaitRegionOnlineAttemptTimes != taskCfg.TikvImporter.RegionCheckBackoffLimit {
		// it will cause data race if lightning is used as a library, but this is a
		// hidden config so we ignore that case
		split.WaitRegionOnlineAttemptTimes = taskCfg.TikvImporter.RegionCheckBackoffLimit
	}

	metrics := metric.NewMetrics(o.promFactory)
	metrics.RegisterTo(o.promRegistry)
	defer func() {
		metrics.UnregisterFrom(o.promRegistry)
	}()
	l.metrics = metrics

	ctx := metric.WithMetric(taskCtx, metrics)
	ctx = log.NewContext(ctx, o.logger)
	ctx, cancel := context.WithCancel(ctx)
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

	failpoint.Inject("SetCertExpiredSoon", func(val failpoint.Value) {
		rootKeyPath := val.(string)
		rootCaPath := taskCfg.Security.CAPath
		keyPath := taskCfg.Security.KeyPath
		certPath := taskCfg.Security.CertPath
		if err := updateCertExpiry(rootKeyPath, rootCaPath, keyPath, certPath, time.Second*10); err != nil {
			panic(err)
		}
	})

	failpoint.Inject("PrintStatus", func() {
		defer func() {
			finished, total := l.Status()
			o.logger.Warn("PrintStatus Failpoint",
				zap.Int64("finished", finished),
				zap.Int64("total", total),
				zap.Bool("equal", finished == total))
		}()
	})

	if err := taskCfg.TiDB.Security.BuildTLSConfig(); err != nil {
		return common.ErrInvalidTLSConfig.Wrap(err)
	}

	s := o.dumpFileStorage
	if s == nil {
		u, err := storage.ParseBackend(taskCfg.Mydumper.SourceDir, nil)
		if err != nil {
			return common.NormalizeError(err)
		}
		s, err = storage.New(ctx, u, &storage.ExternalStorageOptions{})
		if err != nil {
			return common.NormalizeError(err)
		}
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

	loadTask := o.logger.Begin(zap.InfoLevel, "load data source")
	var mdl *mydump.MDLoader
	mdl, err = mydump.NewLoaderWithStore(ctx, mydump.NewLoaderCfg(taskCfg), s)
	loadTask.End(zap.ErrorLevel, err)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkSystemRequirement(taskCfg, mdl.GetDatabases())
	if err != nil {
		o.logger.Error("check system requirements failed", zap.Error(err))
		return common.ErrSystemRequirementNotMet.Wrap(err).GenWithStackByArgs()
	}
	// check table schema conflicts
	err = checkSchemaConflict(taskCfg, mdl.GetDatabases())
	if err != nil {
		o.logger.Error("checkpoint schema conflicts with data files", zap.Error(err))
		return errors.Trace(err)
	}

	dbMetas := mdl.GetDatabases()
	web.BroadcastInitProgress(dbMetas)

	// db is only not nil in unit test
	db := o.db
	if db == nil {
		// initiation of default db should be after BuildTLSConfig
		db, err = importer.DBFromConfig(ctx, taskCfg.TiDB)
		if err != nil {
			return common.ErrDBConnect.Wrap(err)
		}
	}

	var keyspaceName string
	if taskCfg.TikvImporter.Backend == config.BackendLocal {
		keyspaceName = taskCfg.TikvImporter.KeyspaceName
		if keyspaceName == "" {
			keyspaceName, err = getKeyspaceName(db)
			if err != nil {
				o.logger.Warn("unable to get keyspace name, lightning will use empty keyspace name", zap.Error(err))
			}
		}
		o.logger.Info("acquired keyspace name", zap.String("keyspaceName", keyspaceName))
	}

	param := &importer.ControllerParam{
		DBMetas:           dbMetas,
		Status:            &l.status,
		DumpFileStorage:   s,
		OwnExtStorage:     o.dumpFileStorage == nil,
		DB:                db,
		CheckpointStorage: o.checkpointStorage,
		CheckpointName:    o.checkpointName,
		DupIndicator:      o.dupIndicator,
		KeyspaceName:      keyspaceName,
	}

	var procedure *importer.Controller
	procedure, err = importer.NewImportController(ctx, taskCfg, param)
	if err != nil {
		o.logger.Error("restore failed", log.ShortError(err))
		return errors.Trace(err)
	}

	failpoint.Inject("orphanWriterGoRoutine", func() {
		// don't exit too quickly to expose panic
		defer time.Sleep(time.Second * 10)
	})
	defer procedure.Close()

	err = procedure.Run(ctx)
	return errors.Trace(err)
}

// Stop stops the lightning server.
func (l *Lightning) Stop() {
	l.cancelLock.Lock()
	if l.cancel != nil {
		l.taskCanceled = true
		l.cancel()
	}
	l.cancelLock.Unlock()
	if err := l.server.Shutdown(l.ctx); err != nil {
		log.L().Warn("failed to shutdown HTTP server", log.ShortError(err))
	}
	l.shutdown()
}

// TaskCanceled return whether the current task is canceled.
func (l *Lightning) TaskCanceled() bool {
	l.cancelLock.Lock()
	defer l.cancelLock.Unlock()
	return l.taskCanceled
}

// Status return the sum size of file which has been imported to TiKV and the total size of source file.
func (l *Lightning) Status() (finished int64, total int64) {
	finished = l.status.FinishedFileSize.Load()
	total = l.status.TotalFileSize.Load()
	return
}

// Metrics returns the metrics of lightning.
// it's inited during `run`, so might return nil.
func (l *Lightning) Metrics() *metric.Metrics {
	return l.metrics
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
	log.L().Info("received task config")

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
		fmt.Fprintf(w, `{"paused":%v}`, importer.DeliverPauser.IsPaused())

	case http.MethodPut:
		w.WriteHeader(http.StatusOK)
		importer.DeliverPauser.Pause()
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
		importer.DeliverPauser.Resume()
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
		log.L().Info("changed log level. No effects if task has specified its logger",
			zap.Stringer("old", oldLevel),
			zap.Stringer("new", logLevel.Level))
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
		slices.SortFunc(tableTotalSizes, func(i, j int64) int {
			return cmp.Compare(j, i)
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
		estimateMaxFiles := local.RlimT(cfg.App.RegionConcurrency) + local.RlimT(maxOpenDBFiles)
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

// CheckpointRemove removes the checkpoint of the given table.
func CheckpointRemove(ctx context.Context, cfg *config.Config, tableName string) error {
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
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

// CleanupMetas removes the table metas of the given table.
func CleanupMetas(ctx context.Context, cfg *config.Config, tableName string) error {
	if tableName == "all" {
		tableName = ""
	}
	// try to clean up table metas if exists
	db, err := importer.DBFromConfig(ctx, cfg.TiDB)
	if err != nil {
		return errors.Trace(err)
	}

	tableMetaExist, err := common.TableExists(ctx, db, cfg.App.MetaSchemaName, importer.TableMetaTableName)
	if err != nil {
		return errors.Trace(err)
	}
	if tableMetaExist {
		metaTableName := common.UniqueTable(cfg.App.MetaSchemaName, importer.TableMetaTableName)
		if err = importer.RemoveTableMetaByTableName(ctx, db, metaTableName, tableName); err != nil {
			return errors.Trace(err)
		}
	}

	exist, err := common.TableExists(ctx, db, cfg.App.MetaSchemaName, importer.TaskMetaTableName)
	if err != nil || !exist {
		return errors.Trace(err)
	}
	return errors.Trace(importer.MaybeCleanupAllMetas(ctx, log.L(), db, cfg.App.MetaSchemaName, tableMetaExist))
}

// SwitchMode switches the mode of the TiKV cluster.
func SwitchMode(ctx context.Context, cli pdhttp.Client, tls *tls.Config, mode string, ranges ...*import_sstpb.Range) error {
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
		cli,
		metapb.StoreState_Offline,
		func(c context.Context, store *pdhttp.MetaStore) error {
			return tikv.SwitchMode(c, tls, store.Address, m, ranges...)
		},
	)
}

func updateCertExpiry(rootKeyPath, rootCaPath, keyPath, certPath string, expiry time.Duration) error {
	rootKey, err := parsePrivateKey(rootKeyPath)
	if err != nil {
		return err
	}
	rootCaPem, err := os.ReadFile(rootCaPath)
	if err != nil {
		return err
	}
	rootCaDer, _ := pem.Decode(rootCaPem)
	rootCa, err := x509.ParseCertificate(rootCaDer.Bytes)
	if err != nil {
		return err
	}
	key, err := parsePrivateKey(keyPath)
	if err != nil {
		return err
	}
	certPem, err := os.ReadFile(certPath)
	if err != nil {
		panic(err)
	}
	certDer, _ := pem.Decode(certPem)
	cert, err := x509.ParseCertificate(certDer.Bytes)
	if err != nil {
		return err
	}
	cert.NotBefore = time.Now()
	cert.NotAfter = time.Now().Add(expiry)
	derBytes, err := x509.CreateCertificate(rand.Reader, cert, rootCa, &key.PublicKey, rootKey)
	if err != nil {
		return err
	}
	return os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}), 0o600)
}

func parsePrivateKey(keyPath string) (*ecdsa.PrivateKey, error) {
	keyPemBlock, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	var keyDERBlock *pem.Block
	for {
		keyDERBlock, keyPemBlock = pem.Decode(keyPemBlock)
		if keyDERBlock == nil {
			return nil, errors.New("failed to find PEM block with type ending in \"PRIVATE KEY\"")
		}
		if keyDERBlock.Type == "PRIVATE KEY" || strings.HasSuffix(keyDERBlock.Type, " PRIVATE KEY") {
			break
		}
	}
	return x509.ParseECPrivateKey(keyDERBlock.Bytes)
}
