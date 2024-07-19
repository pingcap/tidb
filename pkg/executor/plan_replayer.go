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

package executor

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"go.uber.org/zap"
)

var _ exec.Executor = &PlanReplayerExec{}
var _ exec.Executor = &PlanReplayerLoadExec{}

// PlanReplayerExec represents a plan replayer executor.
type PlanReplayerExec struct {
	exec.BaseExecutor
	CaptureInfo *PlanReplayerCaptureInfo
	DumpInfo    *PlanReplayerDumpInfo
	endFlag     bool
}

// PlanReplayerCaptureInfo indicates capture info
type PlanReplayerCaptureInfo struct {
	SQLDigest  string
	PlanDigest string
	Remove     bool
}

// PlanReplayerDumpInfo indicates dump info
type PlanReplayerDumpInfo struct {
	ExecStmts         []ast.StmtNode
	Analyze           bool
	HistoricalStatsTS uint64
	StartTS           uint64
	Path              string
	File              *os.File
	FileName          string
	ctx               sessionctx.Context
}

// Next implements the Executor Next interface.
func (e *PlanReplayerExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.endFlag {
		return nil
	}
	if e.CaptureInfo != nil {
		if e.CaptureInfo.Remove {
			return e.removeCaptureTask(ctx)
		}
		return e.registerCaptureTask(ctx)
	}
	err := e.createFile()
	if err != nil {
		return err
	}
	// Note:
	// For the dumping for SQL file case (len(e.DumpInfo.Path) > 0), the DumpInfo.dump() is called in
	// handleFileTransInConn(), which is after TxnManager.OnTxnEnd(), where we can't access the TxnManager anymore.
	// So we must fetch the startTS now.
	startTS, err := sessiontxn.GetTxnManager(e.Ctx()).GetStmtReadTS()
	if err != nil {
		return err
	}
	e.DumpInfo.StartTS = startTS
	if len(e.DumpInfo.Path) > 0 {
		err = e.prepare()
		if err != nil {
			return err
		}
		// As we can only read file from handleSpecialQuery, thus we store the file token in the session var during `dump`
		// and return nil here.
		e.endFlag = true
		return nil
	}
	if e.DumpInfo.ExecStmts == nil {
		return errors.New("plan replayer: sql is empty")
	}
	err = e.DumpInfo.dump(ctx)
	if err != nil {
		return err
	}
	req.AppendString(0, e.DumpInfo.FileName)
	e.endFlag = true
	return nil
}

func (e *PlanReplayerExec) removeCaptureTask(ctx context.Context) error {
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	exec := e.Ctx().GetRestrictedSQLExecutor()
	_, _, err := exec.ExecRestrictedSQL(ctx1, nil, fmt.Sprintf("delete from mysql.plan_replayer_task where sql_digest = '%s' and plan_digest = '%s'",
		e.CaptureInfo.SQLDigest, e.CaptureInfo.PlanDigest))
	if err != nil {
		logutil.BgLogger().Warn("remove mysql.plan_replayer_status record failed",
			zap.Error(err))
		return err
	}
	err = domain.GetDomain(e.Ctx()).GetPlanReplayerHandle().CollectPlanReplayerTask()
	if err != nil {
		logutil.BgLogger().Warn("collect task failed", zap.Error(err))
	}
	logutil.BgLogger().Info("collect plan replayer task success")
	e.endFlag = true
	return nil
}

func (e *PlanReplayerExec) registerCaptureTask(ctx context.Context) error {
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	exists, err := domain.CheckPlanReplayerTaskExists(ctx1, e.Ctx(), e.CaptureInfo.SQLDigest, e.CaptureInfo.PlanDigest)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("plan replayer capture task already exists")
	}
	exec := e.Ctx().GetRestrictedSQLExecutor()
	_, _, err = exec.ExecRestrictedSQL(ctx1, nil, fmt.Sprintf("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('%s','%s')",
		e.CaptureInfo.SQLDigest, e.CaptureInfo.PlanDigest))
	if err != nil {
		logutil.BgLogger().Warn("insert mysql.plan_replayer_status record failed",
			zap.Error(err))
		return err
	}
	err = domain.GetDomain(e.Ctx()).GetPlanReplayerHandle().CollectPlanReplayerTask()
	if err != nil {
		logutil.BgLogger().Warn("collect task failed", zap.Error(err))
	}
	logutil.BgLogger().Info("collect plan replayer task success")
	e.endFlag = true
	return nil
}

func (e *PlanReplayerExec) createFile() error {
	var err error
	e.DumpInfo.File, e.DumpInfo.FileName, err = replayer.GeneratePlanReplayerFile(false, false, false)
	if err != nil {
		return err
	}
	return nil
}

func (e *PlanReplayerDumpInfo) dump(ctx context.Context) (err error) {
	fileName := e.FileName
	zf := e.File
	task := &domain.PlanReplayerDumpTask{
		StartTS:           e.StartTS,
		FileName:          fileName,
		Zf:                zf,
		SessionVars:       e.ctx.GetSessionVars(),
		TblStats:          nil,
		ExecStmts:         e.ExecStmts,
		Analyze:           e.Analyze,
		HistoricalStatsTS: e.HistoricalStatsTS,
	}
	err = domain.DumpPlanReplayerInfo(ctx, e.ctx, task)
	if err != nil {
		return err
	}
	e.ctx.GetSessionVars().LastPlanReplayerToken = e.FileName
	return nil
}

func (e *PlanReplayerExec) prepare() error {
	val := e.Ctx().Value(PlanReplayerDumpVarKey)
	if val != nil {
		e.Ctx().SetValue(PlanReplayerDumpVarKey, nil)
		return errors.New("plan replayer: previous plan replayer dump option isn't closed normally, please try again")
	}
	e.Ctx().SetValue(PlanReplayerDumpVarKey, e.DumpInfo)
	return nil
}

// DumpSQLsFromFile dumps plan replayer results for sqls from file
func (e *PlanReplayerDumpInfo) DumpSQLsFromFile(ctx context.Context, b []byte) error {
	sqls := strings.Split(string(b), ";")
	e.ExecStmts = make([]ast.StmtNode, 0)
	for _, sql := range sqls {
		s := strings.Trim(sql, "\n")
		if len(s) < 1 {
			continue
		}
		node, err := e.ctx.GetRestrictedSQLExecutor().ParseWithParams(ctx, s)
		if err != nil {
			return fmt.Errorf("parse sql error, sql:%v, err:%v", s, err)
		}
		e.ExecStmts = append(e.ExecStmts, node)
	}
	return e.dump(ctx)
}

// PlanReplayerLoadExec represents a plan replayer load executor.
type PlanReplayerLoadExec struct {
	exec.BaseExecutor
	info *PlanReplayerLoadInfo
}

// PlanReplayerLoadInfo contains file path and session context.
type PlanReplayerLoadInfo struct {
	Path string
	Ctx  sessionctx.Context
}

type planReplayerDumpKeyType int

func (planReplayerDumpKeyType) String() string {
	return "plan_replayer_dump_var"
}

type planReplayerLoadKeyType int

func (planReplayerLoadKeyType) String() string {
	return "plan_replayer_load_var"
}

// PlanReplayerLoadVarKey is a variable key for plan replayer load.
const PlanReplayerLoadVarKey planReplayerLoadKeyType = 0

// PlanReplayerDumpVarKey is a variable key for plan replayer dump.
const PlanReplayerDumpVarKey planReplayerDumpKeyType = 1

// Next implements the Executor Next interface.
func (e *PlanReplayerLoadExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if len(e.info.Path) == 0 {
		return errors.New("plan replayer: file path is empty")
	}
	val := e.Ctx().Value(PlanReplayerLoadVarKey)
	if val != nil {
		e.Ctx().SetValue(PlanReplayerLoadVarKey, nil)
		return errors.New("plan replayer: previous plan replayer load option isn't closed normally, please try again")
	}
	e.Ctx().SetValue(PlanReplayerLoadVarKey, e.info)
	return nil
}

func loadSetTiFlashReplica(ctx sessionctx.Context, z *zip.Reader) error {
	for _, zipFile := range z.File {
		if strings.Compare(zipFile.Name, domain.PlanReplayerTiFlashReplicasFile) == 0 {
			v, err := zipFile.Open()
			if err != nil {
				return errors.AddStack(err)
			}
			//nolint: errcheck,all_revive,revive
			defer v.Close()
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(v)
			if err != nil {
				return errors.AddStack(err)
			}
			rows := strings.Split(buf.String(), "\n")
			for _, row := range rows {
				if len(row) < 1 {
					continue
				}
				r := strings.Split(row, "\t")
				if len(r) < 3 {
					logutil.BgLogger().Debug("plan replayer: skip error",
						zap.Error(errors.New("setting tiflash replicas failed")))
					continue
				}
				dbName := r[0]
				tableName := r[1]
				c := context.Background()
				// Though we record tiflash replica in txt, we only set 1 tiflash replica as it's enough for reproduce the plan
				sql := fmt.Sprintf("alter table %s.%s set tiflash replica 1", dbName, tableName)
				_, err = ctx.GetSQLExecutor().Execute(c, sql)
				logutil.BgLogger().Debug("plan replayer: skip error", zap.Error(err))
			}
		}
	}
	return nil
}

func loadAllBindings(ctx sessionctx.Context, z *zip.Reader) error {
	for _, f := range z.File {
		if strings.Compare(f.Name, domain.PlanReplayerSessionBindingFile) == 0 {
			err := loadBindings(ctx, f, true)
			if err != nil {
				return err
			}
		} else if strings.Compare(f.Name, domain.PlanReplayerGlobalBindingFile) == 0 {
			err := loadBindings(ctx, f, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func loadBindings(ctx sessionctx.Context, f *zip.File, isSession bool) error {
	r, err := f.Open()
	if err != nil {
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return errors.AddStack(err)
	}
	if len(buf.String()) < 1 {
		return nil
	}
	bindings := strings.Split(buf.String(), "\n")
	for _, binding := range bindings {
		cols := strings.Split(binding, "\t")
		if len(cols) < 3 {
			continue
		}
		originSQL := cols[0]
		bindingSQL := cols[1]
		enabled := cols[3]
		newNormalizedSQL := parser.NormalizeForBinding(originSQL, true)
		if strings.Compare(enabled, "enabled") == 0 {
			sql := fmt.Sprintf("CREATE %s BINDING FOR %s USING %s", func() string {
				if isSession {
					return "SESSION"
				}
				return "GLOBAL"
			}(), newNormalizedSQL, bindingSQL)
			c := context.Background()
			_, err = ctx.GetSQLExecutor().Execute(c, sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func loadVariables(ctx sessionctx.Context, z *zip.Reader) error {
	unLoadVars := make([]string, 0)
	for _, zipFile := range z.File {
		if strings.Compare(zipFile.Name, domain.PlanReplayerVariablesFile) == 0 {
			varMap := make(map[string]string)
			v, err := zipFile.Open()
			if err != nil {
				return errors.AddStack(err)
			}
			//nolint: errcheck,all_revive,revive
			defer v.Close()
			_, err = toml.NewDecoder(v).Decode(&varMap)
			if err != nil {
				return errors.AddStack(err)
			}
			vars := ctx.GetSessionVars()
			for name, value := range varMap {
				sysVar := variable.GetSysVar(name)
				if sysVar == nil {
					unLoadVars = append(unLoadVars, name)
					logutil.BgLogger().Warn(fmt.Sprintf("skip set variable %s:%s", name, value), zap.Error(err))
					continue
				}
				sVal, err := sysVar.Validate(vars, value, variable.ScopeSession)
				if err != nil {
					unLoadVars = append(unLoadVars, name)
					logutil.BgLogger().Warn(fmt.Sprintf("skip variable %s:%s", name, value), zap.Error(err))
					continue
				}
				err = vars.SetSystemVar(name, sVal)
				if err != nil {
					unLoadVars = append(unLoadVars, name)
					logutil.BgLogger().Warn(fmt.Sprintf("skip set variable %s:%s", name, value), zap.Error(err))
					continue
				}
			}
		}
	}
	if len(unLoadVars) > 0 {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("variables set failed:%s", strings.Join(unLoadVars, ",")))
	}
	return nil
}

// createSchemaAndItems creates schema and tables or views
func createSchemaAndItems(ctx sessionctx.Context, f *zip.File) error {
	r, err := f.Open()
	if err != nil {
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return errors.AddStack(err)
	}
	originText := buf.String()
	index1 := strings.Index(originText, ";")
	createDatabaseSQL := originText[:index1+1]
	index2 := strings.Index(originText[index1+1:], ";")
	useDatabaseSQL := originText[index1+1:][:index2+1]
	createTableSQL := originText[index1+1:][index2+1:]
	c := context.Background()
	// create database if not exists
	_, err = ctx.GetSQLExecutor().Execute(c, createDatabaseSQL)
	logutil.BgLogger().Debug("plan replayer: skip error", zap.Error(err))
	// use database
	_, err = ctx.GetSQLExecutor().Execute(c, useDatabaseSQL)
	if err != nil {
		return err
	}
	// create table or view
	_, err = ctx.GetSQLExecutor().Execute(c, createTableSQL)
	if err != nil {
		return err
	}
	return nil
}

func loadStats(ctx sessionctx.Context, f *zip.File) error {
	jsonTbl := &util.JSONTable{}
	r, err := f.Open()
	if err != nil {
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return errors.AddStack(err)
	}
	if err := json.Unmarshal(buf.Bytes(), jsonTbl); err != nil {
		return errors.AddStack(err)
	}
	do := domain.GetDomain(ctx)
	h := do.StatsHandle()
	if h == nil {
		return errors.New("plan replayer: hanlde is nil")
	}
	return h.LoadStatsFromJSON(context.Background(), ctx.GetInfoSchema().(infoschema.InfoSchema), jsonTbl, 0)
}

// Update updates the data of the corresponding table.
func (e *PlanReplayerLoadInfo) Update(data []byte) error {
	b := bytes.NewReader(data)
	z, err := zip.NewReader(b, int64(len(data)))
	if err != nil {
		return errors.AddStack(err)
	}

	// load variable
	err = loadVariables(e.Ctx, z)
	if err != nil {
		return err
	}

	// build schema and table first
	for _, zipFile := range z.File {
		if zipFile.Name == fmt.Sprintf("schema/%v", domain.PlanReplayerSchemaMetaFile) {
			continue
		}
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "schema") == 0 && zipFile.Mode().IsRegular() {
			err = createSchemaAndItems(e.Ctx, zipFile)
			if err != nil {
				return err
			}
		}
	}

	// set tiflash replica if exists
	err = loadSetTiFlashReplica(e.Ctx, z)
	if err != nil {
		return err
	}

	// build view next
	for _, zipFile := range z.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "view") == 0 && zipFile.Mode().IsRegular() {
			err = createSchemaAndItems(e.Ctx, zipFile)
			if err != nil {
				return err
			}
		}
	}

	// load stats
	for _, zipFile := range z.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "stats") == 0 && zipFile.Mode().IsRegular() {
			err = loadStats(e.Ctx, zipFile)
			if err != nil {
				return err
			}
		}
	}

	err = loadAllBindings(e.Ctx, z)
	if err != nil {
		logutil.BgLogger().Warn("load bindings failed", zap.Error(err))
		e.Ctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("load bindings failed, err:%v", err))
	}
	return nil
}
