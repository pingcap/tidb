// Copyright 2023 PingCAP, Inc.
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

package loaddatatest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// if multiple engine sorting concurrently, and one of them failed, it should cancel all other engines.
func (s *mockGCSSuite) createPhysicalImporter(sql string) *importer.TableImporter {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	s.NoError(err)
	ld := stmt.(*ast.LoadDataStmt)
	options := make([]*plannercore.LoadDataOpt, 0, len(ld.Options))
	for _, opt := range ld.Options {
		loadDataOpt := plannercore.LoadDataOpt{Name: opt.Name}
		if opt.Value != nil {
			loadDataOpt.Value, err = expression.RewriteAstExpr(s.tk.Session(), opt.Value, nil, nil, false)
			s.NoError(err)
		}
		options = append(options, &loadDataOpt)
	}
	is := domain.GetDomain(s.tk.Session()).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("load_data"), model.NewCIStr("t"))
	s.NoError(err)
	var ok bool
	ld.Table.DBInfo, ok = is.SchemaByTable(tbl.Meta())
	s.True(ok)
	plan := &plannercore.LoadData{
		FileLocRef:         ld.FileLocRef,
		OnDuplicate:        ld.OnDuplicate,
		Path:               ld.Path,
		Format:             ld.Format,
		Table:              ld.Table,
		Charset:            ld.Charset,
		Columns:            ld.Columns,
		FieldsInfo:         ld.FieldsInfo,
		LinesInfo:          ld.LinesInfo,
		IgnoreLines:        ld.IgnoreLines,
		ColumnAssignments:  ld.ColumnAssignments,
		ColumnsAndUserVars: ld.ColumnsAndUserVars,
		Options:            options,
	}
	importPlan, err := importer.NewPlan(s.tk.Session(), plan, tbl)
	s.NoError(err)
	controller, err := importer.NewLoadDataController(importPlan, tbl, importer.ASTArgsFromPlan(plan))
	s.NoError(err)
	err = controller.InitDataFiles(context.Background())
	s.NoError(err)
	group, groupCtx := errgroup.WithContext(context.Background())
	param := &importer.JobImportParam{
		Job: &asyncloaddata.Job{
			ID: 1,
		},
		Group:    group,
		GroupCtx: groupCtx,
		Done:     make(chan struct{}),
		Progress: asyncloaddata.NewProgress(false),
	}
	ti, err := importer.NewTableImporter(param, controller)
	s.NoError(err)
	return ti
}

func (s *mockGCSSuite) TestCancelOnSortErr() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "cancel-on-sort-err",
			Name:       "t-01.csv",
		},
		Content: []byte(`1,test1,11`),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "cancel-on-sort-err",
			Name:       "t-02.csv",
		},
		Content: []byte(`2,test2,22`),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), c int);")

	// separated into 2 engines
	backup := config.DefaultBatchSize
	config.DefaultBatchSize = 1
	s.T().Cleanup(func() {
		config.DefaultBatchSize = backup
	})

	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://cancel-on-sort-err/t-*.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' with import_mode='physical', thread=2`, gcsEndpoint)
	ti := s.createPhysicalImporter(loadDataSQL)
	defer func() {
		s.NoError(ti.Close())
	}()

	importer.TestImportCancelledOnErr.Store(false)

	// chunk in engine 0 wait
	// chunk in engine 1 fail and should trigger cancel
	// chunk in engine 0 continue, and fail with context canceled, and should not reach AfterProcessChunkSync
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/BeforeProcessChunkSync", `return("t-01.csv")`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/BeforeProcessChunkFail", `return("t-02.csv")`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/AfterProcessChunkSync", `return("t-01.csv")`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/SetImportCancelledOnErr", `return()`)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Eventually(s.T(), func() bool {
			return importer.TestImportCancelledOnErr.Load()
		}, 5*time.Second, time.Millisecond*100)

		importer.TestSyncCh <- struct{}{}
	}()
	ti.Import()
	err := ti.Group.Wait()
	s.Equal(3, len(ti.TableCP.Engines)) // 2 data engines + 1 index engine
	s.ErrorContains(err, "mock process chunk fail")

	wg.Wait()
}

// if ingest of some engine fails, it should cancel all other sorting routines.
func (s *mockGCSSuite) TestCancelOnIngestErr() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cancel-on-ingest-err", Name: "t-01.csv"},
		Content:     []byte(`1,test1,11`),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cancel-on-ingest-err", Name: "t-02.csv"},
		Content:     []byte(`2,test2,22`),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cancel-on-ingest-err", Name: "t-03.csv"},
		Content:     []byte(`3,test3,33`),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), c int);")

	// separated into 3 engines
	backup := config.DefaultBatchSize
	config.DefaultBatchSize = 1
	s.T().Cleanup(func() {
		config.DefaultBatchSize = backup
	})

	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://cancel-on-ingest-err/t-*.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' with import_mode='physical', thread=3`, gcsEndpoint)
	ti := s.createPhysicalImporter(loadDataSQL)
	defer func() {
		s.NoError(ti.Close())
	}()

	importer.TestImportCancelledOnErr.Store(false)

	// engine 0 sort success, but ingest fail. engine 1/2 wait on sort.
	// continue engine 1/2, and fail with context canceled, and should not reach AfterProcessChunkSync
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/BeforeProcessChunkSync", `return("t-02.csv,t-03.csv")`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/AfterProcessChunkSync", `return("t-02.csv,t-03.csv")`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/SetImportCancelledOnErr", `return()`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/IngestAndCleanupError", `return()`)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Eventually(s.T(), func() bool {
			return importer.TestImportCancelledOnErr.Load()
		}, 5*time.Second, time.Millisecond*100)

		// continue engine 1/2
		importer.TestSyncCh <- struct{}{}
		importer.TestSyncCh <- struct{}{}
	}()
	ti.Import()
	err := ti.Group.Wait()
	s.Equal(4, len(ti.TableCP.Engines)) // 3 data engines + 1 index engine
	s.ErrorContains(err, "mock ingest fail")

	wg.Wait()
}
