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

package importer

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// GetEtcdClient returns an etcd client.
// exported for testing.
var GetEtcdClient = store.NewEtcdCli

// CheckRequirements checks the requirements for IMPORT INTO.
// we check the following things here:
//   - when import from file
//     1. there is no active job on the target table
//     2. the total file size > 0
//     3. if global sort, check required privileges
//   - target table should be empty
//   - no CDC or PiTR tasks running
//
// we check them one by one, and return the first error we meet.
func (e *LoadDataController) CheckRequirements(ctx context.Context, se sessionctx.Context) error {
	conn := se.GetSQLExecutor()
	if e.DataSourceType == DataSourceTypeFile {
		cnt, err := GetActiveJobCnt(ctx, conn, e.Plan.DBName, e.Plan.TableInfo.Name.L)
		if err != nil {
			return errors.Trace(err)
		}
		if cnt > 0 {
			return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("there is active job on the target table already")
		}
		if err := e.checkTotalFileSize(); err != nil {
			return err
		}
	}
	if err := e.checkTableEmpty(ctx, conn); err != nil {
		return err
	}
	if !e.DisablePrecheck {
		if err := e.checkCDCPiTRTasks(ctx, se); err != nil {
			return err
		}
		if e.Format == DataFormatParquet && len(e.dataFiles) > 0 {
			if err := mydump.PrecheckParquetFile(ctx, e.dataStore, e.dataFiles[0].Path); err != nil {
				return err
			}
		}
	}
	if e.IsGlobalSort() {
		return e.checkGlobalSortStorePrivilege(ctx)
	}
	return nil
}

func (e *LoadDataController) checkTotalFileSize() error {
	if e.TotalFileSize == 0 {
		// this happens when:
		// 1. no file matched when using wildcard
		// 2. all matched file is empty(with or without wildcard)
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("No file matched, or the file is empty. Please provide a valid file location.")
	}
	return nil
}

func (e *LoadDataController) checkTableEmpty(ctx context.Context, conn sqlexec.SQLExecutor) error {
	sql := common.SprintfWithIdentifiers("SELECT 1 FROM %s.%s USE INDEX() LIMIT 1", e.DBName, e.Table.Meta().Name.L)
	rs, err := conn.ExecuteInternal(ctx, sql)
	if err != nil {
		return err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("target table is not empty")
	}
	return nil
}

func (*LoadDataController) checkCDCPiTRTasks(ctx context.Context, se sessionctx.Context) error {
	cli, err := GetEtcdClient(se.GetStore())
	if err != nil {
		return err
	}
	defer terror.Call(cli.Close)

	pitrCli := streamhelper.NewMetaDataClient(cli)
	tasks, err := pitrCli.GetAllTasks(ctx)
	if err != nil {
		return err
	}
	if len(tasks) > 0 {
		names := make([]string, 0, len(tasks))
		for _, task := range tasks {
			names = append(names, task.Info.GetName())
		}
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(fmt.Sprintf("found PiTR log streaming task(s): %v,", names))
	}

	nameSet, err := cdcutil.GetRunningChangefeeds(ctx, cli)
	if err != nil {
		return errors.Trace(err)
	}

	if !nameSet.Empty() {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(nameSet.MessageToUser())
	}
	return nil
}

func (e *LoadDataController) checkGlobalSortStorePrivilege(ctx context.Context) error {
	// we need read/put/delete/list privileges on global sort store.
	// only support S3 now.
	target := "cloud storage"
	cloudStorageURL, err3 := objstore.ParseRawURL(e.Plan.CloudStorageURI)
	if err3 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, err3.Error())
	}
	b, err2 := objstore.ParseBackendFromURL(cloudStorageURL, nil)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, errors.GetErrStackMsg(err2))
	}

	if b.GetS3() == nil && b.GetGcs() == nil {
		// we only support S3 now, but in test we are using GCS.
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("unsupported cloud storage uri scheme: " + cloudStorageURL.Scheme)
	}

	opt := &objstore.Options{
		CheckPermissions: []objstore.Permission{
			objstore.GetObject,
			objstore.ListObjects,
			objstore.PutAndDeleteObject,
		},
	}
	if intest.InTest {
		opt.NoCredentials = true
	}
	_, err := objstore.New(ctx, b, opt)
	if err != nil {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("check cloud storage uri access: " + err.Error())
	}
	return nil
}
