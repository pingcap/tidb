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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/utils"
	tidb "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/etcd"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	etcdDialTimeout = 5 * time.Second
)

// GetEtcdClient returns an etcd client.
// exported for testing.
var GetEtcdClient = getEtcdClient

// CheckRequirements checks the requirements for IMPORT INTO.
// we check the following things here:
//  1. target table should be empty
//  2. no CDC or PiTR tasks running
//
// todo: check if there's running lightning tasks?
// we check them one by one, and return the first error we meet.
func (e *LoadDataController) CheckRequirements(ctx context.Context, conn sqlexec.SQLExecutor) error {
	if err := e.checkTotalFileSize(); err != nil {
		return err
	}
	if err := e.checkTableEmpty(ctx, conn); err != nil {
		return err
	}
	return e.checkCDCPiTRTasks(ctx)
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
	sql := fmt.Sprintf("SELECT 1 FROM %s USE INDEX() LIMIT 1", common.UniqueTable(e.DBName, e.Table.Meta().Name.L))
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

func (*LoadDataController) checkCDCPiTRTasks(ctx context.Context) error {
	cli, err := GetEtcdClient()
	if err != nil {
		return err
	}
	defer terror.Call(cli.Close)

	pitrCli := streamhelper.NewMetaDataClient(cli.GetClient())
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

	nameSet, err := utils.GetCDCChangefeedNameSet(ctx, cli.GetClient())
	if err != nil {
		return errors.Trace(err)
	}

	if !nameSet.Empty() {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(nameSet.MessageToUser())
	}
	return nil
}

func getEtcdClient() (*etcd.Client, error) {
	tidbCfg := tidb.GetGlobalConfig()
	tls, err := util.NewTLSConfig(
		util.WithCAPath(tidbCfg.Security.ClusterSSLCA),
		util.WithCertAndKeyPath(tidbCfg.Security.ClusterSSLCert, tidbCfg.Security.ClusterSSLKey),
	)
	if err != nil {
		return nil, err
	}
	ectdEndpoints, err := util.ParseHostPortAddr(tidbCfg.Path)
	if err != nil {
		return nil, err
	}
	return etcd.NewClientFromCfg(ectdEndpoints, etcdDialTimeout, "", tls)
}
