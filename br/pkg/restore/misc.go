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

package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// deprecated parameter
type Granularity string

const (
	FineGrained   Granularity = "fine-grained"
	CoarseGrained Granularity = "coarse-grained"
)

type UniqueTableName struct {
	DB    string
	Table string
}

func TransferBoolToValue(enable bool) string {
	if enable {
		return "ON"
	}
	return "OFF"
}

// GetTableSchema returns the schema of a table from TiDB.
func GetTableSchema(
	dom *domain.Domain,
	dbName pmodel.CIStr,
	tableName pmodel.CIStr,
) (*model.TableInfo, error) {
	info := dom.InfoSchema()
	table, err := info.TableByName(context.Background(), dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

const maxUserTablesNum = 10

// AssertUserDBsEmpty check whether user dbs exist in the cluster
func AssertUserDBsEmpty(dom *domain.Domain) error {
	databases := dom.InfoSchema().AllSchemas()
	m := meta.NewSnapshotMeta(dom.Store().GetSnapshot(kv.MaxVersion))
	userTables := make([]string, 0, maxUserTablesNum+1)
	appendTables := func(dbName, tableName string) bool {
		if len(userTables) >= maxUserTablesNum {
			userTables = append(userTables, "...")
			return true
		}
		userTables = append(userTables, fmt.Sprintf("%s.%s", dbName, tableName))
		return false
	}
LISTDBS:
	for _, db := range databases {
		dbName := db.Name.L
		if tidbutil.IsMemOrSysDB(dbName) {
			continue
		}
		tables, err := m.ListSimpleTables(db.ID)
		if err != nil {
			return errors.Annotatef(err, "failed to iterator tables of database[id=%d]", db.ID)
		}
		if len(tables) == 0 {
			// tidb create test db on fresh cluster
			// if it's empty we don't take it as user db
			if dbName != "test" {
				if appendTables(db.Name.O, "") {
					break LISTDBS
				}
			}
			continue
		}
		for _, table := range tables {
			if appendTables(db.Name.O, table.Name.O) {
				break LISTDBS
			}
		}
	}
	if len(userTables) > 0 {
		return errors.Annotate(berrors.ErrRestoreNotFreshCluster,
			"user db/tables: "+strings.Join(userTables, ", "))
	}
	return nil
}

// GetTS gets a new timestamp from PD.
func GetTS(ctx context.Context, pdClient pd.Client) (uint64, error) {
	p, l, err := pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// GetTSWithRetry gets a new timestamp with retry from PD.
func GetTSWithRetry(ctx context.Context, pdClient pd.Client) (uint64, error) {
	var (
		startTS  uint64
		getTSErr error
		retry    uint
	)

	err := utils.WithRetry(ctx, func() error {
		startTS, getTSErr = GetTS(ctx, pdClient)
		failpoint.Inject("get-ts-error", func(val failpoint.Value) {
			if val.(bool) && retry < 3 {
				getTSErr = errors.Errorf("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
			}
		})

		retry++
		if getTSErr != nil {
			log.Warn("failed to get TS, retry it", zap.Uint("retry time", retry), logutil.ShortError(getTSErr))
		}
		return getTSErr
	}, utils.NewPDReqBackoffer())

	if err != nil {
		log.Error("failed to get TS", zap.Error(err))
	}
	return startTS, errors.Trace(err)
}
