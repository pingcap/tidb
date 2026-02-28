// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/tikv"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

type tableCfg struct {
	schema string
	table  string
	group  string
}

func run() error {
	// such as 16777214
	keyspaceID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		return err
	}
	// such as root:@tcp(127.0.0.1:4000)/
	connStr := os.Args[2]
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}

	// such as test,t_curr,A;test,t_next,B
	items := strings.Split(os.Args[3], ";")
	tableCfgs := make([]tableCfg, 0, len(items))
	for _, itm := range items {
		parts := strings.Split(itm, ",")
		if len(parts) != 3 {
			return fmt.Errorf("invalid table config: %s, should be schema,table,groupID", itm)
		}
		tableCfgs = append(tableCfgs, tableCfg{
			schema: parts[0],
			table:  parts[1],
			group:  parts[2],
		})
	}

	var opts []*RuleOp
	for _, cc := range tableCfgs {
		tableID, err := getTableID(db, cc.schema, cc.table)
		if err != nil {
			return err
		}
		fmt.Printf("table %s.%s id: %d\n", cc.schema, cc.table, tableID)
		ksCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: uint32(keyspaceID)})
		if err != nil {
			panic(err)
		}
		tblStartKey := tablecodec.EncodeTablePrefix(tableID)
		tblEndKey := tablecodec.EncodeTablePrefix(tableID + 1)
		rangeStart, rangeEnd := ksCodec.EncodeRange(tblStartKey, tblEndKey)
		pdRangeStart := codec.EncodeBytes(nil, rangeStart)
		pdRangeEnd := codec.EncodeBytes(nil, rangeEnd)
		opts = append(opts, &RuleOp{
			Rule: &Rule{
				GroupID:     "pd",
				ID:          fmt.Sprintf("%s_%s", cc.schema, cc.table),
				Index:       1024,
				Override:    true,
				StartKeyHex: hex.EncodeToString(pdRangeStart),
				EndKeyHex:   hex.EncodeToString(pdRangeEnd),
				Role:        Voter,
				Count:       3,
				LabelConstraints: []LabelConstraint{
					{
						Key:    "group",
						Op:     In,
						Values: []string{cc.group},
					},
				},
			},
		})
	}
	bytes, err := json.Marshal(opts)
	if err != nil {
		return err
	}
	fmt.Printf("%s", string(bytes))
	return nil
}

func getTableID(db *sql.DB, schema, table string) (int64, error) {
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "select tidb_table_id from information_schema.tables where table_schema=? and table_name=?;",
		schema, table)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("table %s.%s not found", schema, table)
	}
	var tableID int64
	if err = rows.Scan(&tableID); err != nil {
		return 0, err
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	return tableID, nil
}
