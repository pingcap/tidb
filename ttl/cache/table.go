// Copyright 2022 PingCAP, Inc.
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

package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func getTableKeyColumns(tbl *model.TableInfo) ([]*model.ColumnInfo, []*types.FieldType, error) {
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				return []*model.ColumnInfo{tbl.Columns[i]}, []*types.FieldType{&tbl.Columns[i].FieldType}, nil
			}
		}
		return nil, nil, errors.Errorf("Cannot find primary key for table: %s", tbl.Name)
	}

	if tbl.IsCommonHandle {
		idxInfo := tables.FindPrimaryIndex(tbl)
		columns := make([]*model.ColumnInfo, len(idxInfo.Columns))
		fieldTypes := make([]*types.FieldType, len(idxInfo.Columns))
		for i, idxCol := range idxInfo.Columns {
			columns[i] = tbl.Columns[idxCol.Offset]
			fieldTypes[i] = &tbl.Columns[idxCol.Offset].FieldType
		}
		return columns, fieldTypes, nil
	}

	extraHandleColInfo := model.NewExtraHandleColInfo()
	return []*model.ColumnInfo{extraHandleColInfo}, []*types.FieldType{&extraHandleColInfo.FieldType}, nil
}

// PhysicalTable is used to provide some information for a physical table in TTL job
type PhysicalTable struct {
	// ID is the physical ID of the table
	ID int64
	// Schema is the database name of the table
	Schema model.CIStr
	*model.TableInfo
	// Partition is the partition name
	Partition model.CIStr
	// PartitionDef is the partition definition
	PartitionDef *model.PartitionDefinition
	// KeyColumns is the cluster index key columns for the table
	KeyColumns []*model.ColumnInfo
	// KeyColumnTypes is the types of the key columns
	KeyColumnTypes []*types.FieldType
	// TimeColum is the time column used for TTL
	TimeColumn *model.ColumnInfo
}

// NewPhysicalTable create a new PhysicalTable
func NewPhysicalTable(schema model.CIStr, tbl *model.TableInfo, partition model.CIStr) (*PhysicalTable, error) {
	if tbl.State != model.StatePublic {
		return nil, errors.Errorf("table '%s.%s' is not a public table", schema, tbl.Name)
	}

	ttlInfo := tbl.TTLInfo
	if ttlInfo == nil {
		return nil, errors.Errorf("table '%s.%s' is not a ttl table", schema, tbl.Name)
	}

	timeColumn := tbl.FindPublicColumnByName(ttlInfo.ColumnName.L)
	if timeColumn == nil {
		return nil, errors.Errorf("time column '%s' is not public in ttl table '%s.%s'", ttlInfo.ColumnName, schema, tbl.Name)
	}

	keyColumns, keyColumTypes, err := getTableKeyColumns(tbl)
	if err != nil {
		return nil, err
	}

	var physicalID int64
	var partitionDef *model.PartitionDefinition
	if tbl.Partition == nil {
		if partition.L != "" {
			return nil, errors.Errorf("table '%s.%s' is not a partitioned table", schema, tbl.Name)
		}
		physicalID = tbl.ID
	} else {
		if partition.L == "" {
			return nil, errors.Errorf("partition name is required, table '%s.%s' is a partitioned table", schema, tbl.Name)
		}

		for i := range tbl.Partition.Definitions {
			def := &tbl.Partition.Definitions[i]
			if def.Name.L == partition.L {
				partitionDef = def
			}
		}

		if partitionDef == nil {
			return nil, errors.Errorf("partition '%s' is not found in ttl table '%s.%s'", partition.O, schema, tbl.Name)
		}

		physicalID = partitionDef.ID
	}

	return &PhysicalTable{
		ID:             physicalID,
		Schema:         schema,
		TableInfo:      tbl,
		Partition:      partition,
		PartitionDef:   partitionDef,
		KeyColumns:     keyColumns,
		KeyColumnTypes: keyColumTypes,
		TimeColumn:     timeColumn,
	}, nil
}

// ValidateKey validates a key
func (t *PhysicalTable) ValidateKey(key []types.Datum) error {
	if len(t.KeyColumns) != len(key) {
		return errors.Errorf("invalid key length: %d, expected %d", len(key), len(t.KeyColumns))
	}
	return nil
}

// EvalExpireTime returns the expired time
func (t *PhysicalTable) EvalExpireTime(ctx context.Context, se session.Session, now time.Time) (expire time.Time, err error) {
	tz := se.GetSessionVars().TimeZone

	expireExpr := t.TTLInfo.IntervalExprStr
	unit := ast.TimeUnitType(t.TTLInfo.IntervalTimeUnit)

	var rows []chunk.Row
	rows, err = se.ExecuteSQL(
		ctx,
		// FROM_UNIXTIME does not support negative value, so we use `FROM_UNIXTIME(0) + INTERVAL <current_ts>` to present current time
		fmt.Sprintf("SELECT FROM_UNIXTIME(0) + INTERVAL %d SECOND - INTERVAL %s %s", now.Unix(), expireExpr, unit.String()),
	)

	if err != nil {
		return
	}

	tm := rows[0].GetTime(0)
	return tm.CoreTime().GoTime(tz)
}
