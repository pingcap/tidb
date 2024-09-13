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

package notifier

import (
	"context"
	"fmt"

	sess "github.com/pingcap/tidb/pkg/ddl/session"
)

// Store is the (de)serialization and persistent layer.
type Store interface {
	Insert(context.Context, *sess.Session, *schemaChange) error
	UpdateProcessed(
		ctx context.Context,
		se *sess.Session,
		ddlJobID int64,
		multiSchemaChangeID int,
		processedBy uint64,
	) error
	Delete(ctx context.Context, se *sess.Session, ddlJobID int64, multiSchemaChangeID int) error
	List(ctx context.Context, se *sess.Session, limit int) ([]*schemaChange, error)
}

// DefaultStore is the system table store. Still WIP now.
var DefaultStore Store

type tableStore struct {
	db    string
	table string
}

func (t *tableStore) Insert(ctx context.Context, s *sess.Session, change *schemaChange) error {
	// TODO: fill schema_change after we implement JSON serialization.
	sql := fmt.Sprintf(`
		INSERT INTO %s.%s (
			ddl_job_id,
			multi_schema_change_seq,
			schema_change,
			processed_by_flag
		) VALUES (%d, %d, '%s', 0)`,
		t.db, t.table,
		change.ddlJobID, change.multiSchemaChangeSeq, "{}",
	)
	_, err := s.Execute(ctx, sql, "ddl_notifier")
	return err
}

//revive:disable

func (t *tableStore) UpdateProcessed(ctx context.Context, se *sess.Session, ddlJobID int64, multiSchemaChangeID int, processedBy uint64) error {
	//TODO implement me
	panic("implement me")
}

func (t *tableStore) Delete(ctx context.Context, se *sess.Session, ddlJobID int64, multiSchemaChangeID int) error {
	//TODO implement me
	panic("implement me")
}

//revive:enable

func (t *tableStore) List(ctx context.Context, se *sess.Session, limit int) ([]*schemaChange, error) {
	sql := fmt.Sprintf(`
		SELECT
			ddl_job_id,
			multi_schema_change_seq,
			schema_change,
			processed_by_flag
		FROM %s.%s ORDER BY ddl_job_id, multi_schema_change_seq LIMIT %d`,
		t.db, t.table, limit)
	rows, err := se.Execute(ctx, sql, "ddl_notifier")
	if err != nil {
		return nil, err
	}
	ret := make([]*schemaChange, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, &schemaChange{
			ddlJobID:             row.GetInt64(0),
			multiSchemaChangeSeq: row.GetInt64(1),
			// TODO: fill schema_change after we implement JSON serialization.
			processedByFlag: row.GetUint64(3),
		})
	}
	return ret, nil
}

// OpenTableStore opens a store on a created table `db`.`table`. The table should
// be created with the table structure:
//
//	ddl_job_id BIGINT,
//	multi_schema_change_seq BIGINT COMMENT '-1 if the schema change does not belong to a multi-schema change DDL. 0 or positive numbers representing the sub-job index of a multi-schema change DDL',
//	schema_change JSON COMMENT 'SchemaChange at rest',
//	processed_by_flag BIGINT UNSIGNED DEFAULT 0 COMMENT 'flag to mark which subscriber has processed the event',
//	PRIMARY KEY(ddl_job_id, multi_schema_change_id)
func OpenTableStore(db, table string) Store {
	return &tableStore{db: db, table: table}
}
