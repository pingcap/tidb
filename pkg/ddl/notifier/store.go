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
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
)

// Store is the (de)serialization and persistent layer.
type Store interface {
	Insert(context.Context, *sess.Session, *schemaChange) error
	UpdateProcessed(
		ctx context.Context,
		se *sess.Session,
		ddlJobID int64,
		multiSchemaChangeID int64,
		processedBy uint64,
	) error
	DeleteAndCommit(ctx context.Context, se *sess.Session, ddlJobID int64, multiSchemaChangeID int) error
	List(ctx context.Context, se *sess.Session) ([]*schemaChange, error)
}

// DefaultStore is the system table store. Still WIP now.
var DefaultStore Store

type tableStore struct {
	db    string
	table string
}

func (t *tableStore) Insert(ctx context.Context, s *sess.Session, change *schemaChange) error {
	event, err := json.Marshal(change.event)
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`
		INSERT INTO %s.%s (
			ddl_job_id,
			sub_job_id,
			schema_change,
			processed_by_flag
		) VALUES (%%?, %%?, %%?, 0)`,
		t.db, t.table,
	)
	_, err = s.Execute(
		ctx, sql, "ddl_notifier",
		change.ddlJobID, change.multiSchemaChangeSeq, event,
	)
	return err
}

func (t *tableStore) UpdateProcessed(
	ctx context.Context,
	se *sess.Session,
	ddlJobID int64,
	multiSchemaChangeID int64,
	processedBy uint64,
) error {
	sql := fmt.Sprintf(`
		UPDATE %s.%s
		SET processed_by_flag = %d
		WHERE ddl_job_id = %d AND sub_job_id = %d`,
		t.db, t.table,
		processedBy,
		ddlJobID, multiSchemaChangeID)
	_, err := se.Execute(ctx, sql, "ddl_notifier")
	return err
}

func (t *tableStore) DeleteAndCommit(
	ctx context.Context,
	se *sess.Session,
	ddlJobID int64,
	multiSchemaChangeID int,
) (err error) {
	if err = se.Begin(ctx); err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err == nil {
			err = errors.Trace(se.Commit(ctx))
		} else {
			se.Rollback()
		}
	}()
	sql := fmt.Sprintf(`
		DELETE FROM %s.%s
		WHERE ddl_job_id = %d AND sub_job_id = %d`,
		t.db, t.table,
		ddlJobID, multiSchemaChangeID)
	_, err = se.Execute(ctx, sql, "ddl_notifier")
	return errors.Trace(err)
}

func (t *tableStore) List(ctx context.Context, se *sess.Session) ([]*schemaChange, error) {
	sql := fmt.Sprintf(`
		SELECT
			ddl_job_id,
			sub_job_id,
			schema_change,
			processed_by_flag
		FROM %s.%s ORDER BY ddl_job_id, sub_job_id`,
		t.db, t.table)
	rows, err := se.Execute(ctx, sql, "ddl_notifier")
	if err != nil {
		return nil, err
	}
	ret := make([]*schemaChange, 0, len(rows))
	for _, row := range rows {
		event := SchemaChangeEvent{}
		err = json.Unmarshal(row.GetBytes(2), &event)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, &schemaChange{
			ddlJobID:             row.GetInt64(0),
			multiSchemaChangeSeq: row.GetInt64(1),
			event:                &event,
			processedByFlag:      row.GetUint64(3),
		})
	}
	return ret, nil
}

// OpenTableStore opens a store on a created table `db`.`table`. The table should
// be created with the table structure:
//
//	ddl_job_id BIGINT,
//	sub_job_id BIGINT COMMENT '-1 if the schema change does not belong to a multi-schema change DDL or a merged DDL. 0 or positive numbers representing the sub-job index of a multi-schema change DDL or a merged DDL',
//	schema_change JSON COMMENT 'SchemaChange at rest',
//	processed_by_flag BIGINT UNSIGNED DEFAULT 0 COMMENT 'flag to mark which subscriber has processed the event',
//	PRIMARY KEY(ddl_job_id, multi_schema_change_id)
func OpenTableStore(db, table string) Store {
	return &tableStore{db: db, table: table}
}
