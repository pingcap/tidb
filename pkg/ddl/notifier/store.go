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
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// CloseFn is the function to release the resource.
type CloseFn func()

// Store is the (de)serialization and persistent layer.
type Store interface {
	Insert(context.Context, *sess.Session, *SchemaChange) error
	UpdateProcessed(
		ctx context.Context,
		se *sess.Session,
		ddlJobID int64,
		multiSchemaChangeID int64,
		processedBy uint64,
	) error
	DeleteAndCommit(ctx context.Context, se *sess.Session, ddlJobID int64, multiSchemaChangeID int) error
	// List will start a transaction of given session and read all schema changes
	// through a ListResult. The ownership of session is occupied by Store until
	// CloseFn is called.
	List(ctx context.Context, se *sess.Session) (ListResult, CloseFn)
}

// ListResult is the result stream of a List operation.
type ListResult interface {
	// Read tries to decode at most `len(changes)` SchemaChange into given slices. It
	// returns the number of schemaChanges decoded, 0 means no more schemaChanges.
	//
	// Note that the previous SchemaChange in the slice will be overwritten when call
	// Read.
	Read(changes []*SchemaChange) (int, error)
}

type tableStore struct {
	db    string
	table string
}

// Insert implements Store interface.
func (t *tableStore) Insert(ctx context.Context, s *sess.Session, change *SchemaChange) error {
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
		change.ddlJobID, change.subJobID, event,
	)
	return err
}

// UpdateProcessed implements Store interface.
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

// DeleteAndCommit implements Store interface.
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

// List implements Store interface.
func (t *tableStore) List(ctx context.Context, se *sess.Session) (ListResult, CloseFn) {
	return &listResult{
		ctx: ctx,
		se:  se,
		sqlTemplate: fmt.Sprintf(`
			SELECT
				ddl_job_id,
				sub_job_id,
				schema_change,
				processed_by_flag
			FROM %s.%s
			WHERE (ddl_job_id, sub_job_id) > (%%?, %%?)
			ORDER BY ddl_job_id, sub_job_id
			LIMIT %%?`,
			t.db, t.table),
		// DDL job ID are always positive, so we can use 0 as the initial value.
		maxReturnedDDLJobID: 0,
		maxReturnedSubJobID: 0,
	}, se.Rollback
}

type listResult struct {
	ctx                 context.Context
	se                  *sess.Session
	sqlTemplate         string
	maxReturnedDDLJobID int64
	maxReturnedSubJobID int64
}

// Read implements ListResult interface.
func (r *listResult) Read(changes []*SchemaChange) (int, error) {
	if r.maxReturnedDDLJobID == 0 && r.maxReturnedSubJobID == 0 {
		err := r.se.Begin(r.ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	rows, err := r.se.Execute(
		r.ctx, r.sqlTemplate, "ddl_notifier",
		r.maxReturnedDDLJobID, r.maxReturnedSubJobID, len(changes),
	)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err = r.unmarshalSchemaChanges(rows, changes); err != nil {
		return 0, errors.Trace(err)
	}
	return len(rows), nil
}

func (r *listResult) unmarshalSchemaChanges(rows []chunk.Row, changes []*SchemaChange) error {
	for i, row := range rows {
		if changes[i] == nil {
			changes[i] = new(SchemaChange)
		}
		if changes[i].event == nil {
			changes[i].event = new(SchemaChangeEvent)
		}
		if changes[i].event.inner == nil {
			changes[i].event.inner = new(jsonSchemaChangeEvent)
		}

		err := json.Unmarshal(row.GetBytes(2), changes[i].event.inner)
		if err != nil {
			return errors.Trace(err)
		}
		changes[i].ddlJobID = row.GetInt64(0)
		changes[i].subJobID = row.GetInt64(1)
		changes[i].processedByFlag = row.GetUint64(3)

		if i == len(rows)-1 {
			r.maxReturnedDDLJobID = changes[i].ddlJobID
			r.maxReturnedSubJobID = changes[i].subJobID
		}
	}
	return nil
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
