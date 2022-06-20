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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
)

type miniDDL struct {
	*ddl

	store kv.Storage
}

func NewMiniDDL(ctx context.Context) DDL {
	store, err := mockstore.NewMockStore()
	if err != nil {
		panic(err)
	}
	infoCache := infoschema.NewCache(1)
	// TODO: this will create `test` db, should drop it
	is := infoschema.MockInfoSchemaWithSchemaVer(nil, 0)
	infoCache.Insert(is, 0)

	ret := &miniDDL{
		store: store,
	}
	ret.ddl = newDDL(
		ctx,
		WithStore(store),
		WithInfoCache(infoCache),
		WithDoDDLJob(ret.DoDDLJob),
	)

	return ret
}

var FetchAllSchemasWithTables func(m *meta.Meta) ([]*model.DBInfo, error)

func (m *miniDDL) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	// imitate enQueue and deQueue
	b, err := job.Encode(true)
	if err != nil {
		return err
	}
	j := &model.Job{}
	err = j.Decode(b)
	if err != nil {
		return err
	}

	txn, err := m.store.Begin()
	if err != nil {
		return err
	}

	me := meta.NewMeta(txn)
	var ver int64
	for !j.IsDone() {
		switch j.Type {
		case model.ActionCreateSchema:
			ver, err = onCreateSchema(m.ddl.ddlCtx, me, j)
		case model.ActionCreateTable:
			ver, err = onCreateTable(m.ddl.ddlCtx, me, j)
		case model.ActionAddColumn:
			ver, err = onAddColumn(m.ddl.ddlCtx, me, j)
		case model.ActionDropIndex, model.ActionDropPrimaryKey:
			ver, err = onDropIndex(m.ddl.ddlCtx, me, j)
		default:
			panic("not implement")
		}
		if err != nil {
			return err
		}
		println(j.String())
	}

	err = txn.Commit(context.Background())
	if err != nil {
		return err
	}

	txn, err = m.store.Begin()
	if err != nil {
		return err
	}
	me = meta.NewMeta(txn)

	// TODO: update from diff
	dbInfos, err := FetchAllSchemasWithTables(me)
	if err != nil {
		return err
	}

	is := infoschema.MockInfoSchemaFromDBInfoWithSchemaVer(dbInfos, ver)
	m.ddl.infoCache.Insert(is, uint64(ver))

	return nil
}
