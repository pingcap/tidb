// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store      kv.Storage
	infoHandle *infoschema.Handle
	ddl        ddl.DDL
}

func (do *Domain) loadInfoSchema(txn kv.Transaction) (err error) {
	var schemas []*model.DBInfo
	err = util.ScanMetaWithPrefix(txn, meta.SchemaMetaPrefix, func(key []byte, value []byte) bool {
		di := &model.DBInfo{}
		err := json.Unmarshal(value, di)
		if err != nil {
			log.Fatal(err)
		}
		schemas = append(schemas, di)
		return true
	})
	if err != nil {
		return errors.Trace(err)
	}
	do.infoHandle.Set(schemas)
	return
}

// InfoSchema gets information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoHandle.Get()
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// Store gets KV store from domain.
func (do *Domain) Store() kv.Storage {
	return do.store
}

// NewDomain creates a new domain.
func NewDomain(store kv.Storage) (d *Domain, err error) {
	infoHandle := infoschema.NewHandle(store)
	ddl := ddl.NewDDL(store, infoHandle)
	d = &Domain{
		store:      store,
		infoHandle: infoHandle,
		ddl:        ddl,
	}
	err = kv.RunInNewTxn(d.store, false, d.loadInfoSchema)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return d, nil
}
