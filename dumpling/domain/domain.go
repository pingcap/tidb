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
	schemaMetaVersion, err := txn.GetInt64(meta.SchemaMetaVersion)
	if err != nil {
		return
	}
	do.infoHandle.SetSchemaMetaVersion(schemaMetaVersion)
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

func (do *Domain) onDDLChange(err error) error {
	if err != nil {
		return err
	}

	return do.reload()
}

func (do *Domain) reload() error {
	infoHandle := infoschema.NewHandle(do.store)
	do.infoHandle = infoHandle
	do.ddl = ddl.NewDDL(do.store, infoHandle, do.onDDLChange)
	err := kv.RunInNewTxn(do.store, false, do.loadInfoSchema)
	return errors.Trace(err)
}

// NewDomain creates a new domain.
func NewDomain(store kv.Storage) (d *Domain, err error) {
	d = &Domain{
		store: store,
	}

	err = d.reload()
	return d, errors.Trace(err)
}
