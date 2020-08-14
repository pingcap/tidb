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

package structure

import (
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
)

var (
	// ErrInvalidHashKeyFlag used by structure
	ErrInvalidHashKeyFlag = terror.ClassStructure.New(mysql.ErrInvalidHashKeyFlag, mysql.MySQLErrName[mysql.ErrInvalidHashKeyFlag])
	// ErrInvalidListIndex used by structure
	ErrInvalidListIndex = terror.ClassStructure.New(mysql.ErrInvalidListIndex, mysql.MySQLErrName[mysql.ErrInvalidListIndex])
	// ErrInvalidListMetaData used by structure
	ErrInvalidListMetaData = terror.ClassStructure.New(mysql.ErrInvalidListMetaData, mysql.MySQLErrName[mysql.ErrInvalidListMetaData])
	// ErrWriteOnSnapshot used by structure
	ErrWriteOnSnapshot = terror.ClassStructure.New(mysql.ErrWriteOnSnapshot, mysql.MySQLErrName[mysql.ErrWriteOnSnapshot])
)

// NewStructure creates a TxStructure with Retriever, RetrieverMutator and key prefix.
func NewStructure(reader kv.Retriever, readWriter kv.RetrieverMutator, prefix []byte) *TxStructure {
	return &TxStructure{
		reader:     reader,
		readWriter: readWriter,
		prefix:     prefix,
	}
}

// TxStructure supports some simple data structures like string, hash, list, etc... and
// you can use these in a transaction.
type TxStructure struct {
	reader     kv.Retriever
	readWriter kv.RetrieverMutator
	prefix     []byte
}
