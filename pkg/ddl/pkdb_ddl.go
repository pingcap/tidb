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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import "github.com/pingcap/tidb/pkg/parser/ast"

// CreateTableCTASConfig is the configuration of `create table as select`
type CreateTableCTASConfig struct {
	CTASSnapshotTS      uint64
	CTASSql             string
	CTASOnDuplicate     ast.OnDuplicateKeyHandlingType
	CTASSubmitterExecID string
	CTASSubmitterConnID uint64
}


// WithCTASSnapshotTS applies the snapshot timestamp when CTAS is submitted
func WithCTASSnapshotTS(ts uint64) CreateTableOption {
	return func(cfg *CreateTableConfig) {
		cfg.CTASSnapshotTS = ts
	}
}

// WithCTASCTASSql applies the original query when CTAS is submitted
func WithCTASCTASSql(sql string) CreateTableOption {
	return func(cfg *CreateTableConfig) {
		cfg.CTASSql = sql
	}
}

// WithCTASOnDuplicate applies how CTAS handles duplicate rows
func WithCTASOnDuplicate(dup ast.OnDuplicateKeyHandlingType) CreateTableOption {
	return func(cfg *CreateTableConfig) {
		cfg.CTASOnDuplicate = dup
	}
}

// WithCTASSubmitterExecID applies the submitter's identifier
func WithCTASSubmitterExecID(execID string) CreateTableOption {
	return func(cfg *CreateTableConfig) {
		cfg.CTASSubmitterExecID = execID
	}
}

// WithCTASSubmitterConnID applies CTAS's session identifier
func WithCTASSubmitterConnID(connID uint64) CreateTableOption {
	return func(cfg *CreateTableConfig) {
		cfg.CTASSubmitterConnID = connID
	}
}
