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

// Package ddl is the core of TiDB DDL layer. It is used to manage the schema of
// TiDB Cluster.
//
// TiDB executes using the Online DDL algorithm, see docs/design/2018-10-08-online-DDL.md
// for more details.
//
// DDL maintains the following invariant:
//
//	At any time, for each schema object, such as a table, there are at most 2 versions
//	can exist for it, current version N loaded by all TiDBs and version N+1 pushed
//	forward by DDL, before we can finish the DDL or continue to next operation, we
//	need to make sure all TiDBs have synchronized to version N+1.
//	Note that we are using a global version number for all schema objects, so the
//	versions related some table might not be continuous, as DDLs are executed in parallel.
package ddl
