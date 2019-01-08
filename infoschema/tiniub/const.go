// Copyright 2019 PingCAP, Inc.
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

package tiniub

const tableSlowQuery = "CREATE TABLE if not exists slow_query (" +
	"`SQL` VARCHAR(4096)," +
	"`START` TIMESTAMP," +
	"`DURATION` TIME," +
	"DETAILS VARCHAR(256)," +
	"SUCC TINYINT," +
	"CONN_ID BIGINT," +
	"TRANSACTION_TS BIGINT," +
	"USER VARCHAR(32) NOT NULL," +
	"DB VARCHAR(64) NOT NULL," +
	"TABLE_IDS VARCHAR(256)," +
	"INDEX_IDS VARCHAR(256)," +
	"INTERNAL TINYINT);"
