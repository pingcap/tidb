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

package logutil

import (
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DDLLogger with category "ddl" is used to log DDL related messages. Do not use
// it to log the message that is not related to DDL.
func DDLLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String(logutil.LogFieldCategory, "ddl"))
}

// DDLUpgradingLogger with category "ddl-upgrading" is used to log DDL related
// messages during the upgrading process. Do not use it to log the message that
// is not related to DDL.
func DDLUpgradingLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String(logutil.LogFieldCategory, "ddl-upgrading"))
}

// DDLIngestLogger with category "ddl-ingest" is used to log DDL related messages
// during the fast DDL process. Do not use it to log the message that is not
// related to DDL.
func DDLIngestLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String(logutil.LogFieldCategory, "ddl-ingest"))
}
