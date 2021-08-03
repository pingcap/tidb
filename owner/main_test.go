//Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"os"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()

	// Ignore this test on the windows platform, because calling unix socket with address in
	// host:port format fails on windows.
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		log.Fatal("put failed", zap.Error(err))
	}

	goleak.VerifyTestMain(m)
}
