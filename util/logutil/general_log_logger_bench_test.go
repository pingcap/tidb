// Copyright 2021 PingCAP, Inc.
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
	"strings"
	"testing"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func BenchmarkGeneralLog(b *testing.B) {
	var buf strings.Builder
	for i := 0; i < 256; i++ {
		buf.WriteString("s")
	}
	query := buf.String()
	glEntry := GeneralLogEntry{
		ConnID:            1,
		user:              "user",
		schemaMetaVersion: 1,
		TxnStartTS:        1,
		TxnForUpdateTS:    1,
		IsReadConsistency: true,
		CurrentDB:         "database",
		TxnMode:           "optimistic",
		query:             query,
	}
	var glBuf buffer.Buffer
	zapFields := []zapcore.Field{
		{Key: "conn", Integer: 1, Type: zapcore.Uint64Type},
		{Key: "user", String: "user", Type: zapcore.StringType},
		{Key: "schemaVersion", Integer: 1, Type: zapcore.Uint64Type},
		{Key: "txnStartTS", Integer: 1, Type: zapcore.Uint64Type},
		{Key: "forUpdateTS", Integer: 1, Type: zapcore.Uint64Type},
		{Key: "isReadConsistency", Integer: 1, Type: zapcore.BoolType},
		{Key: "current_db", String: "database", Type: zapcore.StringType},
		{Key: "txn_mode", String: "optimistic", Type: zapcore.StringType},
		{Key: "sql", String: query, Type: zapcore.StringType},
	}
	zapEntry := zapcore.Entry{
		Level: zap.InfoLevel,
	}
	ec := zap.NewProductionEncoderConfig()
	ec.LevelKey = ""
	jsonEncoder := zapcore.NewJSONEncoder(ec)

	cfg := log.Config{
		DisableTimestamp: true,
	}
	textEncoder := log.NewTextEncoder(&cfg)

	b.Run("manual encoding", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			glEntry.writeToBufferDirect(&glBuf)
		}
	})
	b.Run("zap.jsonEncoder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jsonEncoder.EncodeEntry(zapEntry, zapFields)
			// fmt.Printf("%s", buf.String())
		}
	})
	b.Run("log.textEncoder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			textEncoder.EncodeEntry(zapEntry, zapFields)
		}
	})
}
