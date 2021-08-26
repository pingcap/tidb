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
		User:              "user",
		SchemaMetaVersion: 1,
		TxnStartTS:        1,
		TxnForUpdateTS:    1,
		IsReadConsistency: true,
		CurrentDB:         "database",
		TxnMode:           "optimistic",
		Query:             query,
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
