package lightstep

import (
	"encoding/json"
	"fmt"

	"github.com/lightstep/lightstep-tracer-go/lightstep_thrift"
	"github.com/opentracing/opentracing-go/log"
)

const (
	deprecatedFieldKeyEvent   = "event"
	deprecatedFieldKeyPayload = "payload"
)

// thrift_rpc.thriftLogFieldEncoder is an implementation of the log.Encoder interface
// that handles only the deprecated OpenTracing
// Span.LogEvent/LogEventWithPayload calls. (Since the thrift client is being
// phased out anyway)
type thriftLogFieldEncoder struct {
	logRecord *lightstep_thrift.LogRecord
	recorder  *thriftCollectorClient
}

func (lfe *thriftLogFieldEncoder) EmitString(key, value string) {
	if len(key) > lfe.recorder.maxLogMessageLen {
		key = key[:(lfe.recorder.maxLogKeyLen-1)] + ellipsis
	}

	if len(value) > lfe.recorder.maxLogMessageLen {
		value = value[:(lfe.recorder.maxLogMessageLen-1)] + ellipsis
	}

	lfe.logRecord.Fields = append(lfe.logRecord.Fields, &lightstep_thrift.KeyValue{
		Key:   key,
		Value: value,
	})
}

func (lfe *thriftLogFieldEncoder) EmitObject(key string, value interface{}) {
	var thriftPayload string
	jsonString, err := json.Marshal(value)
	if err != nil {
		thriftPayload = fmt.Sprintf("Error encoding payload object: %v", err)
	} else {
		thriftPayload = string(jsonString)
	}
	if len(thriftPayload) > lfe.recorder.maxLogMessageLen {
		thriftPayload = thriftPayload[:(lfe.recorder.maxLogMessageLen-1)] + ellipsis
	}
	lfe.logRecord.Fields = append(lfe.logRecord.Fields, &lightstep_thrift.KeyValue{
		Key:   key,
		Value: thriftPayload,
	})
}

func (lfe *thriftLogFieldEncoder) EmitBool(key string, value bool) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitInt(key string, value int) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitInt32(key string, value int32) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitInt64(key string, value int64) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitUint32(key string, value uint32) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitUint64(key string, value uint64) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitFloat32(key string, value float32) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitFloat64(key string, value float64) {
	lfe.EmitString(key, fmt.Sprint(value))
}
func (lfe *thriftLogFieldEncoder) EmitLazyLogger(value log.LazyLogger) {}
