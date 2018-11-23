package basictracer

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/opentracing/opentracing-go/log"
)

// LogFieldValidator facilitates testing of Span.Log*() implementations.
//
// Usage:
//
//     fv := log.NewLogFieldValidator(t, someLogStructure.Fields)
//     fv.
//         ExpectNextFieldEquals("key1", reflect.String, "some string value").
//         ExpectNextFieldEquals("key2", reflect.Uint32, "4294967295")
//
// LogFieldValidator satisfies the log.Encoder interface and thus is able to
// marshal log.Field instances (which it takes advantage of internally).
type LogFieldValidator struct {
	t               *testing.T
	fieldIdx        int
	fields          []log.Field
	nextKey         string
	nextKind        reflect.Kind
	nextValAsString string
}

// NewLogFieldValidator returns a new validator that will test the contents of
// `fields`.
func NewLogFieldValidator(t *testing.T, fields []log.Field) *LogFieldValidator {
	return &LogFieldValidator{
		t:      t,
		fields: fields,
	}
}

// ExpectNextFieldEquals facilitates a fluent way of testing the contents
// []Field slices.
func (fv *LogFieldValidator) ExpectNextFieldEquals(key string, kind reflect.Kind, valAsString string) *LogFieldValidator {
	if len(fv.fields) < fv.fieldIdx {
		_, file, line, _ := runtime.Caller(1)
		fv.t.Errorf("%s:%d Expecting more than the %v Fields we have", file, line, len(fv.fields))
	}
	fv.nextKey = key
	fv.nextKind = kind
	fv.nextValAsString = valAsString
	fv.fields[fv.fieldIdx].Marshal(fv)
	fv.fieldIdx++
	return fv
}

// EmitString satisfies the Encoder interface
func (fv *LogFieldValidator) EmitString(key, value string) {
	fv.validateNextField(key, reflect.String, value)
}

// EmitBool satisfies the Encoder interface
func (fv *LogFieldValidator) EmitBool(key string, value bool) {
	fv.validateNextField(key, reflect.Bool, value)
}

// EmitInt satisfies the Encoder interface
func (fv *LogFieldValidator) EmitInt(key string, value int) {
	fv.validateNextField(key, reflect.Int, value)
}

// EmitInt32 satisfies the Encoder interface
func (fv *LogFieldValidator) EmitInt32(key string, value int32) {
	fv.validateNextField(key, reflect.Int32, value)
}

// EmitInt64 satisfies the Encoder interface
func (fv *LogFieldValidator) EmitInt64(key string, value int64) {
	fv.validateNextField(key, reflect.Int64, value)
}

// EmitUint32 satisfies the Encoder interface
func (fv *LogFieldValidator) EmitUint32(key string, value uint32) {
	fv.validateNextField(key, reflect.Uint32, value)
}

// EmitUint64 satisfies the Encoder interface
func (fv *LogFieldValidator) EmitUint64(key string, value uint64) {
	fv.validateNextField(key, reflect.Uint64, value)
}

// EmitFloat32 satisfies the Encoder interface
func (fv *LogFieldValidator) EmitFloat32(key string, value float32) {
	fv.validateNextField(key, reflect.Float32, value)
}

// EmitFloat64 satisfies the Encoder interface
func (fv *LogFieldValidator) EmitFloat64(key string, value float64) {
	fv.validateNextField(key, reflect.Float64, value)
}

// EmitObject satisfies the Encoder interface
func (fv *LogFieldValidator) EmitObject(key string, value interface{}) {
	fv.validateNextField(key, reflect.Interface, value)
}

// EmitLazyLogger satisfies the Encoder interface
func (fv *LogFieldValidator) EmitLazyLogger(value log.LazyLogger) {
	fv.t.Error("Test infrastructure does not support EmitLazyLogger yet")
}

func (fv *LogFieldValidator) validateNextField(key string, actualKind reflect.Kind, value interface{}) {
	// Reference the ExpectNextField caller in error messages.
	_, file, line, _ := runtime.Caller(4)
	if fv.nextKey != key {
		fv.t.Errorf("%s:%d Bad key: expected %q, found %q", file, line, fv.nextKey, key)
	}
	if fv.nextKind != actualKind {
		fv.t.Errorf("%s:%d Bad reflect.Kind: expected %v, found %v", file, line, fv.nextKind, actualKind)
		return
	}
	if fv.nextValAsString != fmt.Sprint(value) {
		fv.t.Errorf("%s:%d Bad value: expected %q, found %q", file, line, fv.nextValAsString, fmt.Sprint(value))
	}
	// All good.
}
