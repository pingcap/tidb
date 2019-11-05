package logrus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFieldValueError(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &Logger{
		Out:       buf,
		Formatter: new(JSONFormatter),
		Hooks:     make(LevelHooks),
		Level:     DebugLevel,
	}
	l.WithField("func", func() {}).Info("test")
	fmt.Println(buf.String())
	var data map[string]interface{}
	json.Unmarshal(buf.Bytes(), &data)
	_, ok := data[FieldKeyLogrusError]
	require.True(t, ok)
}

func TestNoFieldValueError(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &Logger{
		Out:       buf,
		Formatter: new(JSONFormatter),
		Hooks:     make(LevelHooks),
		Level:     DebugLevel,
	}
	l.WithField("str", "str").Info("test")
	fmt.Println(buf.String())
	var data map[string]interface{}
	json.Unmarshal(buf.Bytes(), &data)
	_, ok := data[FieldKeyLogrusError]
	require.False(t, ok)
}
