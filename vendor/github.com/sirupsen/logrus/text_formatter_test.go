package logrus

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestQuoting(t *testing.T) {
	tf := &TextFormatter{DisableColors: true}

	checkQuoting := func(q bool, value interface{}) {
		b, _ := tf.Format(WithField("test", value))
		idx := bytes.Index(b, ([]byte)("test="))
		cont := bytes.Contains(b[idx+5:], []byte(tf.QuoteCharacter))
		if cont != q {
			if q {
				t.Errorf("quoting expected for: %#v", value)
			} else {
				t.Errorf("quoting not expected for: %#v", value)
			}
		}
	}

	checkQuoting(false, "")
	checkQuoting(false, "abcd")
	checkQuoting(false, "v1.0")
	checkQuoting(false, "1234567890")
	checkQuoting(true, "/foobar")
	checkQuoting(true, "x y")
	checkQuoting(true, "x,y")
	checkQuoting(false, errors.New("invalid"))
	checkQuoting(true, errors.New("invalid argument"))

	// Test for custom quote character.
	tf.QuoteCharacter = "`"
	checkQuoting(false, "")
	checkQuoting(false, "abcd")
	checkQuoting(true, "/foobar")
	checkQuoting(true, errors.New("invalid argument"))

	// Test for multi-character quotes.
	tf.QuoteCharacter = "§~±"
	checkQuoting(false, "abcd")
	checkQuoting(true, errors.New("invalid argument"))

	// Test for quoting empty fields.
	tf.QuoteEmptyFields = true
	checkQuoting(true, "")
	checkQuoting(false, "abcd")
	checkQuoting(true, errors.New("invalid argument"))
}

func TestTimestampFormat(t *testing.T) {
	checkTimeStr := func(format string) {
		customFormatter := &TextFormatter{DisableColors: true, TimestampFormat: format}
		customStr, _ := customFormatter.Format(WithField("test", "test"))
		timeStart := bytes.Index(customStr, ([]byte)("time="))
		timeEnd := bytes.Index(customStr, ([]byte)("level="))
		timeStr := customStr[timeStart+5+len(customFormatter.QuoteCharacter) : timeEnd-1-len(customFormatter.QuoteCharacter)]
		if format == "" {
			format = time.RFC3339
		}
		_, e := time.Parse(format, (string)(timeStr))
		if e != nil {
			t.Errorf("time string \"%s\" did not match provided time format \"%s\": %s", timeStr, format, e)
		}
	}

	checkTimeStr("2006-01-02T15:04:05.000000000Z07:00")
	checkTimeStr("Mon Jan _2 15:04:05 2006")
	checkTimeStr("")
}

func TestDisableTimestampWithColoredOutput(t *testing.T) {
	tf := &TextFormatter{DisableTimestamp: true, ForceColors: true}

	b, _ := tf.Format(WithField("test", "test"))
	if strings.Contains(string(b), "[0000]") {
		t.Error("timestamp not expected when DisableTimestamp is true")
	}
}

// TODO add tests for sorting etc., this requires a parser for the text
// formatter output.
