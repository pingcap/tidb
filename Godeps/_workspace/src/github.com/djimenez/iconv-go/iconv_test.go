package iconv

import (
	"syscall"
	"testing"
)

type iconvTest struct {
	description    string
	input          string
	inputEncoding  string
	output         string
	outputEncoding string
	bytesRead      int
	bytesWritten   int
	err            error
}

var iconvTests = []iconvTest{
	iconvTest{
		"simple utf-8 to latin1 conversion success",
		"Hello World!", "utf-8",
		"Hello World!", "latin1",
		12, 12, nil,
	},
	iconvTest{
		"invalid source encoding causes EINVAL",
		"", "doesnotexist",
		"", "utf-8",
		0, 0, syscall.EINVAL,
	},
	iconvTest{
		"invalid destination encoding causes EINVAL",
		"", "utf-8",
		"", "doesnotexist",
		0, 0, syscall.EINVAL,
	},
	iconvTest{
		"invalid input sequence causes EILSEQ",
		"\xFF", "utf-8",
		"", "latin1",
		0, 0, syscall.EILSEQ,
	},
	iconvTest{
		"invalid input causes partial output and EILSEQ",
		"Hello\xFF", "utf-8",
		"Hello", "latin1",
		5, 5, syscall.EILSEQ,
	},
}

func TestConvertString(t *testing.T) {
	for _, test := range iconvTests {
		// perform the conversion
		output, err := ConvertString(test.input, test.inputEncoding, test.outputEncoding)

		// check that output and err match
		if output != test.output {
			t.Errorf("test \"%s\" failed, output did not match expected", test.description)
		}

		// check that err is same as expected
		if err != test.err {
			if test.err != nil {
				if err != nil {
					t.Errorf("test \"%s\" failed, got %s when expecting %s", test.description, err, test.err)
				} else {
					t.Errorf("test \"%s\" failed, got nil when expecting %s", test.description, test.err)
				}
			} else {
				t.Errorf("test \"%s\" failed, got unexpected error: %s", test.description, err)
			}
		}
	}
}

func TestConvert(t *testing.T) {
	for _, test := range iconvTests {
		// setup input buffer
		input := []byte(test.input)

		// setup a buffer as large as the expected bytesWritten
		output := make([]byte, 50)

		// peform the conversion
		bytesRead, bytesWritten, err := Convert(input, output, test.inputEncoding, test.outputEncoding)

		// check that bytesRead is same as expected
		if bytesRead != test.bytesRead {
			t.Errorf("test \"%s\" failed, bytesRead did not match expected", test.description)
		}

		// check that bytesWritten is same as expected
		if bytesWritten != test.bytesWritten {
			t.Errorf("test \"%s\" failed, bytesWritten did not match expected", test.description)
		}

		// check output bytes against expected - simplest to convert output to
		// string and then do an equality check which is actually a byte wise operation
		if string(output[:bytesWritten]) != test.output {
			t.Errorf("test \"%s\" failed, output did not match expected", test.description)
		}

		// check that err is same as expected
		if err != test.err {
			if test.err != nil {
				if err != nil {
					t.Errorf("test \"%s\" failed, got %s when expecting %s", test.description, err, test.err)
				} else {
					t.Errorf("test \"%s\" failed, got nil when expecting %s", test.description, test.err)
				}
			} else {
				t.Errorf("test \"%s\" failed, got unexpected error: %s", test.description, err)
			}
		}
	}
}
