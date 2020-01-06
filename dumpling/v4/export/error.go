package export

import (
	"errors"
	"runtime/debug"
	"strings"
)

type errWithStack struct {
	stack []byte
	raw   error
}

func (e errWithStack) Unwrap() error {
	return e.raw
}

func (e errWithStack) Error() string {
	var b strings.Builder
	b.WriteString("err = ")
	b.WriteString(e.raw.Error())
	b.WriteString("\n")
	b.Write(e.stack)
	return b.String()
}

var stackErr errWithStack

func withStack(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, stackErr) {
		return err
	}

	return errWithStack{
		raw:   err,
		stack: debug.Stack(),
	}
}
