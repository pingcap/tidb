package export

import (
	"errors"
	"fmt"
	"io"
	"runtime/debug"
)

type errWithStack struct {
	stack []byte
	raw   error
}

func (e errWithStack) Is(err error) bool {
	_, ok := err.(errWithStack)
	return ok
}

func (e errWithStack) Unwrap() error {
	return e.raw
}

func (e errWithStack) Error() string {
	return e.raw.Error()
}

func (e errWithStack) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, "err = ")
			io.WriteString(s, e.raw.Error())
			io.WriteString(s, "\n")
			io.WriteString(s, string(e.stack))
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, e.raw.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.raw.Error())
	}
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
