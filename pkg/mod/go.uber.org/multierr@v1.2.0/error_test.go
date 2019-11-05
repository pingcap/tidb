// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package multierr

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// richFormatError is an error that prints a different output depending on
// whether %v or %+v was used.
type richFormatError struct{}

func (r richFormatError) Error() string {
	return fmt.Sprint(r)
}

func (richFormatError) Format(f fmt.State, c rune) {
	if c == 'v' && f.Flag('+') {
		io.WriteString(f, "multiline\nmessage\nwith plus")
	} else {
		io.WriteString(f, "without plus")
	}
}

func appendN(initial, err error, n int) error {
	errs := initial
	for i := 0; i < n; i++ {
		errs = Append(errs, err)
	}
	return errs
}

func newMultiErr(errors ...error) error {
	return &multiError{errors: errors}
}

func TestCombine(t *testing.T) {
	tests := []struct {
		// Input
		giveErrors []error

		// Resulting error
		wantError error

		// %+v and %v string representations
		wantMultiline  string
		wantSingleline string
	}{
		{
			giveErrors: nil,
			wantError:  nil,
		},
		{
			giveErrors: []error{},
			wantError:  nil,
		},
		{
			giveErrors: []error{
				errors.New("foo"),
				nil,
				newMultiErr(
					errors.New("bar"),
				),
				nil,
			},
			wantError: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  foo\n" +
				" -  bar",
			wantSingleline: "foo; bar",
		},
		{
			giveErrors: []error{
				errors.New("foo"),
				newMultiErr(
					errors.New("bar"),
				),
			},
			wantError: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  foo\n" +
				" -  bar",
			wantSingleline: "foo; bar",
		},
		{
			giveErrors:     []error{errors.New("great sadness")},
			wantError:      errors.New("great sadness"),
			wantMultiline:  "great sadness",
			wantSingleline: "great sadness",
		},
		{
			giveErrors: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
			wantError: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  foo\n" +
				" -  bar",
			wantSingleline: "foo; bar",
		},
		{
			giveErrors: []error{
				errors.New("great sadness"),
				errors.New("multi\n  line\nerror message"),
				errors.New("single line error message"),
			},
			wantError: newMultiErr(
				errors.New("great sadness"),
				errors.New("multi\n  line\nerror message"),
				errors.New("single line error message"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  great sadness\n" +
				" -  multi\n" +
				"      line\n" +
				"    error message\n" +
				" -  single line error message",
			wantSingleline: "great sadness; " +
				"multi\n  line\nerror message; " +
				"single line error message",
		},
		{
			giveErrors: []error{
				errors.New("foo"),
				newMultiErr(
					errors.New("bar"),
					errors.New("baz"),
				),
				errors.New("qux"),
			},
			wantError: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
				errors.New("baz"),
				errors.New("qux"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  foo\n" +
				" -  bar\n" +
				" -  baz\n" +
				" -  qux",
			wantSingleline: "foo; bar; baz; qux",
		},
		{
			giveErrors: []error{
				errors.New("foo"),
				nil,
				newMultiErr(
					errors.New("bar"),
				),
				nil,
			},
			wantError: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  foo\n" +
				" -  bar",
			wantSingleline: "foo; bar",
		},
		{
			giveErrors: []error{
				errors.New("foo"),
				newMultiErr(
					errors.New("bar"),
				),
			},
			wantError: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  foo\n" +
				" -  bar",
			wantSingleline: "foo; bar",
		},
		{
			giveErrors: []error{
				errors.New("foo"),
				richFormatError{},
				errors.New("bar"),
			},
			wantError: newMultiErr(
				errors.New("foo"),
				richFormatError{},
				errors.New("bar"),
			),
			wantMultiline: "the following errors occurred:\n" +
				" -  foo\n" +
				" -  multiline\n" +
				"    message\n" +
				"    with plus\n" +
				" -  bar",
			wantSingleline: "foo; without plus; bar",
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			err := Combine(tt.giveErrors...)
			require.Equal(t, tt.wantError, err)

			if tt.wantMultiline != "" {
				t.Run("Sprintf/multiline", func(t *testing.T) {
					assert.Equal(t, tt.wantMultiline, fmt.Sprintf("%+v", err))
				})
			}

			if tt.wantSingleline != "" {
				t.Run("Sprintf/singleline", func(t *testing.T) {
					assert.Equal(t, tt.wantSingleline, fmt.Sprintf("%v", err))
				})

				t.Run("Error()", func(t *testing.T) {
					assert.Equal(t, tt.wantSingleline, err.Error())
				})

				if s, ok := err.(fmt.Stringer); ok {
					t.Run("String()", func(t *testing.T) {
						assert.Equal(t, tt.wantSingleline, s.String())
					})
				}
			}
		})
	}
}

func TestCombineDoesNotModifySlice(t *testing.T) {
	errors := []error{
		errors.New("foo"),
		nil,
		errors.New("bar"),
	}

	assert.NotNil(t, Combine(errors...))
	assert.Len(t, errors, 3)
	assert.Nil(t, errors[1], 3)
}

func TestAppend(t *testing.T) {
	tests := []struct {
		left  error
		right error
		want  error
	}{
		{
			left:  nil,
			right: nil,
			want:  nil,
		},
		{
			left:  nil,
			right: errors.New("great sadness"),
			want:  errors.New("great sadness"),
		},
		{
			left:  errors.New("great sadness"),
			right: nil,
			want:  errors.New("great sadness"),
		},
		{
			left:  errors.New("foo"),
			right: errors.New("bar"),
			want: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			left: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
			right: errors.New("baz"),
			want: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
				errors.New("baz"),
			),
		},
		{
			left: errors.New("baz"),
			right: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
			want: newMultiErr(
				errors.New("baz"),
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			left: newMultiErr(
				errors.New("foo"),
			),
			right: newMultiErr(
				errors.New("bar"),
			),
			want: newMultiErr(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, Append(tt.left, tt.right))
	}
}

type notMultiErr struct{}

var _ errorGroup = notMultiErr{}

func (notMultiErr) Error() string {
	return "great sadness"
}

func (notMultiErr) Errors() []error {
	return []error{errors.New("great sadness")}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		give error
		want []error

		// Don't attempt to cast to errorGroup or *multiError
		dontCast bool
	}{
		{dontCast: true}, // nil
		{
			give:     errors.New("hi"),
			want:     []error{errors.New("hi")},
			dontCast: true,
		},
		{
			// We don't yet support non-multierr errors.
			give:     notMultiErr{},
			want:     []error{notMultiErr{}},
			dontCast: true,
		},
		{
			give: Combine(
				errors.New("foo"),
				errors.New("bar"),
			),
			want: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
		},
		{
			give: Append(
				errors.New("foo"),
				errors.New("bar"),
			),
			want: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
		},
		{
			give: Append(
				errors.New("foo"),
				Combine(
					errors.New("bar"),
				),
			),
			want: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
		},
		{
			give: Combine(
				errors.New("foo"),
				Append(
					errors.New("bar"),
					errors.New("baz"),
				),
				errors.New("qux"),
			),
			want: []error{
				errors.New("foo"),
				errors.New("bar"),
				errors.New("baz"),
				errors.New("qux"),
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Run("Errors()", func(t *testing.T) {
				require.Equal(t, tt.want, Errors(tt.give))
			})

			if tt.dontCast {
				return
			}

			t.Run("multiError", func(t *testing.T) {
				require.Equal(t, tt.want, tt.give.(*multiError).Errors())
			})

			t.Run("errorGroup", func(t *testing.T) {
				require.Equal(t, tt.want, tt.give.(errorGroup).Errors())
			})
		})
	}
}

func createMultiErrWithCapacity() error {
	// Create a multiError that has capacity for more errors so Append will
	// modify the underlying array that may be shared.
	return appendN(nil, errors.New("append"), 50)
}

func TestAppendDoesNotModify(t *testing.T) {
	initial := createMultiErrWithCapacity()
	err1 := Append(initial, errors.New("err1"))
	err2 := Append(initial, errors.New("err2"))

	// Make sure the error messages match, since we do modify the copyNeeded
	// atomic, the values cannot be compared.
	assert.EqualError(t, initial, createMultiErrWithCapacity().Error(), "Initial should not be modified")

	assert.EqualError(t, err1, Append(createMultiErrWithCapacity(), errors.New("err1")).Error())
	assert.EqualError(t, err2, Append(createMultiErrWithCapacity(), errors.New("err2")).Error())
}

func TestAppendRace(t *testing.T) {
	initial := createMultiErrWithCapacity()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := initial
			for j := 0; j < 10; j++ {
				err = Append(err, errors.New("err"))
			}
		}()
	}

	wg.Wait()
}

func TestErrorsSliceIsImmutable(t *testing.T) {
	err1 := errors.New("err1")
	err2 := errors.New("err2")

	err := Append(err1, err2)
	gotErrors := Errors(err)
	require.Equal(t, []error{err1, err2}, gotErrors, "errors must match")

	gotErrors[0] = nil
	gotErrors[1] = errors.New("err3")

	require.Equal(t, []error{err1, err2}, Errors(err),
		"errors must match after modification")
}

func TestNilMultierror(t *testing.T) {
	// For safety, all operations on multiError should be safe even if it is
	// nil.
	var err *multiError

	require.Empty(t, err.Error())
	require.Empty(t, err.Errors())
}
