// Copyright (c) 2014 The sortutil Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package strutil collects utils supplemental to the standard strings package.
package strutil

import (
	"bytes"
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Base32ExtDecode decodes base32 extended (RFC 4648) text to binary data.
func Base32ExtDecode(text []byte) (data []byte, err error) {
	n := base32.HexEncoding.DecodedLen(len(text))
	data = make([]byte, n)
	decoder := base32.NewDecoder(base32.HexEncoding, bytes.NewBuffer(text))
	if n, err = decoder.Read(data); err != nil {
		n = 0
	}
	data = data[:n]
	return
}

// Base32ExtEncode encodes binary data to base32 extended (RFC 4648) encoded text.
func Base32ExtEncode(data []byte) (text []byte) {
	n := base32.HexEncoding.EncodedLen(len(data))
	buf := bytes.NewBuffer(make([]byte, 0, n))
	encoder := base32.NewEncoder(base32.HexEncoding, buf)
	encoder.Write(data)
	encoder.Close()
	if buf.Len() != n {
		panic("internal error")
	}
	return buf.Bytes()
}

// Base64Decode decodes base64 text to binary data.
func Base64Decode(text []byte) (data []byte, err error) {
	n := base64.StdEncoding.DecodedLen(len(text))
	data = make([]byte, n)
	decoder := base64.NewDecoder(base64.StdEncoding, bytes.NewBuffer(text))
	if n, err = decoder.Read(data); err != nil {
		n = 0
	}
	data = data[:n]
	return
}

// Base64Encode encodes binary data to base64 encoded text.
func Base64Encode(data []byte) (text []byte) {
	n := base64.StdEncoding.EncodedLen(len(data))
	buf := bytes.NewBuffer(make([]byte, 0, n))
	encoder := base64.NewEncoder(base64.StdEncoding, buf)
	encoder.Write(data)
	encoder.Close()
	if buf.Len() != n {
		panic("internal error")
	}
	return buf.Bytes()
}

// Formatter is an io.Writer extended by a fmt.Printf like function Format
type Formatter interface {
	io.Writer
	Format(format string, args ...interface{}) (n int, errno error)
}

type indentFormatter struct {
	io.Writer
	indent      []byte
	indentLevel int
	state       int
}

const (
	st0 = iota
	stBOL
	stPERC
	stBOLPERC
)

// IndentFormatter returns a new Formatter which interprets %i and %u in the
// Format() format string as indent and undent commands. The commands can
// nest. The Formatter writes to io.Writer 'w' and inserts one 'indent'
// string per current indent level value.
// Behaviour of commands reaching negative indent levels is undefined.
//	IndentFormatter(os.Stdout, "\t").Format("abc%d%%e%i\nx\ny\n%uz\n", 3)
// output:
//	abc3%e
//		x
//		y
//	z
// The Go quoted string literal form of the above is:
//	"abc%%e\n\tx\n\tx\nz\n"
// The commands can be scattered between separate invocations of Format(),
// i.e. the formatter keeps track of the indent level and knows if it is
// positioned on start of a line and should emit indentation(s).
// The same output as above can be produced by e.g.:
//	f := IndentFormatter(os.Stdout, " ")
//	f.Format("abc%d%%e%i\nx\n", 3)
//	f.Format("y\n%uz\n")
func IndentFormatter(w io.Writer, indent string) Formatter {
	return &indentFormatter{w, []byte(indent), 0, stBOL}
}

func (f *indentFormatter) format(flat bool, format string, args ...interface{}) (n int, errno error) {
	buf := []byte{}
	for i := 0; i < len(format); i++ {
		c := format[i]
		switch f.state {
		case st0:
			switch c {
			case '\n':
				cc := c
				if flat && f.indentLevel != 0 {
					cc = ' '
				}
				buf = append(buf, cc)
				f.state = stBOL
			case '%':
				f.state = stPERC
			default:
				buf = append(buf, c)
			}
		case stBOL:
			switch c {
			case '\n':
				cc := c
				if flat && f.indentLevel != 0 {
					cc = ' '
				}
				buf = append(buf, cc)
			case '%':
				f.state = stBOLPERC
			default:
				if !flat {
					for i := 0; i < f.indentLevel; i++ {
						buf = append(buf, f.indent...)
					}
				}
				buf = append(buf, c)
				f.state = st0
			}
		case stBOLPERC:
			switch c {
			case 'i':
				f.indentLevel++
				f.state = stBOL
			case 'u':
				f.indentLevel--
				f.state = stBOL
			default:
				if !flat {
					for i := 0; i < f.indentLevel; i++ {
						buf = append(buf, f.indent...)
					}
				}
				buf = append(buf, '%', c)
				f.state = st0
			}
		case stPERC:
			switch c {
			case 'i':
				f.indentLevel++
				f.state = st0
			case 'u':
				f.indentLevel--
				f.state = st0
			default:
				buf = append(buf, '%', c)
				f.state = st0
			}
		default:
			panic("unexpected state")
		}
	}
	switch f.state {
	case stPERC, stBOLPERC:
		buf = append(buf, '%')
	}
	return f.Write([]byte(fmt.Sprintf(string(buf), args...)))
}

func (f *indentFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	return f.format(false, format, args...)
}

type flatFormatter indentFormatter

// FlatFormatter returns a newly created Formatter with the same functionality as the one returned
// by IndentFormatter except it allows a newline in the 'format' string argument of Format
// to pass through iff indent level is currently zero.
//
// If indent level is non-zero then such new lines are changed to a space character.
// There is no indent string, the %i and %u format verbs are used solely to determine the indent level.
//
// The FlatFormatter is intended for flattening of normally nested structure textual representation to
// a one top level structure per line form.
//	FlatFormatter(os.Stdout, " ").Format("abc%d%%e%i\nx\ny\n%uz\n", 3)
// output in the form of a Go quoted string literal:
//	"abc3%%e x y z\n"
func FlatFormatter(w io.Writer) Formatter {
	return (*flatFormatter)(IndentFormatter(w, "").(*indentFormatter))
}

func (f *flatFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	return (*indentFormatter)(f).format(true, format, args...)
}

// Pool handles aligning of strings having equal values to the same string instance.
// Intended use is to conserve some memory e.g. where a large number of identically valued strings
// with non identical backing arrays may exists in several semantically distinct instances of some structs.
// Pool is *not* concurrent access safe. It doesn't handle common prefix/suffix aligning,
// e.g. having s1 == "abc" and s2 == "bc", s2 is not automatically aligned as s1[1:].
type Pool struct {
	pool map[string]string
}

// NewPool returns a newly created Pool.
func NewPool() *Pool {
	return &Pool{map[string]string{}}
}

// Align returns a string with the same value as its argument. It guarantees that
// all aligned strings share a single instance in memory.
func (p *Pool) Align(s string) string {
	if a, ok := p.pool[s]; ok {
		return a
	}

	s = StrPack(s)
	p.pool[s] = s
	return s
}

// Count returns the number of items in the pool.
func (p *Pool) Count() int {
	return len(p.pool)
}

// GoPool is a concurrent access safe version of Pool.
type GoPool struct {
	pool map[string]string
	rwm  *sync.RWMutex
}

// NewGoPool returns a newly created GoPool.
func NewGoPool() (p *GoPool) {
	return &GoPool{map[string]string{}, &sync.RWMutex{}}
}

// Align returns a string with the same value as its argument. It guarantees that
// all aligned strings share a single instance in memory.
func (p *GoPool) Align(s string) (y string) {
	if s != "" {
		p.rwm.RLock()               // R++
		if a, ok := p.pool[s]; ok { // found
			p.rwm.RUnlock() // R--
			return a
		}

		p.rwm.RUnlock() // R--
		// not found but with a race condition, retry within a write lock
		p.rwm.Lock()                // W++
		defer p.rwm.Unlock()        // W--
		if a, ok := p.pool[s]; ok { // done in a race
			return a
		}

		// we won
		s = StrPack(s)
		p.pool[s] = s
		return s
	}

	return
}

// Count returns the number of items in the pool.
func (p *GoPool) Count() int {
	return len(p.pool)
}

// Dict is a string <-> id bijection. Dict is *not* concurrent access safe for assigning new ids
// to strings not yet contained in the bijection.
// Id for an empty string is guaranteed to be 0,
// thus Id for any non empty string is guaranteed to be non zero.
type Dict struct {
	si map[string]int
	is []string
}

// NewDict returns a newly created Dict.
func NewDict() (d *Dict) {
	d = &Dict{map[string]int{}, []string{}}
	d.Id("")
	return
}

// Count returns the number of items in the dict.
func (d *Dict) Count() int {
	return len(d.is)
}

// Id maps string s to its numeric identificator.
func (d *Dict) Id(s string) (y int) {
	if y, ok := d.si[s]; ok {
		return y
	}

	s = StrPack(s)
	y = len(d.is)
	d.si[s] = y
	d.is = append(d.is, s)
	return
}

// S maps an id to its string value and ok == true. Id values not contained in the bijection
// return "", false.
func (d *Dict) S(id int) (s string, ok bool) {
	if id >= len(d.is) {
		return "", false
	}
	return d.is[id], true
}

// GoDict is a concurrent access safe version of Dict.
type GoDict struct {
	si  map[string]int
	is  []string
	rwm *sync.RWMutex
}

// NewGoDict returns a newly created GoDict.
func NewGoDict() (d *GoDict) {
	d = &GoDict{map[string]int{}, []string{}, &sync.RWMutex{}}
	d.Id("")
	return
}

// Count returns the number of items in the dict.
func (d *GoDict) Count() int {
	return len(d.is)
}

// Id maps string s to its numeric identificator. The implementation honors getting
// an existing id at the cost of assigning a new one.
func (d *GoDict) Id(s string) (y int) {
	d.rwm.RLock()             // R++
	if y, ok := d.si[s]; ok { // found
		d.rwm.RUnlock() // R--
		return y
	}

	d.rwm.RUnlock() // R--

	// not found but with a race condition
	d.rwm.Lock()              // W++ recheck with write lock
	defer d.rwm.Unlock()      // W--
	if y, ok := d.si[s]; ok { // some other goroutine won already
		return y
	}

	// a race free not found state => insert the string
	s = StrPack(s)
	y = len(d.is)
	d.si[s] = y
	d.is = append(d.is, s)
	return
}

// S maps an id to its string value and ok == true. Id values not contained in the bijection
// return "", false.
func (d *GoDict) S(id int) (s string, ok bool) {
	d.rwm.RLock()         // R++
	defer d.rwm.RUnlock() // R--
	if id >= len(d.is) {
		return "", false
	}
	return d.is[id], true
}

// StrPack returns a new instance of s which is tightly packed in memory.
// It is intended for avoiding the situation where having a live reference
// to a string slice over an unreferenced biger underlying string keeps the biger one
// in memory anyway - it can't be GCed.
func StrPack(s string) string {
	return string([]byte(s)) // T(U(T)) intentional.
}

// JoinFields returns strings in flds joined by sep. Flds may contain arbitrary
// bytes, including the sep as they are safely escaped. JoinFields panics if
// sep is the backslash character or if len(sep) != 1.
func JoinFields(flds []string, sep string) string {
	if len(sep) != 1 || sep == "\\" {
		panic("invalid separator")
	}

	a := make([]string, len(flds))
	for i, v := range flds {
		v = strings.Replace(v, "\\", "\\0", -1)
		a[i] = strings.Replace(v, sep, "\\1", -1)
	}
	return strings.Join(a, sep)
}

// SplitFields splits s, which must be produced by JoinFields using the same
// sep, into flds.  SplitFields panics if sep is the backslash character or if
// len(sep) != 1.
func SplitFields(s, sep string) (flds []string) {
	if len(sep) != 1 || sep == "\\" {
		panic("invalid separator")
	}

	a := strings.Split(s, sep)
	r := make([]string, len(a))
	for i, v := range a {
		v = strings.Replace(v, "\\1", sep, -1)
		r[i] = strings.Replace(v, "\\0", "\\", -1)
	}
	return r
}

// PrettyPrintHooks allow to customize the result of PrettyPrint for types
// listed in the map value.
type PrettyPrintHooks map[reflect.Type]func(f Formatter, v interface{}, prefix, suffix string)

// PrettyString returns the output of PrettyPrint as a string.
func PrettyString(v interface{}, prefix, suffix string, hooks PrettyPrintHooks) string {
	var b bytes.Buffer
	PrettyPrint(&b, v, prefix, suffix, hooks)
	return b.String()
}

// PrettyPrint pretty prints v to w. Zero values and unexported struct fields
// are omitted.
func PrettyPrint(w io.Writer, v interface{}, prefix, suffix string, hooks PrettyPrintHooks) {
	if v == nil {
		return
	}

	f := IndentFormatter(w, "· ")

	defer func() {
		if e := recover(); e != nil {
			f.Format("\npanic: %v", e)
		}
	}()

	prettyPrint(nil, f, prefix, suffix, v, hooks)
}

func prettyPrint(protect map[interface{}]struct{}, sf Formatter, prefix, suffix string, v interface{}, hooks PrettyPrintHooks) {
	if v == nil {
		return
	}

	rt := reflect.TypeOf(v)
	if handler := hooks[rt]; handler != nil {
		handler(sf, v, prefix, suffix)
		return
	}

	rv := reflect.ValueOf(v)
	switch rt.Kind() {
	case reflect.Slice:
		if rv.Len() == 0 {
			return
		}

		sf.Format("%s[]%T{ // len %d%i\n", prefix, rv.Index(0).Interface(), rv.Len())
		for i := 0; i < rv.Len(); i++ {
			prettyPrint(protect, sf, fmt.Sprintf("%d: ", i), ",\n", rv.Index(i).Interface(), hooks)
		}
		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%u}" + suffix)
	case reflect.Array:
		if reflect.Zero(rt).Interface() == rv.Interface() {
			return
		}

		sf.Format("%s[%d]%T{%i\n", prefix, rv.Len(), rv.Index(0).Interface())
		for i := 0; i < rv.Len(); i++ {
			prettyPrint(protect, sf, fmt.Sprintf("%d: ", i), ",\n", rv.Index(i).Interface(), hooks)
		}
		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%u}" + suffix)
	case reflect.Struct:
		if rt.NumField() == 0 {
			return
		}

		if reflect.DeepEqual(reflect.Zero(rt).Interface(), rv.Interface()) {
			return
		}

		sf.Format("%s%T{%i\n", prefix, v)
		for i := 0; i < rt.NumField(); i++ {
			f := rv.Field(i)
			if !f.CanInterface() {
				continue
			}

			prettyPrint(protect, sf, fmt.Sprintf("%s: ", rt.Field(i).Name), ",\n", f.Interface(), hooks)
		}
		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%u}" + suffix)
	case reflect.Ptr:
		if rv.IsNil() {
			return
		}

		rvi := rv.Interface()
		if _, ok := protect[rvi]; ok {
			suffix = strings.Replace(suffix, "%", "%%", -1)
			sf.Format("%s&%T{ /* recursive/repetitive pointee not shown */ }"+suffix, prefix, rv.Elem().Interface())
			return
		}

		if protect == nil {
			protect = map[interface{}]struct{}{}
		}
		protect[rvi] = struct{}{}
		prettyPrint(protect, sf, prefix+"&", suffix, rv.Elem().Interface(), hooks)
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		if v := rv.Int(); v != 0 {
			suffix = strings.Replace(suffix, "%", "%%", -1)
			sf.Format("%s%v"+suffix, prefix, v)
		}
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		if v := rv.Uint(); v != 0 {
			suffix = strings.Replace(suffix, "%", "%%", -1)
			sf.Format("%s%v"+suffix, prefix, v)
		}
	case reflect.Float32, reflect.Float64:
		if v := rv.Float(); v != 0 {
			suffix = strings.Replace(suffix, "%", "%%", -1)
			sf.Format("%s%v"+suffix, prefix, v)
		}
	case reflect.Complex64, reflect.Complex128:
		if v := rv.Complex(); v != 0 {
			suffix = strings.Replace(suffix, "%", "%%", -1)
			sf.Format("%s%v"+suffix, prefix, v)
		}
	case reflect.Uintptr:
		if v := rv.Uint(); v != 0 {
			suffix = strings.Replace(suffix, "%", "%%", -1)
			sf.Format("%s%v"+suffix, prefix, v)
		}
	case reflect.UnsafePointer:
		s := fmt.Sprintf("%p", rv.Interface())
		if s == "0x0" {
			return
		}

		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%s%s"+suffix, prefix, s)
	case reflect.Bool:
		if v := rv.Bool(); v {
			suffix = strings.Replace(suffix, "%", "%%", -1)
			sf.Format("%s%v"+suffix, prefix, rv.Bool())
		}
	case reflect.String:
		s := rv.Interface().(string)
		if s == "" {
			return
		}

		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%s%q"+suffix, prefix, s)
	case reflect.Chan:
		if reflect.Zero(rt).Interface() == rv.Interface() {
			return
		}

		c := rv.Cap()
		s := ""
		if c != 0 {
			s = fmt.Sprintf("// capacity: %d", c)
		}
		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%s%s %s%s"+suffix, prefix, rt.ChanDir(), rt.Elem().Name(), s)
	case reflect.Func:
		if rv.IsNil() {
			return
		}

		var in, out []string
		for i := 0; i < rt.NumIn(); i++ {
			x := reflect.Zero(rt.In(i))
			in = append(in, fmt.Sprintf("%T", x.Interface()))
		}
		if rt.IsVariadic() {
			i := len(in) - 1
			in[i] = "..." + in[i][2:]
		}
		for i := 0; i < rt.NumOut(); i++ {
			out = append(out, rt.Out(i).Name())
		}
		s := "(" + strings.Join(in, ", ") + ")"
		t := strings.Join(out, ", ")
		if len(out) > 1 {
			t = "(" + t + ")"
		}
		if t != "" {
			t = " " + t
		}
		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%sfunc%s%s { ... }"+suffix, prefix, s, t)
	case reflect.Map:
		keys := rv.MapKeys()
		if len(keys) == 0 {
			return
		}

		var buf bytes.Buffer
		nf := IndentFormatter(&buf, "· ")
		var skeys []string
		for i, k := range keys {
			prettyPrint(protect, nf, "", "", k.Interface(), hooks)
			skeys = append(skeys, fmt.Sprintf("%s%10d", buf.Bytes(), i))
			buf.Reset()
		}
		sort.Strings(skeys)
		sf.Format("%s%T{%i\n", prefix, v)
		for _, k := range skeys {
			si := strings.TrimSpace(k[len(k)-10:])
			k = k[:len(k)-10]
			n, _ := strconv.ParseUint(si, 10, 64)
			mv := rv.MapIndex(keys[n])
			prettyPrint(protect, sf, fmt.Sprintf("%s: ", k), ",\n", mv.Interface(), hooks)
		}
		suffix = strings.Replace(suffix, "%", "%%", -1)
		sf.Format("%u}" + suffix)
	}
}

// Gopath returns the value of the $GOPATH environment variable or its default
// value if not set.
func Gopath() string {
	if r := os.Getenv("GOPATH"); r != "" {
		return r
	}

	// go1.8: https://github.com/golang/go/blob/74628a8b9f102bddd5078ee426efe0fd57033115/doc/code.html#L122
	switch runtime.GOOS {
	case "plan9":
		return os.Getenv("home")
	case "windows":
		return filepath.Join(os.Getenv("USERPROFILE"), "go")
	default:
		return filepath.Join(os.Getenv("HOME"), "go")
	}
}

// Homepath returns the user's home directory path.
func Homepath() string {
	// go1.8: https://github.com/golang/go/blob/74628a8b9f102bddd5078ee426efe0fd57033115/doc/code.html#L122
	switch runtime.GOOS {
	case "plan9":
		return os.Getenv("home")
	case "windows":
		return os.Getenv("USERPROFILE")
	default:
		return os.Getenv("HOME")
	}
}

// ImportPath returns the import path of the caller or an error, if any.
func ImportPath() (string, error) {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}

	gopath := Gopath()
	for _, v := range filepath.SplitList(gopath) {
		gp := filepath.Join(v, "src")
		path, err := filepath.Rel(gp, file)
		if err != nil {
			continue
		}

		return filepath.Dir(path), nil
	}

	return "", fmt.Errorf("cannot determine import path using GOPATH=%s", gopath)
}
