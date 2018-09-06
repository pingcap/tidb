// Copyright (c) 2012-2018 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

import (
	"encoding"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// Some tagging information for error messages.
const (
	msgBadDesc            = "unrecognized descriptor byte"
	msgDecCannotExpandArr = "cannot expand go array from %v to stream length: %v"
)

const decDefSliceCap = 8
const decDefChanCap = 64 // should be large, as cap cannot be expanded
const decScratchByteArrayLen = cacheLineSize - 8

var (
	errstrOnlyMapOrArrayCanDecodeIntoStruct = "only encoded map or array can be decoded into a struct"
	errstrCannotDecodeIntoNil               = "cannot decode into nil"

	errmsgExpandSliceOverflow     = "expand slice: slice overflow"
	errmsgExpandSliceCannotChange = "expand slice: cannot change"

	errDecoderNotInitialized = errors.New("Decoder not initialized")

	errDecUnreadByteNothingToRead   = errors.New("cannot unread - nothing has been read")
	errDecUnreadByteLastByteNotRead = errors.New("cannot unread - last byte has not been read")
	errDecUnreadByteUnknown         = errors.New("cannot unread - reason unknown")
)

// decReader abstracts the reading source, allowing implementations that can
// read from an io.Reader or directly off a byte slice with zero-copying.
type decReader interface {
	unreadn1()

	// readx will use the implementation scratch buffer if possible i.e. n < len(scratchbuf), OR
	// just return a view of the []byte being decoded from.
	// Ensure you call detachZeroCopyBytes later if this needs to be sent outside codec control.
	readx(n int) []byte
	readb([]byte)
	readn1() uint8
	numread() int // number of bytes read
	track()
	stopTrack() []byte

	// skip will skip any byte that matches, and return the first non-matching byte
	skip(accept *bitset256) (token byte)
	// readTo will read any byte that matches, stopping once no-longer matching.
	readTo(in []byte, accept *bitset256) (out []byte)
	// readUntil will read, only stopping once it matches the 'stop' byte.
	readUntil(in []byte, stop byte) (out []byte)
}

type decDriver interface {
	// this will check if the next token is a break.
	CheckBreak() bool
	// Note: TryDecodeAsNil should be careful not to share any temporary []byte with
	// the rest of the decDriver. This is because sometimes, we optimize by holding onto
	// a transient []byte, and ensuring the only other call we make to the decDriver
	// during that time is maybe a TryDecodeAsNil() call.
	TryDecodeAsNil() bool
	// vt is one of: Bytes, String, Nil, Slice or Map. Return unSet if not known.
	ContainerType() (vt valueType)
	// IsBuiltinType(rt uintptr) bool

	// DecodeNaked will decode primitives (number, bool, string, []byte) and RawExt.
	// For maps and arrays, it will not do the decoding in-band, but will signal
	// the decoder, so that is done later, by setting the decNaked.valueType field.
	//
	// Note: Numbers are decoded as int64, uint64, float64 only (no smaller sized number types).
	// for extensions, DecodeNaked must read the tag and the []byte if it exists.
	// if the []byte is not read, then kInterfaceNaked will treat it as a Handle
	// that stores the subsequent value in-band, and complete reading the RawExt.
	//
	// extensions should also use readx to decode them, for efficiency.
	// kInterface will extract the detached byte slice if it has to pass it outside its realm.
	DecodeNaked()

	// Deprecated: use DecodeInt64 and DecodeUint64 instead
	// DecodeInt(bitsize uint8) (i int64)
	// DecodeUint(bitsize uint8) (ui uint64)

	DecodeInt64() (i int64)
	DecodeUint64() (ui uint64)

	DecodeFloat64() (f float64)
	DecodeBool() (b bool)
	// DecodeString can also decode symbols.
	// It looks redundant as DecodeBytes is available.
	// However, some codecs (e.g. binc) support symbols and can
	// return a pre-stored string value, meaning that it can bypass
	// the cost of []byte->string conversion.
	DecodeString() (s string)
	DecodeStringAsBytes() (v []byte)

	// DecodeBytes may be called directly, without going through reflection.
	// Consequently, it must be designed to handle possible nil.
	DecodeBytes(bs []byte, zerocopy bool) (bsOut []byte)
	// DecodeBytes(bs []byte, isstring, zerocopy bool) (bsOut []byte)

	// decodeExt will decode into a *RawExt or into an extension.
	DecodeExt(v interface{}, xtag uint64, ext Ext) (realxtag uint64)
	// decodeExt(verifyTag bool, tag byte) (xtag byte, xbs []byte)

	DecodeTime() (t time.Time)

	ReadArrayStart() int
	ReadArrayElem()
	ReadArrayEnd()
	ReadMapStart() int
	ReadMapElemKey()
	ReadMapElemValue()
	ReadMapEnd()

	reset()
	uncacheRead()
}

type decDriverNoopContainerReader struct{}

func (x decDriverNoopContainerReader) ReadArrayStart() (v int) { return }
func (x decDriverNoopContainerReader) ReadArrayElem()          {}
func (x decDriverNoopContainerReader) ReadArrayEnd()           {}
func (x decDriverNoopContainerReader) ReadMapStart() (v int)   { return }
func (x decDriverNoopContainerReader) ReadMapElemKey()         {}
func (x decDriverNoopContainerReader) ReadMapElemValue()       {}
func (x decDriverNoopContainerReader) ReadMapEnd()             {}
func (x decDriverNoopContainerReader) CheckBreak() (v bool)    { return }

// func (x decNoSeparator) uncacheRead() {}

// DecodeOptions captures configuration options during decode.
type DecodeOptions struct {
	// MapType specifies type to use during schema-less decoding of a map in the stream.
	// If nil (unset), we default to map[string]interface{} iff json handle and MapStringAsKey=true,
	// else map[interface{}]interface{}.
	MapType reflect.Type

	// SliceType specifies type to use during schema-less decoding of an array in the stream.
	// If nil (unset), we default to []interface{} for all formats.
	SliceType reflect.Type

	// MaxInitLen defines the maxinum initial length that we "make" a collection
	// (string, slice, map, chan). If 0 or negative, we default to a sensible value
	// based on the size of an element in the collection.
	//
	// For example, when decoding, a stream may say that it has 2^64 elements.
	// We should not auto-matically provision a slice of that size, to prevent Out-Of-Memory crash.
	// Instead, we provision up to MaxInitLen, fill that up, and start appending after that.
	MaxInitLen int

	// ReaderBufferSize is the size of the buffer used when reading.
	//
	// if > 0, we use a smart buffer internally for performance purposes.
	ReaderBufferSize int

	// If ErrorIfNoField, return an error when decoding a map
	// from a codec stream into a struct, and no matching struct field is found.
	ErrorIfNoField bool

	// If ErrorIfNoArrayExpand, return an error when decoding a slice/array that cannot be expanded.
	// For example, the stream contains an array of 8 items, but you are decoding into a [4]T array,
	// or you are decoding into a slice of length 4 which is non-addressable (and so cannot be set).
	ErrorIfNoArrayExpand bool

	// If SignedInteger, use the int64 during schema-less decoding of unsigned values (not uint64).
	SignedInteger bool

	// MapValueReset controls how we decode into a map value.
	//
	// By default, we MAY retrieve the mapping for a key, and then decode into that.
	// However, especially with big maps, that retrieval may be expensive and unnecessary
	// if the stream already contains all that is necessary to recreate the value.
	//
	// If true, we will never retrieve the previous mapping,
	// but rather decode into a new value and set that in the map.
	//
	// If false, we will retrieve the previous mapping if necessary e.g.
	// the previous mapping is a pointer, or is a struct or array with pre-set state,
	// or is an interface.
	MapValueReset bool

	// SliceElementReset: on decoding a slice, reset the element to a zero value first.
	//
	// concern: if the slice already contained some garbage, we will decode into that garbage.
	SliceElementReset bool

	// InterfaceReset controls how we decode into an interface.
	//
	// By default, when we see a field that is an interface{...},
	// or a map with interface{...} value, we will attempt decoding into the
	// "contained" value.
	//
	// However, this prevents us from reading a string into an interface{}
	// that formerly contained a number.
	//
	// If true, we will decode into a new "blank" value, and set that in the interface.
	// If false, we will decode into whatever is contained in the interface.
	InterfaceReset bool

	// InternString controls interning of strings during decoding.
	//
	// Some handles, e.g. json, typically will read map keys as strings.
	// If the set of keys are finite, it may help reduce allocation to
	// look them up from a map (than to allocate them afresh).
	//
	// Note: Handles will be smart when using the intern functionality.
	// Every string should not be interned.
	// An excellent use-case for interning is struct field names,
	// or map keys where key type is string.
	InternString bool

	// PreferArrayOverSlice controls whether to decode to an array or a slice.
	//
	// This only impacts decoding into a nil interface{}.
	// Consequently, it has no effect on codecgen.
	//
	// *Note*: This only applies if using go1.5 and above,
	// as it requires reflect.ArrayOf support which was absent before go1.5.
	PreferArrayOverSlice bool

	// DeleteOnNilMapValue controls how to decode a nil value in the stream.
	//
	// If true, we will delete the mapping of the key.
	// Else, just set the mapping to the zero value of the type.
	DeleteOnNilMapValue bool
}

// ------------------------------------

type bufioDecReader struct {
	buf []byte
	r   io.Reader

	c   int // cursor
	n   int // num read
	err error

	tr  []byte
	trb bool
	b   [4]byte
}

func (z *bufioDecReader) reset(r io.Reader) {
	z.r, z.c, z.n, z.err, z.trb = r, 0, 0, nil, false
	if z.tr != nil {
		z.tr = z.tr[:0]
	}
}

func (z *bufioDecReader) Read(p []byte) (n int, err error) {
	if z.err != nil {
		return 0, z.err
	}
	p0 := p
	n = copy(p, z.buf[z.c:])
	z.c += n
	if z.c == len(z.buf) {
		z.c = 0
	}
	z.n += n
	if len(p) == n {
		if z.c == 0 {
			z.buf = z.buf[:1]
			z.buf[0] = p[len(p)-1]
			z.c = 1
		}
		if z.trb {
			z.tr = append(z.tr, p0[:n]...)
		}
		return
	}
	p = p[n:]
	var n2 int
	// if we are here, then z.buf is all read
	if len(p) > len(z.buf) {
		n2, err = decReadFull(z.r, p)
		n += n2
		z.n += n2
		z.err = err
		// don't return EOF if some bytes were read. keep for next time.
		if n > 0 && err == io.EOF {
			err = nil
		}
		// always keep last byte in z.buf
		z.buf = z.buf[:1]
		z.buf[0] = p[len(p)-1]
		z.c = 1
		if z.trb {
			z.tr = append(z.tr, p0[:n]...)
		}
		return
	}
	// z.c is now 0, and len(p) <= len(z.buf)
	for len(p) > 0 && z.err == nil {
		// println("len(p) loop starting ... ")
		z.c = 0
		z.buf = z.buf[0:cap(z.buf)]
		n2, err = z.r.Read(z.buf)
		if n2 > 0 {
			if err == io.EOF {
				err = nil
			}
			z.buf = z.buf[:n2]
			n2 = copy(p, z.buf)
			z.c = n2
			n += n2
			z.n += n2
			p = p[n2:]
		}
		z.err = err
		// println("... len(p) loop done")
	}
	if z.c == 0 {
		z.buf = z.buf[:1]
		z.buf[0] = p[len(p)-1]
		z.c = 1
	}
	if z.trb {
		z.tr = append(z.tr, p0[:n]...)
	}
	return
}

func (z *bufioDecReader) ReadByte() (b byte, err error) {
	z.b[0] = 0
	_, err = z.Read(z.b[:1])
	b = z.b[0]
	return
}

func (z *bufioDecReader) UnreadByte() (err error) {
	if z.err != nil {
		return z.err
	}
	if z.c > 0 {
		z.c--
		z.n--
		if z.trb {
			z.tr = z.tr[:len(z.tr)-1]
		}
		return
	}
	return errDecUnreadByteNothingToRead
}

func (z *bufioDecReader) numread() int {
	return z.n
}

func (z *bufioDecReader) readx(n int) (bs []byte) {
	if n <= 0 || z.err != nil {
		return
	}
	if z.c+n <= len(z.buf) {
		bs = z.buf[z.c : z.c+n]
		z.n += n
		z.c += n
		if z.trb {
			z.tr = append(z.tr, bs...)
		}
		return
	}
	bs = make([]byte, n)
	_, err := z.Read(bs)
	if err != nil {
		panic(err)
	}
	return
}

func (z *bufioDecReader) readb(bs []byte) {
	_, err := z.Read(bs)
	if err != nil {
		panic(err)
	}
}

// func (z *bufioDecReader) readn1eof() (b uint8, eof bool) {
// 	b, err := z.ReadByte()
// 	if err != nil {
// 		if err == io.EOF {
// 			eof = true
// 		} else {
// 			panic(err)
// 		}
// 	}
// 	return
// }

func (z *bufioDecReader) readn1() (b uint8) {
	b, err := z.ReadByte()
	if err != nil {
		panic(err)
	}
	return
}

func (z *bufioDecReader) search(in []byte, accept *bitset256, stop, flag uint8) (token byte, out []byte) {
	// flag: 1 (skip), 2 (readTo), 4 (readUntil)
	if flag == 4 {
		for i := z.c; i < len(z.buf); i++ {
			if z.buf[i] == stop {
				token = z.buf[i]
				z.n = z.n + (i - z.c) - 1
				i++
				out = z.buf[z.c:i]
				if z.trb {
					z.tr = append(z.tr, z.buf[z.c:i]...)
				}
				z.c = i
				return
			}
		}
	} else {
		for i := z.c; i < len(z.buf); i++ {
			if !accept.isset(z.buf[i]) {
				token = z.buf[i]
				z.n = z.n + (i - z.c) - 1
				if flag == 1 {
					i++
				} else {
					out = z.buf[z.c:i]
				}
				if z.trb {
					z.tr = append(z.tr, z.buf[z.c:i]...)
				}
				z.c = i
				return
			}
		}
	}
	z.n += len(z.buf) - z.c
	if flag != 1 {
		out = append(in, z.buf[z.c:]...)
	}
	if z.trb {
		z.tr = append(z.tr, z.buf[z.c:]...)
	}
	var n2 int
	if z.err != nil {
		return
	}
	for {
		z.c = 0
		z.buf = z.buf[0:cap(z.buf)]
		n2, z.err = z.r.Read(z.buf)
		if n2 > 0 && z.err != nil {
			z.err = nil
		}
		z.buf = z.buf[:n2]
		if flag == 4 {
			for i := 0; i < n2; i++ {
				if z.buf[i] == stop {
					token = z.buf[i]
					z.n += i - 1
					i++
					out = append(out, z.buf[z.c:i]...)
					if z.trb {
						z.tr = append(z.tr, z.buf[z.c:i]...)
					}
					z.c = i
					return
				}
			}
		} else {
			for i := 0; i < n2; i++ {
				if !accept.isset(z.buf[i]) {
					token = z.buf[i]
					z.n += i - 1
					if flag == 1 {
						i++
					}
					if flag != 1 {
						out = append(out, z.buf[z.c:i]...)
					}
					if z.trb {
						z.tr = append(z.tr, z.buf[z.c:i]...)
					}
					z.c = i
					return
				}
			}
		}
		if flag != 1 {
			out = append(out, z.buf[:n2]...)
		}
		z.n += n2
		if z.err != nil {
			return
		}
		if z.trb {
			z.tr = append(z.tr, z.buf[:n2]...)
		}
	}
}

func (z *bufioDecReader) skip(accept *bitset256) (token byte) {
	token, _ = z.search(nil, accept, 0, 1)
	return
}

func (z *bufioDecReader) readTo(in []byte, accept *bitset256) (out []byte) {
	_, out = z.search(in, accept, 0, 2)
	return
}

func (z *bufioDecReader) readUntil(in []byte, stop byte) (out []byte) {
	_, out = z.search(in, nil, stop, 4)
	return
}

func (z *bufioDecReader) unreadn1() {
	err := z.UnreadByte()
	if err != nil {
		panic(err)
	}
}

func (z *bufioDecReader) track() {
	if z.tr != nil {
		z.tr = z.tr[:0]
	}
	z.trb = true
}

func (z *bufioDecReader) stopTrack() (bs []byte) {
	z.trb = false
	return z.tr
}

// ioDecReader is a decReader that reads off an io.Reader.
//
// It also has a fallback implementation of ByteScanner if needed.
type ioDecReader struct {
	r io.Reader // the reader passed in

	rr io.Reader
	br io.ByteScanner

	l   byte // last byte
	ls  byte // last byte status. 0: init-canDoNothing, 1: canRead, 2: canUnread
	trb bool // tracking bytes turned on
	_   bool
	b   [4]byte // tiny buffer for reading single bytes

	x  [scratchByteArrayLen]byte // for: get struct field name, swallow valueTypeBytes, etc
	n  int                       // num read
	tr []byte                    // tracking bytes read
}

func (z *ioDecReader) reset(r io.Reader) {
	z.r = r
	z.rr = r
	z.l, z.ls, z.n, z.trb = 0, 0, 0, false
	if z.tr != nil {
		z.tr = z.tr[:0]
	}
	var ok bool
	if z.br, ok = r.(io.ByteScanner); !ok {
		z.br = z
		z.rr = z
	}
}

func (z *ioDecReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}
	var firstByte bool
	if z.ls == 1 {
		z.ls = 2
		p[0] = z.l
		if len(p) == 1 {
			n = 1
			return
		}
		firstByte = true
		p = p[1:]
	}
	n, err = z.r.Read(p)
	if n > 0 {
		if err == io.EOF && n == len(p) {
			err = nil // read was successful, so postpone EOF (till next time)
		}
		z.l = p[n-1]
		z.ls = 2
	}
	if firstByte {
		n++
	}
	return
}

func (z *ioDecReader) ReadByte() (c byte, err error) {
	n, err := z.Read(z.b[:1])
	if n == 1 {
		c = z.b[0]
		if err == io.EOF {
			err = nil // read was successful, so postpone EOF (till next time)
		}
	}
	return
}

func (z *ioDecReader) UnreadByte() (err error) {
	switch z.ls {
	case 2:
		z.ls = 1
	case 0:
		err = errDecUnreadByteNothingToRead
	case 1:
		err = errDecUnreadByteLastByteNotRead
	default:
		err = errDecUnreadByteUnknown
	}
	return
}

func (z *ioDecReader) numread() int {
	return z.n
}

func (z *ioDecReader) readx(n int) (bs []byte) {
	if n <= 0 {
		return
	}
	if n < len(z.x) {
		bs = z.x[:n]
	} else {
		bs = make([]byte, n)
	}
	if _, err := decReadFull(z.rr, bs); err != nil {
		panic(err)
	}
	z.n += len(bs)
	if z.trb {
		z.tr = append(z.tr, bs...)
	}
	return
}

func (z *ioDecReader) readb(bs []byte) {
	// if len(bs) == 0 {
	// 	return
	// }
	if _, err := decReadFull(z.rr, bs); err != nil {
		panic(err)
	}
	z.n += len(bs)
	if z.trb {
		z.tr = append(z.tr, bs...)
	}
}

func (z *ioDecReader) readn1eof() (b uint8, eof bool) {
	b, err := z.br.ReadByte()
	if err == nil {
		z.n++
		if z.trb {
			z.tr = append(z.tr, b)
		}
	} else if err == io.EOF {
		eof = true
	} else {
		panic(err)
	}
	return
}

func (z *ioDecReader) readn1() (b uint8) {
	var err error
	if b, err = z.br.ReadByte(); err == nil {
		z.n++
		if z.trb {
			z.tr = append(z.tr, b)
		}
		return
	}
	panic(err)
}

func (z *ioDecReader) skip(accept *bitset256) (token byte) {
	for {
		var eof bool
		token, eof = z.readn1eof()
		if eof {
			return
		}
		if accept.isset(token) {
			continue
		}
		return
	}
}

func (z *ioDecReader) readTo(in []byte, accept *bitset256) (out []byte) {
	out = in
	for {
		token, eof := z.readn1eof()
		if eof {
			return
		}
		if accept.isset(token) {
			out = append(out, token)
		} else {
			z.unreadn1()
			return
		}
	}
}

func (z *ioDecReader) readUntil(in []byte, stop byte) (out []byte) {
	out = in
	for {
		token, eof := z.readn1eof()
		if eof {
			panic(io.EOF)
		}
		out = append(out, token)
		if token == stop {
			return
		}
	}
}

func (z *ioDecReader) unreadn1() {
	err := z.br.UnreadByte()
	if err != nil {
		panic(err)
	}
	z.n--
	if z.trb {
		if l := len(z.tr) - 1; l >= 0 {
			z.tr = z.tr[:l]
		}
	}
}

func (z *ioDecReader) track() {
	if z.tr != nil {
		z.tr = z.tr[:0]
	}
	z.trb = true
}

func (z *ioDecReader) stopTrack() (bs []byte) {
	z.trb = false
	return z.tr
}

// ------------------------------------

var errBytesDecReaderCannotUnread = errors.New("cannot unread last byte read")

// bytesDecReader is a decReader that reads off a byte slice with zero copying
type bytesDecReader struct {
	b []byte // data
	c int    // cursor
	a int    // available
	t int    // track start
}

func (z *bytesDecReader) reset(in []byte) {
	z.b = in
	z.a = len(in)
	z.c = 0
	z.t = 0
}

func (z *bytesDecReader) numread() int {
	return z.c
}

func (z *bytesDecReader) unreadn1() {
	if z.c == 0 || len(z.b) == 0 {
		panic(errBytesDecReaderCannotUnread)
	}
	z.c--
	z.a++
	return
}

func (z *bytesDecReader) readx(n int) (bs []byte) {
	// slicing from a non-constant start position is more expensive,
	// as more computation is required to decipher the pointer start position.
	// However, we do it only once, and it's better than reslicing both z.b and return value.

	if n <= 0 {
	} else if z.a == 0 {
		panic(io.EOF)
	} else if n > z.a {
		panic(io.ErrUnexpectedEOF)
	} else {
		c0 := z.c
		z.c = c0 + n
		z.a = z.a - n
		bs = z.b[c0:z.c]
	}
	return
}

func (z *bytesDecReader) readb(bs []byte) {
	copy(bs, z.readx(len(bs)))
}

func (z *bytesDecReader) readn1() (v uint8) {
	if z.a == 0 {
		panic(io.EOF)
	}
	v = z.b[z.c]
	z.c++
	z.a--
	return
}

// func (z *bytesDecReader) readn1eof() (v uint8, eof bool) {
// 	if z.a == 0 {
// 		eof = true
// 		return
// 	}
// 	v = z.b[z.c]
// 	z.c++
// 	z.a--
// 	return
// }

func (z *bytesDecReader) skip(accept *bitset256) (token byte) {
	if z.a == 0 {
		return
	}
	blen := len(z.b)
	for i := z.c; i < blen; i++ {
		if !accept.isset(z.b[i]) {
			token = z.b[i]
			i++
			z.a -= (i - z.c)
			z.c = i
			return
		}
	}
	z.a, z.c = 0, blen
	return
}

func (z *bytesDecReader) readTo(_ []byte, accept *bitset256) (out []byte) {
	if z.a == 0 {
		return
	}
	blen := len(z.b)
	for i := z.c; i < blen; i++ {
		if !accept.isset(z.b[i]) {
			out = z.b[z.c:i]
			z.a -= (i - z.c)
			z.c = i
			return
		}
	}
	out = z.b[z.c:]
	z.a, z.c = 0, blen
	return
}

func (z *bytesDecReader) readUntil(_ []byte, stop byte) (out []byte) {
	if z.a == 0 {
		panic(io.EOF)
	}
	blen := len(z.b)
	for i := z.c; i < blen; i++ {
		if z.b[i] == stop {
			i++
			out = z.b[z.c:i]
			z.a -= (i - z.c)
			z.c = i
			return
		}
	}
	z.a, z.c = 0, blen
	panic(io.EOF)
}

func (z *bytesDecReader) track() {
	z.t = z.c
}

func (z *bytesDecReader) stopTrack() (bs []byte) {
	return z.b[z.t:z.c]
}

// ----------------------------------------

// func (d *Decoder) builtin(f *codecFnInfo, rv reflect.Value) {
// 	d.d.DecodeBuiltin(f.ti.rtid, rv2i(rv))
// }

func (d *Decoder) rawExt(f *codecFnInfo, rv reflect.Value) {
	d.d.DecodeExt(rv2i(rv), 0, nil)
}

func (d *Decoder) ext(f *codecFnInfo, rv reflect.Value) {
	d.d.DecodeExt(rv2i(rv), f.xfTag, f.xfFn)
}

func (d *Decoder) selferUnmarshal(f *codecFnInfo, rv reflect.Value) {
	rv2i(rv).(Selfer).CodecDecodeSelf(d)
}

func (d *Decoder) binaryUnmarshal(f *codecFnInfo, rv reflect.Value) {
	bm := rv2i(rv).(encoding.BinaryUnmarshaler)
	xbs := d.d.DecodeBytes(nil, true)
	if fnerr := bm.UnmarshalBinary(xbs); fnerr != nil {
		panic(fnerr)
	}
}

func (d *Decoder) textUnmarshal(f *codecFnInfo, rv reflect.Value) {
	tm := rv2i(rv).(encoding.TextUnmarshaler)
	fnerr := tm.UnmarshalText(d.d.DecodeStringAsBytes())
	if fnerr != nil {
		panic(fnerr)
	}
}

func (d *Decoder) jsonUnmarshal(f *codecFnInfo, rv reflect.Value) {
	tm := rv2i(rv).(jsonUnmarshaler)
	// bs := d.d.DecodeBytes(d.b[:], true, true)
	// grab the bytes to be read, as UnmarshalJSON needs the full JSON so as to unmarshal it itself.
	fnerr := tm.UnmarshalJSON(d.nextValueBytes())
	if fnerr != nil {
		panic(fnerr)
	}
}

func (d *Decoder) kErr(f *codecFnInfo, rv reflect.Value) {
	d.errorf("no decoding function defined for kind %v", rv.Kind())
}

// var kIntfCtr uint64

func (d *Decoder) kInterfaceNaked(f *codecFnInfo) (rvn reflect.Value) {
	// nil interface:
	// use some hieristics to decode it appropriately
	// based on the detected next value in the stream.
	n := d.naked()
	d.d.DecodeNaked()
	if n.v == valueTypeNil {
		return
	}
	// We cannot decode non-nil stream value into nil interface with methods (e.g. io.Reader).
	if f.ti.numMeth > 0 {
		d.errorf("cannot decode non-nil codec value into nil %v (%v methods)", f.ti.rt, f.ti.numMeth)
		return
	}
	// var useRvn bool
	switch n.v {
	case valueTypeMap:
		// if json, default to a map type with string keys
		mtid := d.mtid
		if mtid == 0 {
			if d.jsms {
				mtid = mapStrIntfTypId
			} else {
				mtid = mapIntfIntfTypId
			}
		}
		if mtid == mapIntfIntfTypId {
			n.initContainers()
			if n.lm < arrayCacheLen {
				n.ma[n.lm] = nil
				rvn = n.rma[n.lm]
				n.lm++
				d.decode(&n.ma[n.lm-1])
				n.lm--
			} else {
				var v2 map[interface{}]interface{}
				d.decode(&v2)
				rvn = reflect.ValueOf(&v2).Elem()
			}
		} else if mtid == mapStrIntfTypId { // for json performance
			n.initContainers()
			if n.ln < arrayCacheLen {
				n.na[n.ln] = nil
				rvn = n.rna[n.ln]
				n.ln++
				d.decode(&n.na[n.ln-1])
				n.ln--
			} else {
				var v2 map[string]interface{}
				d.decode(&v2)
				rvn = reflect.ValueOf(&v2).Elem()
			}
		} else {
			if d.mtr {
				rvn = reflect.New(d.h.MapType)
				d.decode(rv2i(rvn))
				rvn = rvn.Elem()
			} else {
				rvn = reflect.New(d.h.MapType).Elem()
				d.decodeValue(rvn, nil, true)
			}
		}
	case valueTypeArray:
		if d.stid == 0 || d.stid == intfSliceTypId {
			n.initContainers()
			if n.ls < arrayCacheLen {
				n.sa[n.ls] = nil
				rvn = n.rsa[n.ls]
				n.ls++
				d.decode(&n.sa[n.ls-1])
				n.ls--
			} else {
				var v2 []interface{}
				d.decode(&v2)
				rvn = reflect.ValueOf(&v2).Elem()
			}
			if reflectArrayOfSupported && d.stid == 0 && d.h.PreferArrayOverSlice {
				rvn2 := reflect.New(reflectArrayOf(rvn.Len(), intfTyp)).Elem()
				reflect.Copy(rvn2, rvn)
				rvn = rvn2
			}
		} else {
			if d.str {
				rvn = reflect.New(d.h.SliceType)
				d.decode(rv2i(rvn))
				rvn = rvn.Elem()
			} else {
				rvn = reflect.New(d.h.SliceType).Elem()
				d.decodeValue(rvn, nil, true)
			}
		}
	case valueTypeExt:
		var v interface{}
		tag, bytes := n.u, n.l // calling decode below might taint the values
		if bytes == nil {
			n.initContainers()
			if n.li < arrayCacheLen {
				n.ia[n.li] = nil
				n.li++
				d.decode(&n.ia[n.li-1])
				// v = *(&n.ia[l])
				n.li--
				v = n.ia[n.li]
				n.ia[n.li] = nil
			} else {
				d.decode(&v)
			}
		}
		bfn := d.h.getExtForTag(tag)
		if bfn == nil {
			var re RawExt
			re.Tag = tag
			re.Data = detachZeroCopyBytes(d.bytes, nil, bytes)
			re.Value = v
			rvn = reflect.ValueOf(&re).Elem()
		} else {
			rvnA := reflect.New(bfn.rt)
			if bytes != nil {
				bfn.ext.ReadExt(rv2i(rvnA), bytes)
			} else {
				bfn.ext.UpdateExt(rv2i(rvnA), v)
			}
			rvn = rvnA.Elem()
		}
	case valueTypeNil:
		// no-op
	case valueTypeInt:
		rvn = n.ri
	case valueTypeUint:
		rvn = n.ru
	case valueTypeFloat:
		rvn = n.rf
	case valueTypeBool:
		rvn = n.rb
	case valueTypeString, valueTypeSymbol:
		rvn = n.rs
	case valueTypeBytes:
		rvn = n.rl
	case valueTypeTime:
		rvn = n.rt
	default:
		panicv.errorf("kInterfaceNaked: unexpected valueType: %d", n.v)
	}
	return
}

func (d *Decoder) kInterface(f *codecFnInfo, rv reflect.Value) {
	// Note:
	// A consequence of how kInterface works, is that
	// if an interface already contains something, we try
	// to decode into what was there before.
	// We do not replace with a generic value (as got from decodeNaked).

	// every interface passed here MUST be settable.
	var rvn reflect.Value
	if rv.IsNil() || d.h.InterfaceReset {
		// check if mapping to a type: if so, initialize it and move on
		rvn = d.h.intf2impl(f.ti.rtid)
		if rvn.IsValid() {
			rv.Set(rvn)
		} else {
			rvn = d.kInterfaceNaked(f)
			if rvn.IsValid() {
				rv.Set(rvn)
			} else if d.h.InterfaceReset {
				// reset to zero value based on current type in there.
				rv.Set(reflect.Zero(rv.Elem().Type()))
			}
			return
		}
	} else {
		// now we have a non-nil interface value, meaning it contains a type
		rvn = rv.Elem()
	}
	if d.d.TryDecodeAsNil() {
		rv.Set(reflect.Zero(rvn.Type()))
		return
	}

	// Note: interface{} is settable, but underlying type may not be.
	// Consequently, we MAY have to create a decodable value out of the underlying value,
	// decode into it, and reset the interface itself.
	// fmt.Printf(">>>> kInterface: rvn type: %v, rv type: %v\n", rvn.Type(), rv.Type())

	rvn2, canDecode := isDecodeable(rvn)
	if canDecode {
		d.decodeValue(rvn2, nil, true)
		return
	}

	rvn2 = reflect.New(rvn.Type()).Elem()
	rvn2.Set(rvn)
	d.decodeValue(rvn2, nil, true)
	rv.Set(rvn2)
}

func decStructFieldKey(dd decDriver, keyType valueType, b *[decScratchByteArrayLen]byte) (rvkencname []byte) {
	// use if-else-if, not switch (which compiles to binary-search)
	// since keyType is typically valueTypeString, branch prediction is pretty good.

	if keyType == valueTypeString {
		rvkencname = dd.DecodeStringAsBytes()
	} else if keyType == valueTypeInt {
		rvkencname = strconv.AppendInt(b[:0], dd.DecodeInt64(), 10)
	} else if keyType == valueTypeUint {
		rvkencname = strconv.AppendUint(b[:0], dd.DecodeUint64(), 10)
	} else if keyType == valueTypeFloat {
		rvkencname = strconv.AppendFloat(b[:0], dd.DecodeFloat64(), 'f', -1, 64)
	} else {
		rvkencname = dd.DecodeStringAsBytes()
	}
	return rvkencname
}

func (d *Decoder) kStruct(f *codecFnInfo, rv reflect.Value) {
	fti := f.ti
	dd := d.d
	elemsep := d.esep
	sfn := structFieldNode{v: rv, update: true}
	ctyp := dd.ContainerType()
	if ctyp == valueTypeMap {
		containerLen := dd.ReadMapStart()
		if containerLen == 0 {
			dd.ReadMapEnd()
			return
		}
		tisfi := fti.sfiSort
		hasLen := containerLen >= 0

		var rvkencname []byte
		for j := 0; (hasLen && j < containerLen) || !(hasLen || dd.CheckBreak()); j++ {
			if elemsep {
				dd.ReadMapElemKey()
			}
			rvkencname = decStructFieldKey(dd, fti.keyType, &d.b)
			if elemsep {
				dd.ReadMapElemValue()
			}
			if k := fti.indexForEncName(rvkencname); k > -1 {
				si := tisfi[k]
				if dd.TryDecodeAsNil() {
					si.setToZeroValue(rv)
				} else {
					d.decodeValue(sfn.field(si), nil, true)
				}
			} else {
				d.structFieldNotFound(-1, stringView(rvkencname))
			}
			// keepAlive4StringView(rvkencnameB) // not needed, as reference is outside loop
		}
		dd.ReadMapEnd()
	} else if ctyp == valueTypeArray {
		containerLen := dd.ReadArrayStart()
		if containerLen == 0 {
			dd.ReadArrayEnd()
			return
		}
		// Not much gain from doing it two ways for array.
		// Arrays are not used as much for structs.
		hasLen := containerLen >= 0
		for j, si := range fti.sfiSrc {
			if (hasLen && j == containerLen) || (!hasLen && dd.CheckBreak()) {
				break
			}
			if elemsep {
				dd.ReadArrayElem()
			}
			if dd.TryDecodeAsNil() {
				si.setToZeroValue(rv)
			} else {
				d.decodeValue(sfn.field(si), nil, true)
			}
		}
		if containerLen > len(fti.sfiSrc) {
			// read remaining values and throw away
			for j := len(fti.sfiSrc); j < containerLen; j++ {
				if elemsep {
					dd.ReadArrayElem()
				}
				d.structFieldNotFound(j, "")
			}
		}
		dd.ReadArrayEnd()
	} else {
		d.errorstr(errstrOnlyMapOrArrayCanDecodeIntoStruct)
		return
	}
}

func (d *Decoder) kSlice(f *codecFnInfo, rv reflect.Value) {
	// A slice can be set from a map or array in stream.
	// This way, the order can be kept (as order is lost with map).
	ti := f.ti
	if f.seq == seqTypeChan && ti.chandir&uint8(reflect.SendDir) == 0 {
		d.errorf("receive-only channel cannot be decoded")
	}
	dd := d.d
	rtelem0 := ti.elem
	ctyp := dd.ContainerType()
	if ctyp == valueTypeBytes || ctyp == valueTypeString {
		// you can only decode bytes or string in the stream into a slice or array of bytes
		if !(ti.rtid == uint8SliceTypId || rtelem0.Kind() == reflect.Uint8) {
			d.errorf("bytes/string in stream must decode into slice/array of bytes, not %v", ti.rt)
		}
		if f.seq == seqTypeChan {
			bs2 := dd.DecodeBytes(nil, true)
			irv := rv2i(rv)
			ch, ok := irv.(chan<- byte)
			if !ok {
				ch = irv.(chan byte)
			}
			for _, b := range bs2 {
				ch <- b
			}
		} else {
			rvbs := rv.Bytes()
			bs2 := dd.DecodeBytes(rvbs, false)
			// if rvbs == nil && bs2 != nil || rvbs != nil && bs2 == nil || len(bs2) != len(rvbs) {
			if !(len(bs2) > 0 && len(bs2) == len(rvbs) && &bs2[0] == &rvbs[0]) {
				if rv.CanSet() {
					rv.SetBytes(bs2)
				} else if len(rvbs) > 0 && len(bs2) > 0 {
					copy(rvbs, bs2)
				}
			}
		}
		return
	}

	// array := f.seq == seqTypeChan

	slh, containerLenS := d.decSliceHelperStart() // only expects valueType(Array|Map)

	// an array can never return a nil slice. so no need to check f.array here.
	if containerLenS == 0 {
		if rv.CanSet() {
			if f.seq == seqTypeSlice {
				if rv.IsNil() {
					rv.Set(reflect.MakeSlice(ti.rt, 0, 0))
				} else {
					rv.SetLen(0)
				}
			} else if f.seq == seqTypeChan {
				if rv.IsNil() {
					rv.Set(reflect.MakeChan(ti.rt, 0))
				}
			}
		}
		slh.End()
		return
	}

	rtelem0Size := int(rtelem0.Size())
	rtElem0Kind := rtelem0.Kind()
	rtelem0Mut := !isImmutableKind(rtElem0Kind)
	rtelem := rtelem0
	rtelemkind := rtelem.Kind()
	for rtelemkind == reflect.Ptr {
		rtelem = rtelem.Elem()
		rtelemkind = rtelem.Kind()
	}

	var fn *codecFn

	var rvCanset = rv.CanSet()
	var rvChanged bool
	var rv0 = rv
	var rv9 reflect.Value

	rvlen := rv.Len()
	rvcap := rv.Cap()
	hasLen := containerLenS > 0
	if hasLen && f.seq == seqTypeSlice {
		if containerLenS > rvcap {
			oldRvlenGtZero := rvlen > 0
			rvlen = decInferLen(containerLenS, d.h.MaxInitLen, int(rtelem0.Size()))
			if rvlen <= rvcap {
				if rvCanset {
					rv.SetLen(rvlen)
				}
			} else if rvCanset {
				rv = reflect.MakeSlice(ti.rt, rvlen, rvlen)
				rvcap = rvlen
				rvChanged = true
			} else {
				d.errorf("cannot decode into non-settable slice")
			}
			if rvChanged && oldRvlenGtZero && !isImmutableKind(rtelem0.Kind()) {
				reflect.Copy(rv, rv0) // only copy up to length NOT cap i.e. rv0.Slice(0, rvcap)
			}
		} else if containerLenS != rvlen {
			rvlen = containerLenS
			if rvCanset {
				rv.SetLen(rvlen)
			}
			// else {
			// rv = rv.Slice(0, rvlen)
			// rvChanged = true
			// d.errorf("cannot decode into non-settable slice")
			// }
		}
	}

	// consider creating new element once, and just decoding into it.
	var rtelem0Zero reflect.Value
	var rtelem0ZeroValid bool
	var decodeAsNil bool
	var j int
	d.cfer()
	for ; (hasLen && j < containerLenS) || !(hasLen || dd.CheckBreak()); j++ {
		if j == 0 && (f.seq == seqTypeSlice || f.seq == seqTypeChan) && rv.IsNil() {
			if hasLen {
				rvlen = decInferLen(containerLenS, d.h.MaxInitLen, rtelem0Size)
			} else if f.seq == seqTypeSlice {
				rvlen = decDefSliceCap
			} else {
				rvlen = decDefChanCap
			}
			if rvCanset {
				if f.seq == seqTypeSlice {
					rv = reflect.MakeSlice(ti.rt, rvlen, rvlen)
					rvChanged = true
				} else { // chan
					// xdebugf(">>>>>> haslen = %v, make chan of type '%v' with length: %v", hasLen, ti.rt, rvlen)
					rv = reflect.MakeChan(ti.rt, rvlen)
					rvChanged = true
				}
			} else {
				d.errorf("cannot decode into non-settable slice")
			}
		}
		slh.ElemContainerState(j)
		decodeAsNil = dd.TryDecodeAsNil()
		if f.seq == seqTypeChan {
			if decodeAsNil {
				rv.Send(reflect.Zero(rtelem0))
				continue
			}
			if rtelem0Mut || !rv9.IsValid() { // || (rtElem0Kind == reflect.Ptr && rv9.IsNil()) {
				rv9 = reflect.New(rtelem0).Elem()
			}
			if fn == nil {
				fn = d.cf.get(rtelem, true, true)
			}
			d.decodeValue(rv9, fn, true)
			// xdebugf(">>>> rv9 sent on %v during decode: %v, with len=%v, cap=%v", rv.Type(), rv9, rv.Len(), rv.Cap())
			rv.Send(rv9)
		} else {
			// if indefinite, etc, then expand the slice if necessary
			var decodeIntoBlank bool
			if j >= rvlen {
				if f.seq == seqTypeArray {
					d.arrayCannotExpand(rvlen, j+1)
					decodeIntoBlank = true
				} else { // if f.seq == seqTypeSlice
					// rv = reflect.Append(rv, reflect.Zero(rtelem0)) // append logic + varargs
					var rvcap2 int
					var rvErrmsg2 string
					rv9, rvcap2, rvChanged, rvErrmsg2 =
						expandSliceRV(rv, ti.rt, rvCanset, rtelem0Size, 1, rvlen, rvcap)
					if rvErrmsg2 != "" {
						d.errorf(rvErrmsg2)
					}
					rvlen++
					if rvChanged {
						rv = rv9
						rvcap = rvcap2
					}
				}
			}
			if decodeIntoBlank {
				if !decodeAsNil {
					d.swallow()
				}
			} else {
				rv9 = rv.Index(j)
				if d.h.SliceElementReset || decodeAsNil {
					if !rtelem0ZeroValid {
						rtelem0ZeroValid = true
						rtelem0Zero = reflect.Zero(rtelem0)
					}
					rv9.Set(rtelem0Zero)
				}
				if decodeAsNil {
					continue
				}

				if fn == nil {
					fn = d.cf.get(rtelem, true, true)
				}
				d.decodeValue(rv9, fn, true)
			}
		}
	}
	if f.seq == seqTypeSlice {
		if j < rvlen {
			if rv.CanSet() {
				rv.SetLen(j)
			} else if rvCanset {
				rv = rv.Slice(0, j)
				rvChanged = true
			} // else { d.errorf("kSlice: cannot change non-settable slice") }
			rvlen = j
		} else if j == 0 && rv.IsNil() {
			if rvCanset {
				rv = reflect.MakeSlice(ti.rt, 0, 0)
				rvChanged = true
			} // else { d.errorf("kSlice: cannot change non-settable slice") }
		}
	}
	slh.End()

	if rvChanged { // infers rvCanset=true, so it can be reset
		rv0.Set(rv)
	}
}

// func (d *Decoder) kArray(f *codecFnInfo, rv reflect.Value) {
// 	// d.decodeValueFn(rv.Slice(0, rv.Len()))
// 	f.kSlice(rv.Slice(0, rv.Len()))
// }

func (d *Decoder) kMap(f *codecFnInfo, rv reflect.Value) {
	dd := d.d
	containerLen := dd.ReadMapStart()
	elemsep := d.esep
	ti := f.ti
	if rv.IsNil() {
		rv.Set(makeMapReflect(ti.rt, containerLen))
	}

	if containerLen == 0 {
		dd.ReadMapEnd()
		return
	}

	ktype, vtype := ti.key, ti.elem
	ktypeId := rt2id(ktype)
	vtypeKind := vtype.Kind()

	var keyFn, valFn *codecFn
	var ktypeLo, vtypeLo reflect.Type

	for ktypeLo = ktype; ktypeLo.Kind() == reflect.Ptr; ktypeLo = ktypeLo.Elem() {
	}

	for vtypeLo = vtype; vtypeLo.Kind() == reflect.Ptr; vtypeLo = vtypeLo.Elem() {
	}

	var mapGet, mapSet bool
	rvvImmut := isImmutableKind(vtypeKind)
	if !d.h.MapValueReset {
		// if pointer, mapGet = true
		// if interface, mapGet = true if !DecodeNakedAlways (else false)
		// if builtin, mapGet = false
		// else mapGet = true
		if vtypeKind == reflect.Ptr {
			mapGet = true
		} else if vtypeKind == reflect.Interface {
			if !d.h.InterfaceReset {
				mapGet = true
			}
		} else if !rvvImmut {
			mapGet = true
		}
	}

	var rvk, rvkp, rvv, rvz reflect.Value
	rvkMut := !isImmutableKind(ktype.Kind()) // if ktype is immutable, then re-use the same rvk.
	ktypeIsString := ktypeId == stringTypId
	ktypeIsIntf := ktypeId == intfTypId
	hasLen := containerLen > 0
	var kstrbs []byte
	d.cfer()
	for j := 0; (hasLen && j < containerLen) || !(hasLen || dd.CheckBreak()); j++ {
		if rvkMut || !rvkp.IsValid() {
			rvkp = reflect.New(ktype)
			rvk = rvkp.Elem()
		}
		if elemsep {
			dd.ReadMapElemKey()
		}
		if false && dd.TryDecodeAsNil() { // nil cannot be a map key, so disregard this block
			// Previously, if a nil key, we just ignored the mapped value and continued.
			// However, that makes the result of encoding and then decoding map[intf]intf{nil:nil}
			// to be an empty map.
			// Instead, we treat a nil key as the zero value of the type.
			rvk.Set(reflect.Zero(ktype))
		} else if ktypeIsString {
			kstrbs = dd.DecodeStringAsBytes()
			rvk.SetString(stringView(kstrbs))
			// NOTE: if doing an insert, you MUST use a real string (not stringview)
		} else {
			if keyFn == nil {
				keyFn = d.cf.get(ktypeLo, true, true)
			}
			d.decodeValue(rvk, keyFn, true)
		}
		// special case if a byte array.
		if ktypeIsIntf {
			if rvk2 := rvk.Elem(); rvk2.IsValid() {
				if rvk2.Type() == uint8SliceTyp {
					rvk = reflect.ValueOf(d.string(rvk2.Bytes()))
				} else {
					rvk = rvk2
				}
			}
		}

		if elemsep {
			dd.ReadMapElemValue()
		}

		// Brittle, but OK per TryDecodeAsNil() contract.
		// i.e. TryDecodeAsNil never shares slices with other decDriver procedures
		if dd.TryDecodeAsNil() {
			if ktypeIsString {
				rvk.SetString(d.string(kstrbs))
			}
			if d.h.DeleteOnNilMapValue {
				rv.SetMapIndex(rvk, reflect.Value{})
			} else {
				rv.SetMapIndex(rvk, reflect.Zero(vtype))
			}
			continue
		}

		mapSet = true // set to false if u do a get, and its a non-nil pointer
		if mapGet {
			// mapGet true only in case where kind=Ptr|Interface or kind is otherwise mutable.
			rvv = rv.MapIndex(rvk)
			if !rvv.IsValid() {
				rvv = reflect.New(vtype).Elem()
			} else if vtypeKind == reflect.Ptr {
				if rvv.IsNil() {
					rvv = reflect.New(vtype).Elem()
				} else {
					mapSet = false
				}
			} else if vtypeKind == reflect.Interface {
				// not addressable, and thus not settable.
				// e MUST create a settable/addressable variant
				rvv2 := reflect.New(rvv.Type()).Elem()
				if !rvv.IsNil() {
					rvv2.Set(rvv)
				}
				rvv = rvv2
			}
			// else it is ~mutable, and we can just decode into it directly
		} else if rvvImmut {
			if !rvz.IsValid() {
				rvz = reflect.New(vtype).Elem()
			}
			rvv = rvz
		} else {
			rvv = reflect.New(vtype).Elem()
		}

		// We MUST be done with the stringview of the key, before decoding the value
		// so that we don't bastardize the reused byte array.
		if mapSet && ktypeIsString {
			rvk.SetString(d.string(kstrbs))
		}
		if valFn == nil {
			valFn = d.cf.get(vtypeLo, true, true)
		}
		d.decodeValue(rvv, valFn, true)
		// d.decodeValueFn(rvv, valFn)
		if mapSet {
			rv.SetMapIndex(rvk, rvv)
		}
		// if ktypeIsString {
		// 	// keepAlive4StringView(kstrbs) // not needed, as reference is outside loop
		// }
	}

	dd.ReadMapEnd()
}

// decNaked is used to keep track of the primitives decoded.
// Without it, we would have to decode each primitive and wrap it
// in an interface{}, causing an allocation.
// In this model, the primitives are decoded in a "pseudo-atomic" fashion,
// so we can rest assured that no other decoding happens while these
// primitives are being decoded.
//
// maps and arrays are not handled by this mechanism.
// However, RawExt is, and we accommodate for extensions that decode
// RawExt from DecodeNaked, but need to decode the value subsequently.
// kInterfaceNaked and swallow, which call DecodeNaked, handle this caveat.
//
// However, decNaked also keeps some arrays of default maps and slices
// used in DecodeNaked. This way, we can get a pointer to it
// without causing a new heap allocation.
//
// kInterfaceNaked will ensure that there is no allocation for the common
// uses.

type decNakedContainers struct {
	// array/stacks for reducing allocation
	// keep arrays at the bottom? Chance is that they are not used much.
	ia [arrayCacheLen]interface{}
	ma [arrayCacheLen]map[interface{}]interface{}
	na [arrayCacheLen]map[string]interface{}
	sa [arrayCacheLen][]interface{}

	// ria [arrayCacheLen]reflect.Value // not needed, as we decode directly into &ia[n]
	rma, rna, rsa [arrayCacheLen]reflect.Value // reflect.Value mapping to above
}

func (n *decNakedContainers) init() {
	for i := 0; i < arrayCacheLen; i++ {
		// n.ria[i] = reflect.ValueOf(&(n.ia[i])).Elem()
		n.rma[i] = reflect.ValueOf(&(n.ma[i])).Elem()
		n.rna[i] = reflect.ValueOf(&(n.na[i])).Elem()
		n.rsa[i] = reflect.ValueOf(&(n.sa[i])).Elem()
	}
}

type decNaked struct {
	// r RawExt // used for RawExt, uint, []byte.

	// primitives below
	u uint64
	i int64
	f float64
	l []byte
	s string

	// ---- cpu cache line boundary?
	t time.Time
	b bool

	// state
	v              valueType
	li, lm, ln, ls int8
	inited         bool

	*decNakedContainers

	ru, ri, rf, rl, rs, rb, rt reflect.Value // mapping to the primitives above

	// _ [6]uint64 // padding // no padding - rt goes into next cache line
}

func (n *decNaked) init() {
	if n.inited {
		return
	}
	n.ru = reflect.ValueOf(&n.u).Elem()
	n.ri = reflect.ValueOf(&n.i).Elem()
	n.rf = reflect.ValueOf(&n.f).Elem()
	n.rl = reflect.ValueOf(&n.l).Elem()
	n.rs = reflect.ValueOf(&n.s).Elem()
	n.rt = reflect.ValueOf(&n.t).Elem()
	n.rb = reflect.ValueOf(&n.b).Elem()

	n.inited = true
	// n.rr[] = reflect.ValueOf(&n.)
}

func (n *decNaked) initContainers() {
	if n.decNakedContainers == nil {
		n.decNakedContainers = new(decNakedContainers)
		n.decNakedContainers.init()
	}
}

func (n *decNaked) reset() {
	if n == nil {
		return
	}
	n.li, n.lm, n.ln, n.ls = 0, 0, 0, 0
}

type rtid2rv struct {
	rtid uintptr
	rv   reflect.Value
}

// --------------

type decReaderSwitch struct {
	rb bytesDecReader
	// ---- cpu cache line boundary?
	ri       *ioDecReader
	mtr, str bool // whether maptype or slicetype are known types

	be    bool // is binary encoding
	bytes bool // is bytes reader
	js    bool // is json handle
	jsms  bool // is json handle, and MapKeyAsString
	esep  bool // has elem separators
}

// TODO: Uncomment after mid-stack inlining enabled in go 1.11
//
// func (z *decReaderSwitch) unreadn1() {
// 	if z.bytes {
// 		z.rb.unreadn1()
// 	} else {
// 		z.ri.unreadn1()
// 	}
// }
// func (z *decReaderSwitch) readx(n int) []byte {
// 	if z.bytes {
// 		return z.rb.readx(n)
// 	}
// 	return z.ri.readx(n)
// }
// func (z *decReaderSwitch) readb(s []byte) {
// 	if z.bytes {
// 		z.rb.readb(s)
// 	} else {
// 		z.ri.readb(s)
// 	}
// }
// func (z *decReaderSwitch) readn1() uint8 {
// 	if z.bytes {
// 		return z.rb.readn1()
// 	}
// 	return z.ri.readn1()
// }
// func (z *decReaderSwitch) numread() int {
// 	if z.bytes {
// 		return z.rb.numread()
// 	}
// 	return z.ri.numread()
// }
// func (z *decReaderSwitch) track() {
// 	if z.bytes {
// 		z.rb.track()
// 	} else {
// 		z.ri.track()
// 	}
// }
// func (z *decReaderSwitch) stopTrack() []byte {
// 	if z.bytes {
// 		return z.rb.stopTrack()
// 	}
// 	return z.ri.stopTrack()
// }
// func (z *decReaderSwitch) skip(accept *bitset256) (token byte) {
// 	if z.bytes {
// 		return z.rb.skip(accept)
// 	}
// 	return z.ri.skip(accept)
// }
// func (z *decReaderSwitch) readTo(in []byte, accept *bitset256) (out []byte) {
// 	if z.bytes {
// 		return z.rb.readTo(in, accept)
// 	}
// 	return z.ri.readTo(in, accept)
// }
// func (z *decReaderSwitch) readUntil(in []byte, stop byte) (out []byte) {
// 	if z.bytes {
// 		return z.rb.readUntil(in, stop)
// 	}
// 	return z.ri.readUntil(in, stop)
// }

// A Decoder reads and decodes an object from an input stream in the codec format.
type Decoder struct {
	panicHdl
	// hopefully, reduce derefencing cost by laying the decReader inside the Decoder.
	// Try to put things that go together to fit within a cache line (8 words).

	d decDriver
	// NOTE: Decoder shouldn't call it's read methods,
	// as the handler MAY need to do some coordination.
	r  decReader
	h  *BasicHandle
	bi *bufioDecReader
	// cache the mapTypeId and sliceTypeId for faster comparisons
	mtid uintptr
	stid uintptr

	// ---- cpu cache line boundary?
	decReaderSwitch

	// ---- cpu cache line boundary?
	codecFnPooler
	// cr containerStateRecv
	n   *decNaked
	nsp *sync.Pool
	err error

	// ---- cpu cache line boundary?
	b  [decScratchByteArrayLen]byte // scratch buffer, used by Decoder and xxxEncDrivers
	is map[string]string            // used for interning strings

	// padding - false sharing help // modify 232 if Decoder struct changes.
	// _ [cacheLineSize - 232%cacheLineSize]byte
}

// NewDecoder returns a Decoder for decoding a stream of bytes from an io.Reader.
//
// For efficiency, Users are encouraged to pass in a memory buffered reader
// (eg bufio.Reader, bytes.Buffer).
func NewDecoder(r io.Reader, h Handle) *Decoder {
	d := newDecoder(h)
	d.Reset(r)
	return d
}

// NewDecoderBytes returns a Decoder which efficiently decodes directly
// from a byte slice with zero copying.
func NewDecoderBytes(in []byte, h Handle) *Decoder {
	d := newDecoder(h)
	d.ResetBytes(in)
	return d
}

var defaultDecNaked decNaked

func newDecoder(h Handle) *Decoder {
	d := &Decoder{h: h.getBasicHandle(), err: errDecoderNotInitialized}
	d.hh = h
	d.be = h.isBinary()
	// NOTE: do not initialize d.n here. It is lazily initialized in d.naked()
	var jh *JsonHandle
	jh, d.js = h.(*JsonHandle)
	if d.js {
		d.jsms = jh.MapKeyAsString
	}
	d.esep = d.hh.hasElemSeparators()
	if d.h.InternString {
		d.is = make(map[string]string, 32)
	}
	d.d = h.newDecDriver(d)
	// d.cr, _ = d.d.(containerStateRecv)
	return d
}

func (d *Decoder) resetCommon() {
	d.n.reset()
	d.d.reset()
	d.err = nil
	// reset all things which were cached from the Handle, but could change
	d.mtid, d.stid = 0, 0
	d.mtr, d.str = false, false
	if d.h.MapType != nil {
		d.mtid = rt2id(d.h.MapType)
		d.mtr = fastpathAV.index(d.mtid) != -1
	}
	if d.h.SliceType != nil {
		d.stid = rt2id(d.h.SliceType)
		d.str = fastpathAV.index(d.stid) != -1
	}
}

// Reset the Decoder with a new Reader to decode from,
// clearing all state from last run(s).
func (d *Decoder) Reset(r io.Reader) {
	if r == nil {
		return
	}
	if d.bi == nil {
		d.bi = new(bufioDecReader)
	}
	d.bytes = false
	if d.h.ReaderBufferSize > 0 {
		d.bi.buf = make([]byte, 0, d.h.ReaderBufferSize)
		d.bi.reset(r)
		d.r = d.bi
	} else {
		// d.ri.x = &d.b
		// d.s = d.sa[:0]
		if d.ri == nil {
			d.ri = new(ioDecReader)
		}
		d.ri.reset(r)
		d.r = d.ri
	}
	d.resetCommon()
}

// ResetBytes resets the Decoder with a new []byte to decode from,
// clearing all state from last run(s).
func (d *Decoder) ResetBytes(in []byte) {
	if in == nil {
		return
	}
	d.bytes = true
	d.rb.reset(in)
	d.r = &d.rb
	d.resetCommon()
}

// naked must be called before each call to .DecodeNaked,
// as they will use it.
func (d *Decoder) naked() *decNaked {
	if d.n == nil {
		// consider one of:
		//   - get from sync.Pool  (if GC is frequent, there's no value here)
		//   - new alloc           (safest. only init'ed if it a naked decode will be done)
		//   - field in Decoder    (makes the Decoder struct very big)
		// To support using a decoder where a DecodeNaked is not needed,
		// we prefer #1 or #2.
		// d.n = new(decNaked) // &d.nv // new(decNaked) // grab from a sync.Pool
		// d.n.init()
		var v interface{}
		d.nsp, v = pool.decNaked()
		d.n = v.(*decNaked)
	}
	return d.n
}

// Decode decodes the stream from reader and stores the result in the
// value pointed to by v. v cannot be a nil pointer. v can also be
// a reflect.Value of a pointer.
//
// Note that a pointer to a nil interface is not a nil pointer.
// If you do not know what type of stream it is, pass in a pointer to a nil interface.
// We will decode and store a value in that nil interface.
//
// Sample usages:
//   // Decoding into a non-nil typed value
//   var f float32
//   err = codec.NewDecoder(r, handle).Decode(&f)
//
//   // Decoding into nil interface
//   var v interface{}
//   dec := codec.NewDecoder(r, handle)
//   err = dec.Decode(&v)
//
// When decoding into a nil interface{}, we will decode into an appropriate value based
// on the contents of the stream:
//   - Numbers are decoded as float64, int64 or uint64.
//   - Other values are decoded appropriately depending on the type:
//     bool, string, []byte, time.Time, etc
//   - Extensions are decoded as RawExt (if no ext function registered for the tag)
// Configurations exist on the Handle to override defaults
// (e.g. for MapType, SliceType and how to decode raw bytes).
//
// When decoding into a non-nil interface{} value, the mode of encoding is based on the
// type of the value. When a value is seen:
//   - If an extension is registered for it, call that extension function
//   - If it implements BinaryUnmarshaler, call its UnmarshalBinary(data []byte) error
//   - Else decode it based on its reflect.Kind
//
// There are some special rules when decoding into containers (slice/array/map/struct).
// Decode will typically use the stream contents to UPDATE the container.
//   - A map can be decoded from a stream map, by updating matching keys.
//   - A slice can be decoded from a stream array,
//     by updating the first n elements, where n is length of the stream.
//   - A slice can be decoded from a stream map, by decoding as if
//     it contains a sequence of key-value pairs.
//   - A struct can be decoded from a stream map, by updating matching fields.
//   - A struct can be decoded from a stream array,
//     by updating fields as they occur in the struct (by index).
//
// When decoding a stream map or array with length of 0 into a nil map or slice,
// we reset the destination map or slice to a zero-length value.
//
// However, when decoding a stream nil, we reset the destination container
// to its "zero" value (e.g. nil for slice/map, etc).
//
// Note: we allow nil values in the stream anywhere except for map keys.
// A nil value in the encoded stream where a map key is expected is treated as an error.
func (d *Decoder) Decode(v interface{}) (err error) {
	defer d.deferred(&err)
	d.MustDecode(v)
	return
}

// MustDecode is like Decode, but panics if unable to Decode.
// This provides insight to the code location that triggered the error.
func (d *Decoder) MustDecode(v interface{}) {
	// TODO: Top-level: ensure that v is a pointer and not nil.
	if d.err != nil {
		panic(d.err)
	}
	if d.d.TryDecodeAsNil() {
		setZero(v)
	} else {
		d.decode(v)
	}
	d.alwaysAtEnd()
	// xprintf(">>>>>>>> >>>>>>>> num decFns: %v\n", d.cf.sn)
}

func (d *Decoder) deferred(err1 *error) {
	d.alwaysAtEnd()
	if recoverPanicToErr {
		if x := recover(); x != nil {
			panicValToErr(d, x, err1)
			panicValToErr(d, x, &d.err)
		}
	}
}

func (d *Decoder) alwaysAtEnd() {
	if d.n != nil {
		// if n != nil, then nsp != nil (they are always set together)
		d.nsp.Put(d.n)
		d.n, d.nsp = nil, nil
	}
	d.codecFnPooler.alwaysAtEnd()
}

// // this is not a smart swallow, as it allocates objects and does unnecessary work.
// func (d *Decoder) swallowViaHammer() {
// 	var blank interface{}
// 	d.decodeValueNoFn(reflect.ValueOf(&blank).Elem())
// }

func (d *Decoder) swallow() {
	// smarter decode that just swallows the content
	dd := d.d
	if dd.TryDecodeAsNil() {
		return
	}
	elemsep := d.esep
	switch dd.ContainerType() {
	case valueTypeMap:
		containerLen := dd.ReadMapStart()
		hasLen := containerLen >= 0
		for j := 0; (hasLen && j < containerLen) || !(hasLen || dd.CheckBreak()); j++ {
			// if clenGtEqualZero {if j >= containerLen {break} } else if dd.CheckBreak() {break}
			if elemsep {
				dd.ReadMapElemKey()
			}
			d.swallow()
			if elemsep {
				dd.ReadMapElemValue()
			}
			d.swallow()
		}
		dd.ReadMapEnd()
	case valueTypeArray:
		containerLen := dd.ReadArrayStart()
		hasLen := containerLen >= 0
		for j := 0; (hasLen && j < containerLen) || !(hasLen || dd.CheckBreak()); j++ {
			if elemsep {
				dd.ReadArrayElem()
			}
			d.swallow()
		}
		dd.ReadArrayEnd()
	case valueTypeBytes:
		dd.DecodeBytes(d.b[:], true)
	case valueTypeString:
		dd.DecodeStringAsBytes()
	default:
		// these are all primitives, which we can get from decodeNaked
		// if RawExt using Value, complete the processing.
		n := d.naked()
		dd.DecodeNaked()
		if n.v == valueTypeExt && n.l == nil {
			n.initContainers()
			if n.li < arrayCacheLen {
				n.ia[n.li] = nil
				n.li++
				d.decode(&n.ia[n.li-1])
				n.ia[n.li-1] = nil
				n.li--
			} else {
				var v2 interface{}
				d.decode(&v2)
			}
		}
	}
}

func setZero(iv interface{}) {
	if iv == nil || definitelyNil(iv) {
		return
	}
	var canDecode bool
	switch v := iv.(type) {
	case *string:
		*v = ""
	case *bool:
		*v = false
	case *int:
		*v = 0
	case *int8:
		*v = 0
	case *int16:
		*v = 0
	case *int32:
		*v = 0
	case *int64:
		*v = 0
	case *uint:
		*v = 0
	case *uint8:
		*v = 0
	case *uint16:
		*v = 0
	case *uint32:
		*v = 0
	case *uint64:
		*v = 0
	case *float32:
		*v = 0
	case *float64:
		*v = 0
	case *[]uint8:
		*v = nil
	case *Raw:
		*v = nil
	case *time.Time:
		*v = time.Time{}
	case reflect.Value:
		if v, canDecode = isDecodeable(v); canDecode && v.CanSet() {
			v.Set(reflect.Zero(v.Type()))
		} // TODO: else drain if chan, clear if map, set all to nil if slice???
	default:
		if !fastpathDecodeSetZeroTypeSwitch(iv) {
			v := reflect.ValueOf(iv)
			if v, canDecode = isDecodeable(v); canDecode && v.CanSet() {
				v.Set(reflect.Zero(v.Type()))
			} // TODO: else drain if chan, clear if map, set all to nil if slice???
		}
	}
}

func (d *Decoder) decode(iv interface{}) {
	// check nil and interfaces explicitly,
	// so that type switches just have a run of constant non-interface types.
	if iv == nil {
		d.errorstr(errstrCannotDecodeIntoNil)
		return
	}
	if v, ok := iv.(Selfer); ok {
		v.CodecDecodeSelf(d)
		return
	}

	switch v := iv.(type) {
	// case nil:
	// case Selfer:

	case reflect.Value:
		v = d.ensureDecodeable(v)
		d.decodeValue(v, nil, true)

	case *string:
		*v = d.d.DecodeString()
	case *bool:
		*v = d.d.DecodeBool()
	case *int:
		*v = int(chkOvf.IntV(d.d.DecodeInt64(), intBitsize))
	case *int8:
		*v = int8(chkOvf.IntV(d.d.DecodeInt64(), 8))
	case *int16:
		*v = int16(chkOvf.IntV(d.d.DecodeInt64(), 16))
	case *int32:
		*v = int32(chkOvf.IntV(d.d.DecodeInt64(), 32))
	case *int64:
		*v = d.d.DecodeInt64()
	case *uint:
		*v = uint(chkOvf.UintV(d.d.DecodeUint64(), uintBitsize))
	case *uint8:
		*v = uint8(chkOvf.UintV(d.d.DecodeUint64(), 8))
	case *uint16:
		*v = uint16(chkOvf.UintV(d.d.DecodeUint64(), 16))
	case *uint32:
		*v = uint32(chkOvf.UintV(d.d.DecodeUint64(), 32))
	case *uint64:
		*v = d.d.DecodeUint64()
	case *float32:
		f64 := d.d.DecodeFloat64()
		if chkOvf.Float32(f64) {
			d.errorf("float32 overflow: %v", f64)
		}
		*v = float32(f64)
	case *float64:
		*v = d.d.DecodeFloat64()
	case *[]uint8:
		*v = d.d.DecodeBytes(*v, false)
	case []uint8:
		b := d.d.DecodeBytes(v, false)
		if !(len(b) > 0 && len(b) == len(v) && &b[0] == &v[0]) {
			copy(v, b)
		}
	case *time.Time:
		*v = d.d.DecodeTime()
	case *Raw:
		*v = d.rawBytes()

	case *interface{}:
		d.decodeValue(reflect.ValueOf(iv).Elem(), nil, true)
		// d.decodeValueNotNil(reflect.ValueOf(iv).Elem())

	default:
		if !fastpathDecodeTypeSwitch(iv, d) {
			v := reflect.ValueOf(iv)
			v = d.ensureDecodeable(v)
			d.decodeValue(v, nil, false)
			// d.decodeValueFallback(v)
		}
	}
}

func (d *Decoder) decodeValue(rv reflect.Value, fn *codecFn, chkAll bool) {
	// If stream is not containing a nil value, then we can deref to the base
	// non-pointer value, and decode into that.
	var rvp reflect.Value
	var rvpValid bool
	if rv.Kind() == reflect.Ptr {
		rvpValid = true
		for {
			if rv.IsNil() {
				rv.Set(reflect.New(rv.Type().Elem()))
			}
			rvp = rv
			rv = rv.Elem()
			if rv.Kind() != reflect.Ptr {
				break
			}
		}
	}

	if fn == nil {
		// always pass checkCodecSelfer=true, in case T or ****T is passed, where *T is a Selfer
		fn = d.cfer().get(rv.Type(), chkAll, true) // chkAll, chkAll)
	}
	if fn.i.addrD {
		if rvpValid {
			fn.fd(d, &fn.i, rvp)
		} else if rv.CanAddr() {
			fn.fd(d, &fn.i, rv.Addr())
		} else if !fn.i.addrF {
			fn.fd(d, &fn.i, rv)
		} else {
			d.errorf("cannot decode into a non-pointer value")
		}
	} else {
		fn.fd(d, &fn.i, rv)
	}
	// return rv
}

func (d *Decoder) structFieldNotFound(index int, rvkencname string) {
	// NOTE: rvkencname may be a stringView, so don't pass it to another function.
	if d.h.ErrorIfNoField {
		if index >= 0 {
			d.errorf("no matching struct field found when decoding stream array at index %v", index)
			return
		} else if rvkencname != "" {
			d.errorf("no matching struct field found when decoding stream map with key " + rvkencname)
			return
		}
	}
	d.swallow()
}

func (d *Decoder) arrayCannotExpand(sliceLen, streamLen int) {
	if d.h.ErrorIfNoArrayExpand {
		d.errorf("cannot expand array len during decode from %v to %v", sliceLen, streamLen)
	}
}

func isDecodeable(rv reflect.Value) (rv2 reflect.Value, canDecode bool) {
	switch rv.Kind() {
	case reflect.Array:
		return rv, true
	case reflect.Ptr:
		if !rv.IsNil() {
			return rv.Elem(), true
		}
	case reflect.Slice, reflect.Chan, reflect.Map:
		if !rv.IsNil() {
			return rv, true
		}
	}
	return
}

func (d *Decoder) ensureDecodeable(rv reflect.Value) (rv2 reflect.Value) {
	// decode can take any reflect.Value that is a inherently addressable i.e.
	//   - array
	//   - non-nil chan    (we will SEND to it)
	//   - non-nil slice   (we will set its elements)
	//   - non-nil map     (we will put into it)
	//   - non-nil pointer (we can "update" it)
	rv2, canDecode := isDecodeable(rv)
	if canDecode {
		return
	}
	if !rv.IsValid() {
		d.errorstr(errstrCannotDecodeIntoNil)
		return
	}
	if !rv.CanInterface() {
		d.errorf("cannot decode into a value without an interface: %v", rv)
		return
	}
	rvi := rv2i(rv)
	rvk := rv.Kind()
	d.errorf("cannot decode into value of kind: %v, type: %T, %v", rvk, rvi, rvi)
	return
}

// Possibly get an interned version of a string
//
// This should mostly be used for map keys, where the key type is string.
// This is because keys of a map/struct are typically reused across many objects.
func (d *Decoder) string(v []byte) (s string) {
	if d.is == nil {
		return string(v) // don't return stringView, as we need a real string here.
	}
	s, ok := d.is[string(v)] // no allocation here, per go implementation
	if !ok {
		s = string(v) // new allocation here
		d.is[s] = s
	}
	return s
}

// nextValueBytes returns the next value in the stream as a set of bytes.
func (d *Decoder) nextValueBytes() (bs []byte) {
	d.d.uncacheRead()
	d.r.track()
	d.swallow()
	bs = d.r.stopTrack()
	return
}

func (d *Decoder) rawBytes() []byte {
	// ensure that this is not a view into the bytes
	// i.e. make new copy always.
	bs := d.nextValueBytes()
	bs2 := make([]byte, len(bs))
	copy(bs2, bs)
	return bs2
}

func (d *Decoder) wrapErrstr(v interface{}, err *error) {
	*err = fmt.Errorf("%s decode error [pos %d]: %v", d.hh.Name(), d.r.numread(), v)
}

// --------------------------------------------------

// decSliceHelper assists when decoding into a slice, from a map or an array in the stream.
// A slice can be set from a map or array in stream. This supports the MapBySlice interface.
type decSliceHelper struct {
	d *Decoder
	// ct valueType
	array bool
}

func (d *Decoder) decSliceHelperStart() (x decSliceHelper, clen int) {
	dd := d.d
	ctyp := dd.ContainerType()
	switch ctyp {
	case valueTypeArray:
		x.array = true
		clen = dd.ReadArrayStart()
	case valueTypeMap:
		clen = dd.ReadMapStart() * 2
	default:
		d.errorf("only encoded map or array can be decoded into a slice (%d)", ctyp)
	}
	// x.ct = ctyp
	x.d = d
	return
}

func (x decSliceHelper) End() {
	if x.array {
		x.d.d.ReadArrayEnd()
	} else {
		x.d.d.ReadMapEnd()
	}
}

func (x decSliceHelper) ElemContainerState(index int) {
	if x.array {
		x.d.d.ReadArrayElem()
	} else if index%2 == 0 {
		x.d.d.ReadMapElemKey()
	} else {
		x.d.d.ReadMapElemValue()
	}
}

func decByteSlice(r decReader, clen, maxInitLen int, bs []byte) (bsOut []byte) {
	if clen == 0 {
		return zeroByteSlice
	}
	if len(bs) == clen {
		bsOut = bs
		r.readb(bsOut)
	} else if cap(bs) >= clen {
		bsOut = bs[:clen]
		r.readb(bsOut)
	} else {
		// bsOut = make([]byte, clen)
		len2 := decInferLen(clen, maxInitLen, 1)
		bsOut = make([]byte, len2)
		r.readb(bsOut)
		for len2 < clen {
			len3 := decInferLen(clen-len2, maxInitLen, 1)
			bs3 := bsOut
			bsOut = make([]byte, len2+len3)
			copy(bsOut, bs3)
			r.readb(bsOut[len2:])
			len2 += len3
		}
	}
	return
}

func detachZeroCopyBytes(isBytesReader bool, dest []byte, in []byte) (out []byte) {
	if xlen := len(in); xlen > 0 {
		if isBytesReader || xlen <= scratchByteArrayLen {
			if cap(dest) >= xlen {
				out = dest[:xlen]
			} else {
				out = make([]byte, xlen)
			}
			copy(out, in)
			return
		}
	}
	return in
}

// decInferLen will infer a sensible length, given the following:
//    - clen: length wanted.
//    - maxlen: max length to be returned.
//      if <= 0, it is unset, and we infer it based on the unit size
//    - unit: number of bytes for each element of the collection
func decInferLen(clen, maxlen, unit int) (rvlen int) {
	// handle when maxlen is not set i.e. <= 0
	if clen <= 0 {
		return
	}
	if unit == 0 {
		return clen
	}
	if maxlen <= 0 {
		// no maxlen defined. Use maximum of 256K memory, with a floor of 4K items.
		// maxlen = 256 * 1024 / unit
		// if maxlen < (4 * 1024) {
		// 	maxlen = 4 * 1024
		// }
		if unit < (256 / 4) {
			maxlen = 256 * 1024 / unit
		} else {
			maxlen = 4 * 1024
		}
	}
	if clen > maxlen {
		rvlen = maxlen
	} else {
		rvlen = clen
	}
	return
}

func expandSliceRV(s reflect.Value, st reflect.Type, canChange bool, stElemSize, num, slen, scap int) (
	s2 reflect.Value, scap2 int, changed bool, err string) {
	l1 := slen + num // new slice length
	if l1 < slen {
		err = errmsgExpandSliceOverflow
		return
	}
	if l1 <= scap {
		if s.CanSet() {
			s.SetLen(l1)
		} else if canChange {
			s2 = s.Slice(0, l1)
			scap2 = scap
			changed = true
		} else {
			err = errmsgExpandSliceCannotChange
			return
		}
		return
	}
	if !canChange {
		err = errmsgExpandSliceCannotChange
		return
	}
	scap2 = growCap(scap, stElemSize, num)
	s2 = reflect.MakeSlice(st, l1, scap2)
	changed = true
	reflect.Copy(s2, s)
	return
}

func decReadFull(r io.Reader, bs []byte) (n int, err error) {
	var nn int
	for n < len(bs) && err == nil {
		nn, err = r.Read(bs[n:])
		if nn > 0 {
			if err == io.EOF {
				// leave EOF for next time
				err = nil
			}
			n += nn
		}
	}

	// do not do this - it serves no purpose
	// if n != len(bs) && err == io.EOF { err = io.ErrUnexpectedEOF }
	return
}
