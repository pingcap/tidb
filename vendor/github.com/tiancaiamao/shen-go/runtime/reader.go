package runtime

import (
	"bufio"
	"io"
	"strconv"
	"unicode"
)

type SexpReader struct {
	reader *bufio.Reader
	buf    []rune
}

func NewSexpReader(r io.Reader) *SexpReader {
	return &SexpReader{
		reader: bufio.NewReader(r),
	}
}

func (r *SexpReader) Read() (Obj, error) {
	b, err := peekFirstRune(r.reader)
	if err != nil {
		return Nil, err
	}

	switch b {
	case rune('^'):
		return r.readerMacro()
	case rune('('):
		return r.readSexp()
	case rune('"'):
		return r.readString()
	}

	r.resetBuf()
	r.appendBuf(b)
	b, _, err = r.reader.ReadRune()
	for err == nil {
		if notSymbolChar(b) {
			r.reader.UnreadRune()
			break
		}
		r.appendBuf(b)
		b, _, err = r.reader.ReadRune()
	}

	return tokenToObj(string(r.buf)), err
}

func (r *SexpReader) readerMacro() (Obj, error) {
	rune, _, err := r.reader.ReadRune()
	if err != nil {
		return Nil, err
	}
	obj, err := r.Read()
	if err != nil {
		return obj, err
	}

	switch rune {
	case '\'': // quote macro
		return quoteMacro(obj)
	}

	return obj, nil
}

func RconsForm(o Obj) Obj {
	return rconsForm(o)
}

func rconsForm(o Obj) Obj {
	if *o == scmHeadPair {
		return cons(MakeSymbol("cons"),
			cons(rconsForm(car(o)),
				cons(rconsForm(cdr(o)), Nil)))
	}
	return o
}

func quoteMacro(o Obj) (Obj, error) {
	return rconsForm(o), nil
}

func (r *SexpReader) readString() (Obj, error) {
	r.resetBuf()
	b, _, err := r.reader.ReadRune()
	for err == nil && b != rune('"') {
		r.appendBuf(b)
		b, _, err = r.reader.ReadRune()
	}
	return MakeString(string(r.buf)), err
}

func (r *SexpReader) readSexp() (Obj, error) {
	ret := Nil
	b, err := peekFirstRune(r.reader)
	for err == nil && b != rune(')') {
		var obj Obj
		r.reader.UnreadRune()
		obj, err = r.Read()
		if err == nil {
			ret = cons(obj, ret)
			b, err = peekFirstRune(r.reader)
		}
	}
	return reverse(ret), err
}

func (r *SexpReader) resetBuf() {
	r.buf = r.buf[:0]
}

func (r *SexpReader) appendBuf(b rune) {
	r.buf = append(r.buf, b)
}

func peekFirstRune(r *bufio.Reader) (rune, error) {
	b, _, err := r.ReadRune()
	for err == nil && unicode.IsSpace(b) {
		b, _, err = r.ReadRune()
	}
	return b, err
}

func notSymbolChar(c rune) bool {
	return unicode.IsSpace(c) || c == '(' || c == '"' || c == ')'
}

func tokenToObj(str string) Obj {
	switch str {
	case "true":
		return True
	case "false":
		return False
	}
	if v, err := strconv.ParseFloat(str, 64); err == nil {
		return MakeNumber(v)
	}
	return MakeSymbol(str)
}
