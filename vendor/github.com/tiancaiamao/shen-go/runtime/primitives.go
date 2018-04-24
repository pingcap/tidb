package runtime

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"plugin"
	"time"
)

var allPrimitives []*scmPrimitive = []*scmPrimitive{
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.load-plugin", Required: 1, Function: primLoadPlugin},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.eval-kl", Required: 1, Function: primEvalKL},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.get-time", Required: 1, Function: getTime},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.close", Required: 1, Function: closeStream},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.open", Required: 2, Function: openStream},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.read-byte", Required: 1, Function: primReadByte},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.write-byte", Required: 2, Function: writeByte},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.absvector?", Required: 1, Function: isVector},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.<-address", Required: 2, Function: primVectorGet},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.address->", Required: 3, Function: primVectorSet},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.absvector", Required: 1, Function: primAbsvector},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.str", Required: 1, Function: primStr},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.<=", Required: 2, Function: lessEqual},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.>=", Required: 2, Function: greatEqual},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.<", Required: 2, Function: lessThan},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.>", Required: 2, Function: greatThan},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.error-to-string", Required: 1, Function: primErrorToString},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.simple-error", Required: 1, Function: simpleError},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.=", Required: 2, Function: primEqual},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.-", Required: 2, Function: primNumberSubtract},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.*", Required: 2, Function: primNumberMultiply},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive./", Required: 2, Function: primNumberDivide},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.+", Required: 2, Function: primNumberAdd},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.string->n", Required: 1, Function: primStringToNumber},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.n->string", Required: 1, Function: primNumberToString},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.number?", Required: 1, Function: primIsNumber},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.string?", Required: 1, Function: primIsString},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.pos", Required: 2, Function: pos},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.tlstr", Required: 1, Function: primTailString},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.cn", Required: 2, Function: stringConcat},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.intern", Required: 1, Function: primIntern},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.hd", Required: 1, Function: primHead},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.tl", Required: 1, Function: primTail},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.cons", Required: 2, Function: primCons},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.cons?", Required: 1, Function: primIsPair},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.value", Required: 1, Function: primValue},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.set", Required: 2, Function: primSet},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.not", Required: 1, Function: primNot},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.if", Required: 3, Function: primIf},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.symbol?", Required: 1, Function: primIsSymbol},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.read-file-as-bytelist", Required: 1, Function: primReadFileAsByteList},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.read-file-as-string", Required: 1, Function: primReadFileAsString},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.variable?", Required: 1, Function: primIsVariable},
	&scmPrimitive{scmHead: scmHeadPrimitive, Name: "primitive.integer?", Required: 1, Function: primIsInteger},
}

func primLoadPlugin(args ...Obj) Obj {
	pluginPath := GetString(args[0])
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return MakeError(err.Error())
	}

	entry, err := p.Lookup("Main")
	if err != nil {
		return MakeError(err.Error())
	}

	f, ok := entry.(func())
	if !ok {
		return MakeError("plugin Main should be func(*vm.VM)")
	}

	f()
	return args[0]
}

func primNumberAdd(args ...Obj) Obj {
	x1 := mustNumber(args[0])
	y1 := mustNumber(args[1])
	return MakeNumber(x1.val + y1.val)
}

func primNumberSubtract(args ...Obj) Obj {
	x1 := mustNumber(args[0])
	y1 := mustNumber(args[1])
	return MakeNumber(x1.val - y1.val)
}

func primNumberMultiply(args ...Obj) Obj {
	x1 := mustNumber(args[0])
	y1 := mustNumber(args[1])
	return MakeNumber(x1.val * y1.val)
}

func primNumberDivide(args ...Obj) Obj {
	x1 := mustNumber(args[0])
	y1 := mustNumber(args[1])
	return MakeNumber(x1.val / y1.val)
}

func primIntern(args ...Obj) Obj {
	str := mustString(args[0])
	switch str {
	case "true":
		return True
	case "false":
		return False
	}
	return MakeSymbol(str)
}

func primHead(args ...Obj) Obj {
	return car(args[0])
}

func primTail(args ...Obj) Obj {
	return cdr(args[0])
}

func primIsNumber(args ...Obj) Obj {
	if *args[0] == scmHeadNumber {
		return True
	}
	return False
}

func primStringToNumber(args ...Obj) Obj {
	str := mustString(args[0])
	n := ([]rune(str))[0]
	return MakeInteger(int(n))
}

func primNumberToString(args ...Obj) Obj {
	n := mustInteger(args[0])
	return MakeString(string(rune(n)))
}

func primStr(args ...Obj) Obj {
	switch *args[0] {
	case scmHeadPair:
		// Pair may contain recursive list.
		return MakeError("can't str pair object")
	case scmHeadNull:
		return MakeString("()")
	case scmHeadSymbol:
		str := GetSymbol(args[0])
		return MakeString(str)
	case scmHeadNumber:
		f := mustNumber(args[0])
		if !isPreciseInteger(f.val) {
			return MakeString(fmt.Sprintf("%f", f.val))
		}
		return MakeString(fmt.Sprintf("%d", int(f.val)))
	case scmHeadString:
		return MakeString(fmt.Sprintf(`"%s"`, mustString(args[0])))
	case scmHeadClosure:
		return MakeString("#<closure>")
	case scmHeadPrimitive:
		prim := mustPrimitive(args[0])
		return MakeString(fmt.Sprintf("#<primitive %s>", prim.Name))
	case scmHeadBoolean:
		if args[0] == True {
			return MakeString("true")
		} else if args[0] == False {
			return MakeString("false")
		}
	case scmHeadError:
		e := mustError(args[0])
		return MakeString(e.err)
	case scmHeadStream:
		return MakeString("<stream>")
	case scmHeadRaw:
		return MakeString("#<raw>")
	default:
		return MakeString("primStr unknown")
	}
	return MakeString("wrong input, the object is not atom ...")
}

func stringConcat(args ...Obj) Obj {
	s1 := mustString(args[0])
	s2 := mustString(args[1])
	return MakeString(s1 + s2)
}

func primTailString(args ...Obj) Obj {
	str := mustString(args[0])
	if len(str) == 0 {
		return MakeError("empty string")
	}
	return MakeString(string([]rune(str)[1:]))
}

func pos(args ...Obj) Obj {
	s := []rune(mustString(args[0]))
	n := mustInteger(args[1])
	if n >= len(s) {
		return MakeError(fmt.Sprintf("%d is not valid index for %s", n, string(s)))
	}
	return MakeString(string([]rune(s)[n]))
}

func and(args ...Obj) Obj {
	if args[0] == True && args[1] == True {
		return True
	}
	return False
}

func or(args ...Obj) Obj {
	if args[0] == False || args[1] == False {
		return False
	}
	return True
}

func primSet(args ...Obj) Obj {
	sym := mustSymbol(args[0])
	symVal := &symbolArray[sym.offset]
	symVal.value = args[1]
	return args[1]
}

func primValue(args ...Obj) Obj {
	sym := mustSymbol(args[0])
	symVal := &symbolArray[sym.offset]
	if symVal.value != nil {
		return symVal.value
	}
	return MakeError(fmt.Sprintf("variable %s not bound", symVal.str))
}

func simpleError(args ...Obj) Obj {
	str := mustString(args[0])
	return MakeError(str)
}

func primErrorToString(args ...Obj) Obj {
	e := mustError(args[0])
	return MakeString(e.err)
}

func greatThan(args ...Obj) Obj {
	x := mustNumber(args[0])
	y := mustNumber(args[1])
	if x.val > y.val {
		return True
	}
	return False
}

func lessThan(args ...Obj) Obj {
	x := mustNumber(args[0])
	y := mustNumber(args[1])
	if x.val < y.val {
		return True
	}
	return False
}

func lessEqual(args ...Obj) Obj {
	x := mustNumber(args[0])
	y := mustNumber(args[1])
	if x.val <= y.val {
		return True
	}
	return False
}

func greatEqual(args ...Obj) Obj {
	x := mustNumber(args[0])
	y := mustNumber(args[1])
	if x.val >= y.val {
		return True
	}
	return False
}

func primAbsvector(args ...Obj) Obj {
	n := mustInteger(args[0])
	if n < 0 {
		return MakeError("absvector wrong argument")
	}
	return MakeVector(n)
}

func primVectorSet(args ...Obj) Obj {
	vec := mustVector(args[0])
	off := mustInteger(args[1])
	val := args[2]
	vec[off] = val
	return args[0]
}

func primVectorGet(args ...Obj) Obj {
	vec := mustVector(args[0])
	off := mustInteger(args[1])
	if off >= len(vec) {
		return MakeError(fmt.Sprintf("index %d out of range %d", off, len(vec)))
	}
	ret := vec[off]
	if ret == nil {
		return undefined
	}
	return ret
}

func isVector(args ...Obj) Obj {
	if *args[0] == scmHeadVector {
		return True
	}
	return False
}

func writeByte(args ...Obj) Obj {
	n := mustInteger(args[0])
	s := mustStream(args[1])
	w, ok := s.raw.(io.Writer)
	if !ok {
		return MakeError("stream is not opened in out mode")
	}
	var b [1]byte
	b[0] = byte(n)
	_, err := w.Write(b[:])
	if err != nil {
		return MakeError(err.Error())
	}
	return args[0]
}

func primReadByte(args ...Obj) Obj {
	s := mustStream(args[0])
	r, ok := s.raw.(io.Reader)
	if !ok {
		return MakeError("stream is closed of not opened in in mode")
	}
	var buf [1]byte
	_, err := r.Read(buf[:])
	if err != nil {
		if err == io.EOF {
			return MakeInteger(-1)
		}
		return MakeError(err.Error())
	}
	return MakeInteger(int(buf[0]))
}

func openStream(args ...Obj) Obj {
	file := mustString(args[0])
	var flag int
	mode := GetSymbol(args[1])
	switch mode {
	case "in":
		flag |= os.O_RDONLY
	case "out":
		flag |= os.O_WRONLY | os.O_CREATE
	default:
		flag = os.O_RDWR | os.O_CREATE
	}

	f, err := os.OpenFile(file, flag, 0666)
	if err != nil {
		return MakeError(err.Error())
	}
	return MakeStream(f)
}

func closeStream(args ...Obj) Obj {
	s := mustStream(args[0])
	c := s.raw.(io.Closer)
	c.Close()
	return Nil
}

func getTime(args ...Obj) Obj {
	kind := GetSymbol(args[0])
	switch kind {
	case "unix":
		return MakeNumber(float64(time.Now().Unix()))
	case "run":
		return MakeNumber(time.Since(uptime).Seconds())
	}
	return MakeError(fmt.Sprintf("get-time does not understand the parameter %s", kind))
}

func primIsString(args ...Obj) Obj {
	if *args[0] == scmHeadString {
		return True
	}
	return False
}

func primIsPair(args ...Obj) Obj {
	if *args[0] == scmHeadPair {
		return True
	}
	return False
}

func primNot(args ...Obj) Obj {
	if args[0] == False {
		return True
	} else if args[0] == True {
		return False
	}
	return MakeError("primNot")

}

func primIf(args ...Obj) Obj {
	switch args[0] {
	case True:
		return args[1]
	case False:
		return args[2]
	}
	return MakeError("primIf")
}

func primEqual(args ...Obj) Obj {
	return equal(args[0], args[1])
}

func primCons(args ...Obj) Obj {
	return cons(args[0], args[1])
}

func primIsSymbol(args ...Obj) Obj {
	if IsSymbol(args[0]) {
		return True
	}
	return False
}

func primReadFileAsByteList(args ...Obj) Obj {
	fileName := mustString(args[0])
	buf, err := ioutil.ReadFile(fileName)
	if err != nil {
		return MakeError(err.Error())
	}

	ret := cons(Nil, Nil)
	curr := mustPair(ret)
	for _, b := range buf {
		tmp := cons(MakeInteger(int(b)), Nil)
		curr.cdr = tmp
		curr = mustPair(tmp)
	}
	return cdr(ret)
}

func primReadFileAsString(args ...Obj) Obj {
	fileName := mustString(args[0])
	buf, err := ioutil.ReadFile(fileName)
	if err != nil {
		return MakeError(err.Error())
	}

	return MakeString(string(buf))
}

func primIsVariable(args ...Obj) Obj {
	if *args[0] != scmHeadSymbol {
		return False
	}

	sym := GetSymbol(args[0])
	if len(sym) == 0 || sym[0] < 'A' || sym[0] > 'Z' {
		return False
	}
	return True
}

func primIsInteger(args ...Obj) Obj {
	if *args[0] != scmHeadNumber {
		return False
	}
	f := mustNumber(args[0]).val
	if isPreciseInteger(f) {
		return True
	}
	return False
}

func primEvalKL(args ...Obj) Obj {
	tmp := auxVM.Get()
	result := tmp.Eval(args[0])
	auxVM.Put(tmp)
	return result
}
