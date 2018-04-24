package runtime

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Go doesn't provide MACRO like C, but the compiler optimization can eliminate dead code "if false XX".
const enableDebug = false

type statusType int

const (
	statusNormal statusType = iota
	statusException
	statusHalt
)

type instFunc func(*VM)
type Code []instFunc

type VM struct {
	stack []Obj
	top   int // stack top

	env      []Obj // persistent environment
	volatile []Obj // volatile environment
	envMark  int   // volatile[envMark: len(volatile)] is current env

	pc        int       // pc register refer to the position in current code
	savedAddr []address // saved return address

	// jumpBuf is used to implement exception, similar to setjmp/longjmp in C.
	cc []jumpBuf

	code   []instFunc
	status statusType
}

type jumpBuf struct {
	address
	savedAddrPos  int
	savedStackTop int
	savedEnvTop   int
	closure       Obj
}

// address is the information to be saved before apply a closure.
type address struct {
	pc      int
	code    Code
	env     []Obj
	envMark int
}

const initStackSize = 128

type pool struct {
	sync.Mutex
	data []*VM
}

func (p *pool) Get() *VM {
	p.Lock()
	defer p.Unlock()

	if len(p.data) == 0 {
		return newVM()
	}
	ret := p.data[0]
	p.data = p.data[1:]
	return ret
}

func (p *pool) Put(v *VM) {
	p.Lock()
	p.data = append(p.data, v)
	p.Unlock()
}

var auxVM pool
var stackMarkDummyValue int
var stackMark = MakeRaw(&stackMarkDummyValue)
var nativeFunc = make(map[string]*scmPrimitive)

func NewVM() *VM {
	m := newVM()
	return m
}

func newVM() *VM {
	m := &VM{
		stack:    make([]Obj, initStackSize),
		env:      make([]Obj, 0, 256),
		volatile: make([]Obj, 0, 256),
	}

	return m
}

func RegistNativeCall(name string, arity int, f func(...Obj) Obj) {
	nativeFunc[name] = makePrimitive(name, arity, f)
}

func (m *VM) Run(code Code) Obj {
	m.setup(code)

	// May be the fastest dispatch method in Go.
Dispatch:
	m.status = statusNormal
	for m.status == statusNormal {
		m.code[m.pc](m)
	}

	if m.status == statusException {
		if len(m.cc) == 0 {
			err := m.stack[m.top-1]
			m.Reset()
			return err
		}
		m.handleException()
		goto Dispatch
	}
	return m.done()
}

func (m *VM) setup(code Code) {
	m.code = code
	m.pc = 0
	m.stackPush(stackMark)
	m.savedAddr = append(m.savedAddr, address{pc: len(code) - 1, code: code})
}

func (m *VM) done() Obj {
	if m.top != 1 || len(m.savedAddr) != 0 || m.envMark != 0 {
		if enableDebug {
			m.Debug()
		}
		panic("m in wrong status")
	}
	m.top--
	return m.stack[m.top]
}

func (m *VM) handleException() {
	// clear jmpBuf
	jmpBuf := m.cc[len(m.cc)-1]
	m.cc = m.cc[:len(m.cc)-1]
	// pop trap-error handler, prepare for call.
	value := m.stack[m.top-1]
	m.top = jmpBuf.savedStackTop
	m.stackPush(stackMark)
	m.stackPush(value)
	// recover savedAddr
	m.savedAddr = m.savedAddr[:jmpBuf.savedAddrPos]
	m.savedAddr = append(m.savedAddr, jmpBuf.address)
	// longjmp... tail apply
	closure := mustClosure(jmpBuf.closure)
	m.code = closure.code
	m.pc = 0
	m.env = closure.env
	m.envMark = jmpBuf.savedEnvTop
	m.volatile = m.volatile[:m.envMark]
}

func opSetJmp(n int) instFunc {
	return func(m *VM) {
		m.pc++
		if enableDebug {
			debugf("SETJMP\n")
		}

		m.top--
		cc := jumpBuf{
			address: address{
				pc:      m.pc + n,
				code:    m.code,
				env:     m.env,
				envMark: m.envMark,
			},
			savedAddrPos:  len(m.savedAddr),
			savedStackTop: m.top,
			closure:       m.stack[m.top],
			savedEnvTop:   len(m.volatile),
		}
		m.cc = append(m.cc, cc)
	}
}

func opClearJmp(m *VM) {
	m.pc++
	m.cc = m.cc[:len(m.cc)-1]
}

func opConst(o Obj) instFunc {
	return func(m *VM) {
		m.pc++
		m.stackPush(o)
		if enableDebug {
			debugf("CONST %s\n", ObjString(o))
		}
	}
}

func opAccess(n int) instFunc {
	return func(m *VM) {
		m.pc++
		if n+m.envMark < len(m.volatile) {
			// get value from volatile environment
			v := m.volatile[len(m.volatile)-1-n]
			m.stackPush(v)
			if enableDebug {
				debugf("ACCESS %d, get %s\n", n, ObjString(v))
			}
		} else {
			// get value from persistent environment
			xx := n - (len(m.volatile) - m.envMark)
			v := m.env[len(m.env)-1-xx]
			m.stackPush(v)
			if enableDebug {
				debugf("ACCESS %d from env, get %s\n", n, ObjString(v))
			}
		}
	}
}

func opFreeze(n int) instFunc {
	return func(m *VM) {
		m.pc++
		// create closure directly
		// nearly the same with grab, but if need zero arguments.
		env := m.envClose()
		proc := makeClosure(m.code[m.pc:], m.envClose())
		if enableDebug {
			debugf("FREEZE len(env)=%d\n", len(env))
		}
		m.stackPush(proc)
		m.pc += n
	}
}

func opMark(m *VM) {
	m.pc++
	m.stackPush(stackMark)
	if enableDebug {
		debugln("MARK")
	}
}

func opGrab(m *VM) {
	m.pc++
	m.top--
	if v := m.stack[m.top]; v == stackMark {
		// make closure if there are not enough arguments
		proc := makeClosure(m.code[m.pc-1:], m.envClose())
		m.stackPush(proc)

		// return to saved address
		savedAddr := m.savedAddr[len(m.savedAddr)-1]
		m.savedAddr = m.savedAddr[:len(m.savedAddr)-1]
		m.code = savedAddr.code
		m.pc = savedAddr.pc
		m.env = savedAddr.env
		m.volatile = m.volatile[:m.envMark]
		m.envMark = savedAddr.envMark
		if enableDebug {
			debugln("GRAB not enough argument")
		}
	} else {
		// grab data from stack to volatile env
		m.volatile = append(m.volatile, v)
		if enableDebug {
			debugf("GRAB %s\n", ObjString(v))
		}
	}
}

func opReturn(m *VM) {
	// stack[top-1] is the result, so should check top-2
	if m.stack[m.top-2] == stackMark {
		savedAddr := m.savedAddr[len(m.savedAddr)-1]
		m.savedAddr = m.savedAddr[:len(m.savedAddr)-1]

		m.code = savedAddr.code
		m.pc = savedAddr.pc
		m.env = savedAddr.env
		m.top--
		m.stack[m.top-1] = m.stack[m.top]
		m.volatile = m.volatile[:m.envMark]
		m.envMark = savedAddr.envMark
		if enableDebug {
			debugf("RETURN %d %d %s\n", m.top, savedAddr.envMark, ObjString(m.stack[m.top-1]))
		}
	} else {
		if enableDebug {
			debugf("RETURN more argument\n")
		}
		// more arguments, continue the beta-reduce.
		// similar to tail apply
		m.top--
		obj := m.stack[m.top]
		closure := mustClosure(obj)
		m.code = closure.code
		m.pc = 0
		m.env = closure.env
		m.volatile = m.volatile[:m.envMark]
	}
}

func opTailApply(m *VM) {
	if enableDebug {
		debugln("TAILAPPLY")
	}
	m.top--
	obj := m.stack[m.top]
	closure := mustClosure(obj)
	// The only different with Apply is that TailApply doesn't save return address.
	m.code = closure.code
	m.pc = 0
	m.env = closure.env
	m.volatile = m.volatile[:m.envMark]
}

func opApply(m *VM) {
	m.top--
	m.pc++
	if enableDebug {
		debugf("APPLY %d\n", m.top)
	}
	obj := m.stack[m.top]
	closure := mustClosure(obj)
	// save return address
	m.savedAddr = append(m.savedAddr, address{m.pc, m.code, m.env, m.envMark})
	// set pc to closure code
	m.code = closure.code
	m.pc = 0
	m.env = closure.env
	m.envMark = len(m.volatile)
}

func opPop(m *VM) {
	m.pc++
	if enableDebug {
		debugln("POP")
	}
	m.top--
}

func opDefun(m *VM) {
	m.pc++
	symbol := m.stack[m.top-1]
	function := m.stack[m.top-2]
	bindSymbolFunc(symbol, function)
	m.top--
	m.stack[m.top-1] = m.stack[m.top]
	if enableDebug {
		debugf("DEFUN %s\n", ObjString(symbol))
	}
}

func opGetF(m *VM) {
	m.pc++
	function := GetSymbolFunc(m.stack[m.top-1])
	if enableDebug {
		debugf("GETF %s\n", GetSymbol(m.stack[m.top-1]))
	}
	if function != nil {
		m.stack[m.top-1] = function
	} else {
		m.stack[m.top-1] = MakeError("unknown function:" + GetSymbol(m.stack[m.top-1]))
		m.status = statusException
	}
}

func opJF(n int) instFunc {
	return func(m *VM) {
		m.pc++
		switch m.stack[m.top-1] {
		case False:
			if enableDebug {
				debugln("JF false")
			}
			m.top--
			m.pc += n
			return
		case True:
			if enableDebug {
				debugln("JF true")
			}
			m.top--
		default:
			// TODO: So what?
			m.stack[m.top-1] = MakeError("test condition need to be boolean")
			m.status = statusException
		}
	}
}

func opJMP(n int) instFunc {
	return func(m *VM) {
		m.pc++
		if enableDebug {
			debugln("JMP")
		}
		m.pc += n
	}
}

func opHalt(m *VM) {
	if enableDebug {
		debugln("HALT")
	}
	m.status = statusHalt
}

func opNativeCall(arity int) instFunc {
	return func(m *VM) {
		m.pc++
		method := mustString(m.stack[m.top-arity])
		if enableDebug {
			debugf("NativeCall %s\n", method)
		}
		proc, ok := nativeFunc[method]
		if !ok {
			m.stack[m.top-1] = MakeError("unknown native function:" + method)
			m.status = statusException
			return
		}
		// Note the invariance arity = len(method + args), so arity-1 = proc.Required
		if arity-1 != proc.Required {
			m.stack[m.top-1] = MakeError("wrong arity for native " + method)
			m.status = statusException
			return
		}
		args := m.stack[m.top-proc.Required : m.top]
		result := proc.Function(args...)

		m.stack[m.top-arity] = result
		m.top = m.top - proc.Required
		if IsError(result) {
			m.status = statusException
		}
	}
}

func (m *VM) envClose() []Obj {
	lenVolatile := len(m.volatile) - m.envMark
	if len(m.env) > 0 || lenVolatile > 0 {
		env := make([]Obj, 0, len(m.env)+lenVolatile)
		env = append(env, m.env...)
		env = append(env, m.volatile[m.envMark:len(m.volatile)]...)
		return env
	}
	return nil
}

func (m *VM) stackPush(o Obj) {
	if m.top == len(m.stack) {
		stack := make([]Obj, len(m.stack)*2)
		copy(stack, m.stack)
		m.stack = stack
	}
	m.stack[m.top] = o
	m.top++
}

func (m *VM) Reset() {
	m.stack = m.stack[:initStackSize]
	m.top = 0
	m.env = m.env[:0]
	m.savedAddr = m.savedAddr[:0]
	m.cc = nil
}

func (m *VM) Debug() {
	debugln("pc:", m.pc)
	debugln("top:", m.top)
	debugln("envMark:", m.envMark)
	debugln("stack:")
	for i := m.top - 1; i >= 0; i-- {
		if m.stack[i] == stackMark {
			debugln("MARK")
		} else {
			debugln(ObjString(m.stack[i]))
		}
	}
}

var prototype *VM

func klToSexpByteCode(klambda Obj) Obj {
	// TODO: Better way to do it?
	// tailcall (kl->bytecode klambda)
	var a assember
	a.CONST(klambda)
	a.CONST(MakeSymbol("kl->bytecode"))
	a.GetF()
	a.TAILAPPLY()
	a.HALT()

	code := a.Compile()
	return prototype.Run(code)
}

func klToCode(klambda Obj) (Code, error) {
	bc := klToSexpByteCode(klambda)
	if IsError(bc) || bc == Nil {
		return nil, errors.New("klToByteCode return some thing wrong:" + ObjString(bc))
	}
	var a assember
	err := a.FromSexp(bc)
	if err != nil {
		return nil, err
	}
	code := a.Compile()
	return code, nil
}

func (m *VM) Eval(sexp Obj) (res Obj) {
	defer func() {
		if r := recover(); r != nil {
			m.Reset()
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			fmt.Println("Recovered in Eval:", ObjString(sexp))
			fmt.Println(string(buf[:n]))
			res = MakeError("panic")
		}
	}()

	code, err := klToCode(sexp)
	if err != nil {
		return MakeError(err.Error())
	}
	res = m.Run(code)
	return
}

func shenRead(input string) (res Obj) {
	r := strings.NewReader(input)
	stream := MakeStream(r)
	sexp := Cons(MakeSymbol("read"), Cons(stream, Nil))
	return Eval(sexp)
}

func shenEval(obj Obj) (res Obj) {
	sexp := Cons(MakeSymbol("eval"), Cons(obj, Nil))
	return Eval(sexp)
}

func EvalString(input string) Obj {
	obj := shenRead(input)
	if IsError(obj) {
		return obj
	}
	return Eval(obj)
}

func Eval(sexp Obj) Obj {
	vm := auxVM.Get()
	defer auxVM.Put(vm)
	return vm.Eval(sexp)
}

func BootstrapMin() {
	prototype.mustLoadBytecode(MakeString("primitive.bc"))
	prototype.mustLoadBytecode(MakeString("de-bruijn.bc"))
	prototype.mustLoadBytecode(MakeString("compile.bc"))
	prototype.mustLoadBytecode(MakeString("core.bc"))
	prototype.mustLoadBytecode(MakeString("sys.bc"))
	prototype.mustLoadBytecode(MakeString("yacc.bc"))
	prototype.mustLoadBytecode(MakeString("override.bc"))
}

func BootstrapCora() {
	prototype.mustLoadBytecode(MakeString("primitive.bc"))
	prototype.mustLoadBytecode(MakeString("de-bruijn.bc"))
	prototype.mustLoadBytecode(MakeString("compile.bc"))
	prototype.mustLoadBytecode(MakeString("core.bc"))
	prototype.mustLoadBytecode(MakeString("sys.bc"))
	prototype.mustLoadBytecode(MakeString("yacc.bc"))
	prototype.mustLoadBytecode(MakeString("toplevel.bc"))
	prototype.mustLoadBytecode(MakeString("reader.bc"))
	prototype.mustLoadBytecode(MakeString("track.bc"))
	prototype.mustLoadBytecode(MakeString("load.bc"))
	prototype.mustLoadBytecode(MakeString("writer.bc"))
	prototype.mustLoadBytecode(MakeString("macros.bc"))
	prototype.mustLoadBytecode(MakeString("declarations.bc"))
}

func BootstrapShen() {
	BootstrapMin()
	prototype.mustLoadBytecode(MakeString("toplevel.bc"))
	prototype.mustLoadBytecode(MakeString("dict.bc"))
	prototype.mustLoadBytecode(MakeString("sequent.bc"))
	prototype.mustLoadBytecode(MakeString("reader.bc"))
	prototype.mustLoadBytecode(MakeString("prolog.bc"))
	prototype.mustLoadBytecode(MakeString("track.bc"))
	prototype.mustLoadBytecode(MakeString("load.bc"))
	prototype.mustLoadBytecode(MakeString("writer.bc"))
	prototype.mustLoadBytecode(MakeString("macros.bc"))
	prototype.mustLoadBytecode(MakeString("declarations.bc"))
	prototype.mustLoadBytecode(MakeString("t-star.bc"))
	prototype.mustLoadBytecode(MakeString("types.bc"))
}

var Boot string

func (m *VM) mustLoadBytecode(args ...Obj) {
	res := loadBytecode(args...)
	if IsError(res) {
		panic(ObjString(res) + ObjString(args[0]))
	}
}

func loadBytecode(args ...Obj) Obj {
	fileName := GetString(args[0])
	var f io.ReadCloser
	var err error
	if Boot != "" {
		filePath := path.Join(Boot, "bytecode", fileName)
		f, err = os.Open(filePath)
	} else {
		filePath := path.Join("/bytecode", fileName)
		f, err = FS(false).Open(filePath)
	}
	if err != nil {
		return MakeError(err.Error())
	}
	defer f.Close()

	r := NewSexpReader(f)
	obj, err := r.Read()
	for err == nil {
		var a assember
		if err1 := a.FromSexp(obj); err1 != nil {
			return MakeError(err1.Error())
		}
		code := a.Compile()

		tmp := auxVM.Get()
		res := tmp.Run(code)
		auxVM.Put(tmp)

		if IsError(res) {
			return res
		}
		obj, err = r.Read()
	}
	if err != io.EOF {
		return MakeError(err.Error())
	}
	return args[0]
}

var stdDebug io.Writer

func debugf(format string, a ...interface{}) {
	fmt.Fprintf(stdDebug, format, a...)
}

func debugln(a ...interface{}) {
	fmt.Fprintln(stdDebug, a...)
}

func initSymbolTable() {
	dir, _ := os.Getwd()
	primSet(MakeSymbol("*stinput*"), MakeStream(os.Stdin))
	primSet(MakeSymbol("*stoutput*"), MakeStream(os.Stdout))
	primSet(MakeSymbol("*home-directory*"), MakeString(dir))
	primSet(MakeSymbol("*language*"), MakeString("Go"))
	primSet(MakeSymbol("*implementation*"), MakeString("bytecode"))
	primSet(MakeSymbol("*relase*"), MakeString(runtime.Version()))
	primSet(MakeSymbol("*os*"), MakeString(runtime.GOOS))
	primSet(MakeSymbol("*porters*"), MakeString("Arthur Mao"))
	primSet(MakeSymbol("*port*"), MakeString("0.0.1"))
	primSet(MakeSymbol("*package-path*"), MakeString(PackagePath()))
}

func init() {
	uptime = time.Now()
	tmp1 := &scmBoolean{scmHeadBoolean, false}
	False = Obj(&tmp1.scmHead)

	tmp2 := &scmBoolean{scmHeadBoolean, true}
	True = Obj(&tmp2.scmHead)

	tmp3 := &scmPair{scmHeadNull, nil, nil}
	Nil = Obj(&tmp3.scmHead)

	var tmp4 int
	undefined = MakeRaw(&tmp4)

	for i := 0; i < intConstCount; i++ {
		intConst[i] = makeInteger(i)
	}

	symbolArray = make([]symbolArrayObj, 0, 4096)
	trieRoot = &trieNode{}

	if enableDebug {
		stdDebug, _ = os.OpenFile("debug.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	}

	initSymbolTable()

	for _, v := range allPrimitives {
		RegistNativeCall(v.Name, v.Required, v.Function)
	}

	prototype = newVM()
}
