// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This package provides Rapid Type Analysis (RTA) for Go, a fast
// algorithm for call graph construction and discovery of reachable code
// (and hence dead code) and runtime types.  The algorithm was first
// described in:
//
// David F. Bacon and Peter F. Sweeney. 1996.
// Fast static analysis of C++ virtual function calls. (OOPSLA '96)
// http://doi.acm.org/10.1145/236337.236371
//
// The algorithm uses dynamic programming to tabulate the cross-product
// of the set of known "address taken" functions with the set of known
// dynamic calls of the same type.  As each new address-taken function
// is discovered, call graph edges are added from each known callsite,
// and as each new call site is discovered, call graph edges are added
// from it to each known address-taken function.
//
// A similar approach is used for dynamic calls via interfaces: it
// tabulates the cross-product of the set of known "runtime types",
// i.e. types that may appear in an interface value, or be derived from
// one via reflection, with the set of known "invoke"-mode dynamic
// calls.  As each new "runtime type" is discovered, call edges are
// added from the known call sites, and as each new call site is
// discovered, call graph edges are added to each compatible
// method.
//
// In addition, we must consider all exported methods of any runtime type
// as reachable, since they may be called via reflection.
//
// Each time a newly added call edge causes a new function to become
// reachable, the code of that function is analyzed for more call sites,
// address-taken functions, and runtime types.  The process continues
// until a fixed point is achieved.
//
// The resulting call graph is less precise than one produced by pointer
// analysis, but the algorithm is much faster.  For example, running the
// cmd/callgraph tool on its own source takes ~2.1s for RTA and ~5.4s
// for points-to analysis.
//
package rta // import "golang.org/x/tools/go/callgraph/rta"

// TODO(adonovan): test it by connecting it to the interpreter and
// replacing all "unreachable" functions by a special intrinsic, and
// ensure that that intrinsic is never called.

import (
	"fmt"
	"go/types"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/types/typeutil"
)

// A Result holds the results of Rapid Type Analysis, which includes the
// set of reachable functions/methods, runtime types, and the call graph.
//
type Result struct {
	// CallGraph is the discovered callgraph.
	// It does not include edges for calls made via reflection.
	CallGraph *callgraph.Graph

	// Reachable contains the set of reachable functions and methods.
	// This includes exported methods of runtime types, since
	// they may be accessed via reflection.
	// The value indicates whether the function is address-taken.
	//
	// (We wrap the bool in a struct to avoid inadvertent use of
	// "if Reachable[f] {" to test for set membership.)
	Reachable map[*ssa.Function]struct{ AddrTaken bool }

	// RuntimeTypes contains the set of types that are needed at
	// runtime, for interfaces or reflection.
	//
	// The value indicates whether the type is inaccessible to reflection.
	// Consider:
	// 	type A struct{B}
	// 	fmt.Println(new(A))
	// Types *A, A and B are accessible to reflection, but the unnamed
	// type struct{B} is not.
	RuntimeTypes typeutil.Map
}

// Working state of the RTA algorithm.
type rta struct {
	result *Result

	prog *ssa.Program

	worklist []*ssa.Function // list of functions to visit

	// addrTakenFuncsBySig contains all address-taken *Functions, grouped by signature.
	// Keys are *types.Signature, values are map[*ssa.Function]bool sets.
	addrTakenFuncsBySig typeutil.Map

	// dynCallSites contains all dynamic "call"-mode call sites, grouped by signature.
	// Keys are *types.Signature, values are unordered []ssa.CallInstruction.
	dynCallSites typeutil.Map

	// invokeSites contains all "invoke"-mode call sites, grouped by interface.
	// Keys are *types.Interface (never *types.Named),
	// Values are unordered []ssa.CallInstruction sets.
	invokeSites typeutil.Map

	// The following two maps together define the subset of the
	// m:n "implements" relation needed by the algorithm.

	// concreteTypes maps each concrete type to the set of interfaces that it implements.
	// Keys are types.Type, values are unordered []*types.Interface.
	// Only concrete types used as MakeInterface operands are included.
	concreteTypes typeutil.Map

	// interfaceTypes maps each interface type to
	// the set of concrete types that implement it.
	// Keys are *types.Interface, values are unordered []types.Type.
	// Only interfaces used in "invoke"-mode CallInstructions are included.
	interfaceTypes typeutil.Map
}

// addReachable marks a function as potentially callable at run-time,
// and ensures that it gets processed.
func (r *rta) addReachable(f *ssa.Function, addrTaken bool) {
	reachable := r.result.Reachable
	n := len(reachable)
	v := reachable[f]
	if addrTaken {
		v.AddrTaken = true
	}
	reachable[f] = v
	if len(reachable) > n {
		// First time seeing f.  Add it to the worklist.
		r.worklist = append(r.worklist, f)
	}
}

// addEdge adds the specified call graph edge, and marks it reachable.
// addrTaken indicates whether to mark the callee as "address-taken".
func (r *rta) addEdge(site ssa.CallInstruction, callee *ssa.Function, addrTaken bool) {
	r.addReachable(callee, addrTaken)

	if g := r.result.CallGraph; g != nil {
		if site.Parent() == nil {
			panic(site)
		}
		from := g.CreateNode(site.Parent())
		to := g.CreateNode(callee)
		callgraph.AddEdge(from, site, to)
	}
}

// ---------- addrTakenFuncs × dynCallSites ----------

// visitAddrTakenFunc is called each time we encounter an address-taken function f.
func (r *rta) visitAddrTakenFunc(f *ssa.Function) {
	// Create two-level map (Signature -> Function -> bool).
	S := f.Signature
	funcs, _ := r.addrTakenFuncsBySig.At(S).(map[*ssa.Function]bool)
	if funcs == nil {
		funcs = make(map[*ssa.Function]bool)
		r.addrTakenFuncsBySig.Set(S, funcs)
	}
	if !funcs[f] {
		// First time seeing f.
		funcs[f] = true

		// If we've seen any dyncalls of this type, mark it reachable,
		// and add call graph edges.
		sites, _ := r.dynCallSites.At(S).([]ssa.CallInstruction)
		for _, site := range sites {
			r.addEdge(site, f, true)
		}
	}
}

// visitDynCall is called each time we encounter a dynamic "call"-mode call.
func (r *rta) visitDynCall(site ssa.CallInstruction) {
	S := site.Common().Signature()

	// Record the call site.
	sites, _ := r.dynCallSites.At(S).([]ssa.CallInstruction)
	r.dynCallSites.Set(S, append(sites, site))

	// For each function of signature S that we know is address-taken,
	// add an edge and mark it reachable.
	funcs, _ := r.addrTakenFuncsBySig.At(S).(map[*ssa.Function]bool)
	for g := range funcs {
		r.addEdge(site, g, true)
	}
}

// ---------- concrete types × invoke sites ----------

// addInvokeEdge is called for each new pair (site, C) in the matrix.
func (r *rta) addInvokeEdge(site ssa.CallInstruction, C types.Type) {
	// Ascertain the concrete method of C to be called.
	imethod := site.Common().Method
	cmethod := r.prog.MethodValue(r.prog.MethodSets.MethodSet(C).Lookup(imethod.Pkg(), imethod.Name()))
	r.addEdge(site, cmethod, true)
}

// visitInvoke is called each time the algorithm encounters an "invoke"-mode call.
func (r *rta) visitInvoke(site ssa.CallInstruction) {
	I := site.Common().Value.Type().Underlying().(*types.Interface)

	// Record the invoke site.
	sites, _ := r.invokeSites.At(I).([]ssa.CallInstruction)
	r.invokeSites.Set(I, append(sites, site))

	// Add callgraph edge for each existing
	// address-taken concrete type implementing I.
	for _, C := range r.implementations(I) {
		r.addInvokeEdge(site, C)
	}
}

// ---------- main algorithm ----------

// visitFunc processes function f.
func (r *rta) visitFunc(f *ssa.Function) {
	var space [32]*ssa.Value // preallocate space for common case

	for _, b := range f.Blocks {
		for _, instr := range b.Instrs {
			rands := instr.Operands(space[:0])

			switch instr := instr.(type) {
			case ssa.CallInstruction:
				call := instr.Common()
				if call.IsInvoke() {
					r.visitInvoke(instr)
				} else if g := call.StaticCallee(); g != nil {
					r.addEdge(instr, g, false)
				} else if _, ok := call.Value.(*ssa.Builtin); !ok {
					r.visitDynCall(instr)
				}

				// Ignore the call-position operand when
				// looking for address-taken Functions.
				// Hack: assume this is rands[0].
				rands = rands[1:]

			case *ssa.MakeInterface:
				r.addRuntimeType(instr.X.Type(), false)
			}

			// Process all address-taken functions.
			for _, op := range rands {
				if g, ok := (*op).(*ssa.Function); ok {
					r.visitAddrTakenFunc(g)
				}
			}
		}
	}
}

// Analyze performs Rapid Type Analysis, starting at the specified root
// functions.  It returns nil if no roots were specified.
//
// If buildCallGraph is true, Result.CallGraph will contain a call
// graph; otherwise, only the other fields (reachable functions) are
// populated.
//
func Analyze(roots []*ssa.Function, buildCallGraph bool) *Result {
	if len(roots) == 0 {
		return nil
	}

	r := &rta{
		result: &Result{Reachable: make(map[*ssa.Function]struct{ AddrTaken bool })},
		prog:   roots[0].Prog,
	}

	if buildCallGraph {
		// TODO(adonovan): change callgraph API to eliminate the
		// notion of a distinguished root node.  Some callgraphs
		// have many roots, or none.
		r.result.CallGraph = callgraph.New(roots[0])
	}

	hasher := typeutil.MakeHasher()
	r.result.RuntimeTypes.SetHasher(hasher)
	r.addrTakenFuncsBySig.SetHasher(hasher)
	r.dynCallSites.SetHasher(hasher)
	r.invokeSites.SetHasher(hasher)
	r.concreteTypes.SetHasher(hasher)
	r.interfaceTypes.SetHasher(hasher)

	// Visit functions, processing their instructions, and adding
	// new functions to the worklist, until a fixed point is
	// reached.
	var shadow []*ssa.Function // for efficiency, we double-buffer the worklist
	r.worklist = append(r.worklist, roots...)
	for len(r.worklist) > 0 {
		shadow, r.worklist = r.worklist, shadow[:0]
		for _, f := range shadow {
			r.visitFunc(f)
		}
	}
	return r.result
}

// interfaces(C) returns all currently known interfaces implemented by C.
func (r *rta) interfaces(C types.Type) []*types.Interface {
	// Ascertain set of interfaces C implements
	// and update 'implements' relation.
	var ifaces []*types.Interface
	r.interfaceTypes.Iterate(func(I types.Type, concs interface{}) {
		if I := I.(*types.Interface); types.Implements(C, I) {
			concs, _ := concs.([]types.Type)
			r.interfaceTypes.Set(I, append(concs, C))
			ifaces = append(ifaces, I)
		}
	})
	r.concreteTypes.Set(C, ifaces)
	return ifaces
}

// implementations(I) returns all currently known concrete types that implement I.
func (r *rta) implementations(I *types.Interface) []types.Type {
	var concs []types.Type
	if v := r.interfaceTypes.At(I); v != nil {
		concs = v.([]types.Type)
	} else {
		// First time seeing this interface.
		// Update the 'implements' relation.
		r.concreteTypes.Iterate(func(C types.Type, ifaces interface{}) {
			if types.Implements(C, I) {
				ifaces, _ := ifaces.([]*types.Interface)
				r.concreteTypes.Set(C, append(ifaces, I))
				concs = append(concs, C)
			}
		})
		r.interfaceTypes.Set(I, concs)
	}
	return concs
}

// addRuntimeType is called for each concrete type that can be the
// dynamic type of some interface or reflect.Value.
// Adapted from needMethods in go/ssa/builder.go
//
func (r *rta) addRuntimeType(T types.Type, skip bool) {
	if prev, ok := r.result.RuntimeTypes.At(T).(bool); ok {
		if skip && !prev {
			r.result.RuntimeTypes.Set(T, skip)
		}
		return
	}
	r.result.RuntimeTypes.Set(T, skip)

	mset := r.prog.MethodSets.MethodSet(T)

	if _, ok := T.Underlying().(*types.Interface); !ok {
		// T is a new concrete type.
		for i, n := 0, mset.Len(); i < n; i++ {
			sel := mset.At(i)
			m := sel.Obj()

			if m.Exported() {
				// Exported methods are always potentially callable via reflection.
				r.addReachable(r.prog.MethodValue(sel), true)
			}
		}

		// Add callgraph edge for each existing dynamic
		// "invoke"-mode call via that interface.
		for _, I := range r.interfaces(T) {
			sites, _ := r.invokeSites.At(I).([]ssa.CallInstruction)
			for _, site := range sites {
				r.addInvokeEdge(site, T)
			}
		}
	}

	// Precondition: T is not a method signature (*Signature with Recv()!=nil).
	// Recursive case: skip => don't call makeMethods(T).
	// Each package maintains its own set of types it has visited.

	var n *types.Named
	switch T := T.(type) {
	case *types.Named:
		n = T
	case *types.Pointer:
		n, _ = T.Elem().(*types.Named)
	}
	if n != nil {
		owner := n.Obj().Pkg()
		if owner == nil {
			return // built-in error type
		}
	}

	// Recursion over signatures of each exported method.
	for i := 0; i < mset.Len(); i++ {
		if mset.At(i).Obj().Exported() {
			sig := mset.At(i).Type().(*types.Signature)
			r.addRuntimeType(sig.Params(), true)  // skip the Tuple itself
			r.addRuntimeType(sig.Results(), true) // skip the Tuple itself
		}
	}

	switch t := T.(type) {
	case *types.Basic:
		// nop

	case *types.Interface:
		// nop---handled by recursion over method set.

	case *types.Pointer:
		r.addRuntimeType(t.Elem(), false)

	case *types.Slice:
		r.addRuntimeType(t.Elem(), false)

	case *types.Chan:
		r.addRuntimeType(t.Elem(), false)

	case *types.Map:
		r.addRuntimeType(t.Key(), false)
		r.addRuntimeType(t.Elem(), false)

	case *types.Signature:
		if t.Recv() != nil {
			panic(fmt.Sprintf("Signature %s has Recv %s", t, t.Recv()))
		}
		r.addRuntimeType(t.Params(), true)  // skip the Tuple itself
		r.addRuntimeType(t.Results(), true) // skip the Tuple itself

	case *types.Named:
		// A pointer-to-named type can be derived from a named
		// type via reflection.  It may have methods too.
		r.addRuntimeType(types.NewPointer(T), false)

		// Consider 'type T struct{S}' where S has methods.
		// Reflection provides no way to get from T to struct{S},
		// only to S, so the method set of struct{S} is unwanted,
		// so set 'skip' flag during recursion.
		r.addRuntimeType(t.Underlying(), true)

	case *types.Array:
		r.addRuntimeType(t.Elem(), false)

	case *types.Struct:
		for i, n := 0, t.NumFields(); i < n; i++ {
			r.addRuntimeType(t.Field(i).Type(), false)
		}

	case *types.Tuple:
		for i, n := 0, t.Len(); i < n; i++ {
			r.addRuntimeType(t.At(i).Type(), false)
		}

	default:
		panic(T)
	}
}
