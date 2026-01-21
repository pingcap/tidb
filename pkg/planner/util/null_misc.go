// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// SQL three-valued logic (3VL) quick reference
// --------------------------------------------
// 3VL has three truth values: TRUE, FALSE, UNKNOWN (result from any NULL participation).
// Basic rules:
//   - Comparisons/pattern/regex with a NULL operand → UNKNOWN (i.e., result is NULL), except NULL-safe <=>.
//   - AND: FALSE dominates; TRUE ∧ UNKNOWN = UNKNOWN; UNKNOWN ∧ UNKNOWN = UNKNOWN.
//   - OR:  TRUE dominates; FALSE ∨ UNKNOWN = UNKNOWN; UNKNOWN ∨ UNKNOWN = UNKNOWN.
//   - NOT: NOT UNKNOWN = UNKNOWN.
//
// In this file, "NULL propagation" means: if any relevant argument is NULL, the function yields NULL (UNKNOWN).
// We call such functions NullPreserving.

// NullBehavior describes how a function interacts with SQL 3VL and NULL propagation.
// Function null-behavior traits (centralized, easy to extend):
//   - NullPreserving:   any NULL input (relevant arg) yields NULL output (e.g., +, -, *, comparisons, LIKE, REGEXP)
//   - NullHiding:       may turn NULL into non-NULL or short-circuit to a non-NULL truth value
//     (IF/CASE/COALESCE/IS .../AND/OR)
//   - NullSafeEq:       NULL-safe equality (<=>)
//   - NullTransparentWrapper: unary wrapper that is NULL-transparent and "structurally" transparent for our checks
//     (e.g., CAST/CONVERT/WEIGHT_STRING、UNARY + / UNARY -). We peel these before reasoning.
type NullBehavior uint8

// NullBehavior constants define how functions interact with NULL values
const (
	// NullPreserving indicates any NULL input yields NULL output
	NullPreserving NullBehavior = 1 << iota
	// NullHiding indicates may turn NULL into non-NULL
	NullHiding
	// NullSafeEq indicates NULL-safe equality
	NullSafeEq
	// NullTransparentWrapper indicates unary wrapper that is NULL-transparent
	NullTransparentWrapper
)

// fnTraits maps ast func name → trait flags.
// NOTE: keep names in lower-case (ast constants already are).
var fnTraits = map[string]NullBehavior{
	// Comparisons / pattern / regexp: 3VL → NULL when any side is NULL
	ast.EQ:         NullPreserving,
	ast.NE:         NullPreserving,
	ast.GT:         NullPreserving,
	ast.GE:         NullPreserving,
	ast.LT:         NullPreserving,
	ast.LE:         NullPreserving,
	ast.Like:       NullPreserving,
	ast.Ilike:      NullPreserving,
	ast.RegexpLike: NullPreserving,
	ast.Regexp:     NullPreserving,
	ast.NullEQ:     NullSafeEq,

	// Boolean connectives (may hide NULL via short-circuit)
	ast.LogicAnd: NullHiding,
	ast.LogicOr:  NullHiding,
	ast.LogicXor: NullHiding,

	// Truth predicates & NULL tests – they *handle* NULL explicitly
	ast.IsFalsity:          NullHiding,
	ast.IsTruthWithNull:    NullHiding,
	ast.IsTruthWithoutNull: NullHiding,
	ast.IsNull:             NullHiding,

	// Control flow / NULL-bridging
	ast.If:       NullHiding,
	ast.Case:     NullHiding,
	ast.Coalesce: NullHiding,
	ast.Ifnull:   NullHiding,
	ast.Nullif:   NullHiding,

	// Unary wrappers we want to peel structurally
	ast.Cast:         NullTransparentWrapper,
	ast.Convert:      NullTransparentWrapper,
	ast.WeightString: NullTransparentWrapper,
	ast.UnaryPlus:    NullTransparentWrapper,
	ast.UnaryMinus:   NullTransparentWrapper,

	// Arithmetic / bit ops (all NULL-preserving)
	ast.Plus:       NullPreserving,
	ast.Minus:      NullPreserving,
	ast.Mul:        NullPreserving,
	ast.Div:        NullPreserving,
	ast.IntDiv:     NullPreserving,
	ast.Mod:        NullPreserving,
	ast.And:        NullPreserving, // bitand
	ast.Or:         NullPreserving, // bitor
	ast.Xor:        NullPreserving, // bitxor
	ast.LeftShift:  NullPreserving,
	ast.RightShift: NullPreserving,
	ast.BitNeg:     NullPreserving,

	// N-ary math helpers that propagate NULL if any arg is NULL
	ast.Greatest: NullPreserving,
	ast.Least:    NullPreserving,
}

func traitOf(name string) NullBehavior           { return fnTraits[name] }
func has(t NullBehavior, flag NullBehavior) bool { return t&flag != 0 }

// allConstants returns true if `expr` is composed only of constants (and
// scalar functions whose arguments are all constants). It also guards against
// expressions that are unsafe to treat as constants for plan cache.
func allConstants(ctx expression.BuildContext, expr expression.Expression) bool {
	if expression.MaybeOverOptimized4PlanCache(ctx, expr) {
		return false // expression contains non-deterministic parameter
	}
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !allConstants(ctx, arg) {
				return false
			}
		}
		return true
	case *expression.Constant:
		return true
	}
	return false
}

// isNullRejectedInList checks null filter for IN list using OR logic.
// Reason is that null filtering through evaluation by isNullRejectedExpr
// has problems with IN list. For example, constant in (outer-table.col1, inner-table.col2)
// is not null rejecting since constant in (outer-table.col1, NULL) is not false/unknown.
func isNullRejectedInList(ctx base.PlanContext, expr *expression.ScalarFunction,
	innerSchema *expression.Schema, skipPlanCacheCheck bool,
) bool {
	for i, arg := range expr.GetArgs() {
		if i > 0 {
			newArgs := make([]expression.Expression, 0, 2)
			newArgs = append(newArgs, expr.GetArgs()[0])
			newArgs = append(newArgs, arg)
			eQCondition, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ,
				expr.GetType(ctx.GetExprCtx().GetEvalCtx()), newArgs...)
			if err != nil {
				return false
			}
			if !(isNullRejectedExpr(ctx, innerSchema, eQCondition, skipPlanCacheCheck)) {
				return false
			}
		}
	}
	return true
}

// IsNullRejected takes care of complex predicates like this:
// IsNullRejected(A OR B) = IsNullRejected(A) AND IsNullRejected(B)
// IsNullRejected(A AND B) = IsNullRejected(A) OR IsNullRejected(B)
func IsNullRejected(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	predicate expression.Expression,
	skipPlanCacheCheck bool,
) bool {
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	if expression.ContainOuterNot(predicate) {
		return false
	}

	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		switch expr.FuncName.L {
		case ast.LogicAnd:
			if IsNullRejected(ctx, innerSchema, expr.GetArgs()[0], skipPlanCacheCheck) {
				return true
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1], skipPlanCacheCheck)
		case ast.LogicOr:
			if !IsNullRejected(ctx, innerSchema, expr.GetArgs()[0], skipPlanCacheCheck) {
				return false
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1], skipPlanCacheCheck)
		case ast.In:
			return isNullRejectedInList(ctx, expr, innerSchema, skipPlanCacheCheck)
		default:
			return isNullRejectedExpr(ctx, innerSchema, expr, skipPlanCacheCheck)
		}
	default:
		return isNullRejectedExpr(ctx, innerSchema, predicate, skipPlanCacheCheck)
	}
}

// New: whether the predicate references at least one inner-side column.
func referencesAnyInner(e expression.Expression, inner *expression.Schema) bool {
	for _, c := range expression.ExtractColumns(e) {
		if inner.Contains(c) {
			return true
		}
	}
	return false
}

// judgeFoldedConstant examines an already-folded expression `e`.
// Decision rule (SQL 3-valued logic):
//   - NULL (UNKNOWN)  → null-rejecting (return true, true)
//   - FALSE           → null-rejecting (return true, true)
//   - TRUE            → NOT null-rejecting (return true, false)
//   - non-constant    → undecided (return false, false)
func judgeFoldedConstant(ctx base.PlanContext, e expression.Expression) (decided bool, reject bool) {
	sc := ctx.GetSessionVars().StmtCtx
	if c, ok := e.(*expression.Constant); ok {
		if c.Value.IsNull() { // NULL / UNKNOWN
			return true, true
		}
		if b, err := c.Value.ToBool(sc.TypeCtxOrDefault()); err == nil {
			return true, b == 0 // FALSE ⇒ reject, TRUE ⇒ not reject
		}
	}
	return false, false
}

// tryConstTree handles the case where the whole predicate is a constant expression tree
// (safe for plan cache). We just FoldConstant and apply 3VL on the folded result.
// No "inner-columns-as-NULL" substitution is needed because there are no columns.
func tryConstTree(ctx base.PlanContext, pred expression.Expression) (decided bool, reject bool) {
	folded := expression.FoldConstant(ctx.GetExprCtx(), pred)
	return judgeFoldedConstant(ctx, folded)
}

// -------- Opaque info/metadata functions ------------------------------------
// These functions (like collation/charset/coercibility) should not be used as "NULL propagation" evidence
// to drive outer join elimination, otherwise expressions like `where collation(t2.c) = 'utf8mb4_bin'`
// would be incorrectly judged as allowing LEFT JOIN to be converted to INNER JOIN.
// Approach: The structural judgment phase relies on isNullPropagatingWRTInner
// (treating them as "potentially masking NULL"),
// while in the fallback (inner-only case) we do another contains check as a safety net.
var opaqueInfoFuncSet = map[string]struct{}{
	ast.Collation:    {},
	ast.Charset:      {},
	ast.Coercibility: {},
}

func isOpaqueInfoFuncName(name string) bool {
	_, ok := opaqueInfoFuncSet[name]
	return ok
}

// containsOpaqueInfoFunc: whether "info functions" appear in the expression tree
func containsOpaqueInfoFunc(e expression.Expression) bool {
	switch x := e.(type) {
	case *expression.ScalarFunction:
		if isOpaqueInfoFuncName(x.FuncName.L) {
			return true
		}
		for _, a := range x.GetArgs() {
			if containsOpaqueInfoFunc(a) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// +------------------------+
// | Constant?             |--Yes--> NULL/FALSE? → true
// +------------------------+                     TRUE → false
//            |
//            No
//            v
// +------------------------+
// | All-constant tree?     |--Yes--> Fold → NULL/FALSE? → true
// +------------------------+                     TRUE → false
//            |
//            No
//            v
// +------------------------+
// | Ref inner columns?     |--No--> false
// +------------------------+
//            |
//            Yes
//            v
// +------------------------+
// | Structural decision?   |--Yes--> return decision
// +------------------------+
//            |
//            No
//            v
// +------------------------+
// | Replace inner→NULL &   |
// | constant fold          |--Fold→ NULL/FALSE? → true
// +------------------------+         else → false

// isNullRejectedExpr decides whether `predicate` is null-rejecting w.r.t. the `innerSchema`.
// Strategy (fast → slow):
//  1. Literal constant fast-path: WHERE NULL / 0 / 1.
//  2. Whole constant tree: FoldConstant, then judge under 3VL (TRUE/FALSE/UNKNOWN).
//     (Must run before the inner-column check; constant predicates don't depend on columns.)
//  3. Quick bailout: if the predicate does NOT reference any inner column → false.
//  4. Structural decision: syntactic rules (comparisons/LIKE/REGEXP/<=>/AND/OR/NOT ...).
//  5. Fallback: replace inner columns with NULL, fold, then apply 3VL
//     (NULL/FALSE ⇒ reject; TRUE ⇒ not reject).
func isNullRejectedExpr(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	predicate expression.Expression,
	skipPlanCacheCheck bool,
) bool {
	sc := ctx.GetSessionVars().StmtCtx

	// (1) Literal constant fast-path
	if c, ok := predicate.(*expression.Constant); ok {
		if c.Value.IsNull() { // WHERE NULL
			return true
		}
		if b, err := c.Value.ToBool(sc.TypeCtxOrDefault()); err == nil {
			return b == 0 // WHERE 0 ⇒ reject, WHERE 1 ⇒ not reject
		}
		return false
	}

	// (2) Whole constant expression tree (no columns at all).
	//     Handle upfront to avoid misclassifying expressions like (1=0).
	if allConstants(ctx.GetExprCtx(), predicate) {
		if decided, reject := tryConstTree(ctx, predicate); decided {
			return reject
		}
		return false
	}

	// (3) Quick bailout: no inner columns ⇒ not null-rejecting WRT inner
	if !referencesAnyInner(predicate, innerSchema) {
		return false
	}

	// (4) Structural (syntactic) decision; if deterministic, return immediately
	switch result := isNullRejectingByStructure(predicate, innerSchema); result {
	case NullRejecting:
		return true
	case NotNullRejecting:
		return false
	case Undecided:
		// continue to fallback
	}

	return false // conservative default
}

// ResetNotNullFlag resets the not-null flag of [start, end) columns in the schema.
func ResetNotNullFlag(schema *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.DelFlag(mysql.NotNullFlag)
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}

// -----------------------------------------
// Transparent wrapper peeling (trait-driven)
// -----------------------------------------
// peelTransparentWrappers removes unary shells that are both NULL-transparent and
// structurally transparent for our purposes (e.g., CAST/CONVERT/WEIGHT_STRING).
// For CAST/CONVERT that carry more than one arg, we still peel the first argument.
func peelTransparentWrappers(e expression.Expression) expression.Expression {
	for {
		sf, ok := e.(*expression.ScalarFunction)
		if !ok {
			return e
		}
		if !has(traitOf(sf.FuncName.L), NullTransparentWrapper) {
			return e
		}
		args := sf.GetArgs()
		if len(args) == 0 {
			return e
		}
		// Peel the primary payload (arg0). This is safe because the wrapper is declared transparent.
		e = args[0]
	}
}

// anyColumnFromSchema after peeling wrappers.
func anyColumnFromSchema(e expression.Expression, sch *expression.Schema) bool {
	e = peelTransparentWrappers(e)
	for _, c := range expression.ExtractColumns(e) {
		if sch.Contains(c) {
			return true
		}
	}
	return false
}

// isNullPropagatingWRTInner reports whether expression `e` has at least one
// evaluation path where a NULL coming from the `inner` schema *must* propagate
// to the result of `e` (i.e., no guard like IF/IFNULL/COALESCE catches it).
//
// WRT = "with respect to": here it means we only consider NULLs coming from
// columns in `inner` (not from outer columns or constants).
//
// Intuition:
//   - A plain inner column by itself propagates NULL.
//   - A constant never propagates NULL.
//   - For a scalar function, if the function is "NULL-hiding" (IF/IFNULL/COALESCE/CASE,
//     IS NULL, IS TRUE/FALSE variants), then a NULL may be masked ⇒ we conservatively
//     say it does NOT propagate. Otherwise, we say it propagates if ANY child
//     expression propagates.
//
// Examples:
//   - inner.a                 → true
//   - ifnull(inner.a, 0)      → false   (NULL is hidden)
//   - inner.a + 1             → true
//   - if(inner.a > 0, 1, 2)   → false   (NULL in test doesn't have to bubble up)
//   - outer.b + 1             → false   (no inner cols involved)
func isNullPropagatingWRTInner(e expression.Expression, inner *expression.Schema) bool {
	e = peelTransparentWrappers(e)
	switch x := e.(type) {
	case *expression.Column:
		// Only columns from the `inner` schema are considered.
		return inner.Contains(x)

	case *expression.Constant:
		// A pure constant does not propagate inner NULL.
		return false

	case *expression.ScalarFunction:
		// Trait-driven decisions:
		t := traitOf(x.FuncName.L)
		// Info/meta functions are *not* evidence structurally (stay conservative).
		if isOpaqueInfoFuncName(x.FuncName.L) {
			return false
		}
		// NullHiding / NullSafeEq can potentially "consume/handle" NULL:
		// - IF/CASE/COALESCE/IS.../AND/OR may mask NULL values
		// - `<=>` is NULL-safe (NULL <=> NULL returns TRUE), should not be treated as "always propagating NULL"
		if has(t, NullHiding) || has(t, NullSafeEq) {
			return false
		}
		for _, a := range x.GetArgs() {
			if isNullPropagatingWRTInner(a, inner) {
				return true
			}
		}
		return false

	default:
		// Unknown node kinds → assume not propagating to stay conservative.
		return false
	}
}

// NullRejectionResult represents the result of structural null-rejection analysis
type NullRejectionResult int

const (
	// Undecided means we cannot determine from structure alone, need fallback
	Undecided NullRejectionResult = iota
	// NotNullRejecting means the predicate definitely does not reject NULL values
	NotNullRejecting
	// NullRejecting means the predicate definitely rejects NULL values
	NullRejecting
)

// isNullRejectingByStructure tries to decide *syntactically* whether predicate `p`
// is NULL-rejecting w.r.t. `inner`. It returns:
//
//	NullRejecting    → definitely NULL-rejecting (no fallback needed)
//	NotNullRejecting → definitely NOT NULL-rejecting (no fallback needed)
//	Undecided        → cannot determine structurally (caller should apply fallback)
//
// The rules are deliberately conservative. If we cannot be *certain* from the
// structure, we return Undecided and let the fallback decide.
//
// Key families handled here:
//  1. Comparisons/LIKE/REGEXP    → NULL-rejecting if a side actually propagates inner NULL
//  2. Null-safe equal `<=>`      → NEVER NULL-rejecting
//  3. IN/NOT IN                  → handled via fallback (NOT IN is UnaryNot(In(...)))
//  4. Boolean composition        → AND/OR combine child decisions as in MySQL logic
//  5. NOT (x IS NULL)            → NULL-rejecting if x comes from inner
func isNullRejectingByStructure(p expression.Expression, inner *expression.Schema) NullRejectionResult {
	sf, ok := p.(*expression.ScalarFunction)
	if !ok {
		// Non-scalar (pure column/constant) rarely decides anything structurally.
		return Undecided
	}
	args := sf.GetArgs()
	t := traitOf(sf.FuncName.L)
	switch sf.FuncName.L {
	// ---- 1) Comparisons / LIKE / REGEXP ------------------------------------
	// They are NULL-rejecting only if at least one side *propagates* inner NULL.
	// Examples:
	//   inner.a   = outer.b      → yes (propagates through inner.a)
	//   ifnull(inner.a,0) = 1    → no  (inner NULL is hidden)
	//   outer.b   = 1            → no  (no inner cols)
	case ast.EQ, ast.NE, ast.GT, ast.GE, ast.LT, ast.LE, ast.Like, ast.Ilike, ast.RegexpLike, ast.Regexp:
		cols := expression.ExtractColumns(peelTransparentWrappers(p))
		hasInner := false
		for _, c := range cols {
			if inner.Contains(c) {
				hasInner = true
				break
			}
		}
		if !hasInner {
			// No inner column referenced → not NULL-rejecting, and decision is final.
			return NotNullRejecting
		}
		if len(args) == 2 &&
			(isNullPropagatingWRTInner(args[0], inner) || isNullPropagatingWRTInner(args[1], inner)) {
			return NullRejecting // structurally sufficient (info functions already excluded in isNullPropagatingWRTInner)
		}
		// We saw inner columns but couldn't prove propagation structurally
		// (e.g., both sides have potential NULL-hiding). Let fallback decide.
		return Undecided

	// ---- 2) Null-safe equal `<=>` -------------------------------------------
	// By definition it does not reject NULLs (NULL <=> NULL is TRUE).
	case ast.NullEQ:
		return NotNullRejecting

	// ---- 3) IN / NOT IN -----------------------------------------------------
	// Defer IN to higher-level isNullRejectedInList() in IsNullRejected.
	// Here we do not decide structurally.
	case ast.In:
		return Undecided

	// ---- 4) IS [NOT] TRUE/FALSE (3VL predicates) ----------------------------
	// For `E IS FALSE/TRUE`, we must NOT reuse the boolean-connective rules.
	// Example: (FALSE AND innerNullable) is FALSE, so `IS FALSE` is TRUE and the
	// row is KEPT — i.e. NOT NULL-rejecting. Treat these separately:
	case ast.IsFalsity, ast.IsTruthWithoutNull, ast.IsTruthWithNull:
		if anyColumnFromSchema(args[0], inner) {
			return Undecided
		}
		return NotNullRejecting

	// ---- 5) Boolean composition ---------------------------------------------
	case ast.LogicAnd:
		// AND is NULL-rejecting if EITHER side is NULL-rejecting.
		left := isNullRejectingByStructure(args[0], inner)
		right := isNullRejectingByStructure(args[1], inner)
		if left != Undecided && right != Undecided {
			if left == NullRejecting || right == NullRejecting {
				return NullRejecting
			}
			return NotNullRejecting
		}
		return Undecided

	case ast.LogicOr:
		// OR is NULL-rejecting only if BOTH sides are NULL-rejecting.
		left := isNullRejectingByStructure(args[0], inner)
		right := isNullRejectingByStructure(args[1], inner)
		if left != Undecided && right != Undecided {
			if left == NullRejecting && right == NullRejecting {
				return NullRejecting
			}
			return NotNullRejecting
		}
		return Undecided

	// ---- 6) NOT … -----------------------------------------------------------
	// We only address NOT (x IS NULL) ⇒ x IS NOT NULL, which *is* NULL-rejecting
	// if x comes from the inner side. For other NOT forms, defer to fallback.
	case ast.UnaryNot:
		if child, ok := peelTransparentWrappers(args[0]).(*expression.ScalarFunction); ok {
			switch child.FuncName.L {
			case ast.IsNull:
				// NOT (X IS NULL) ≡ X IS NOT NULL
				// Treat it as NULL-rejecting only if X *propagates* inner NULL.
				// e.g. X=inner.col            → propagates  → true
				//      X=NOT(ISNULL(inner.col))→ never NULL → false
				if isNullPropagatingWRTInner(child.GetArgs()[0], inner) {
					return NullRejecting
				}
				// Deterministically NOT null-rejecting if X doesn't propagate inner NULL.
				return NotNullRejecting
			case ast.In:
				// NOT (IN(...)) → let fallback handle exact 3VL
				return Undecided
			}
		}
		return Undecided
	}
	// ---------- default families ----------
	// Unknown / unhandled function families → undecided, let fallback run.
	// If this is a known NullPreserving comparator family, but we reached here via default,
	// we still honor the generic rule; otherwise, unknown/unhandled → undecided.
	if has(t, NullPreserving) {
		cols := expression.ExtractColumns(peelTransparentWrappers(p))
		hasInner := false
		for _, c := range cols {
			if inner.Contains(c) {
				hasInner = true
				break
			}
		}
		if !hasInner {
			return NotNullRejecting
		}
		a := sf.GetArgs()
		if len(a) == 2 && (isNullPropagatingWRTInner(a[0], inner) || isNullPropagatingWRTInner(a[1], inner)) {
			return NullRejecting
		}
		return Undecided
	}
	// Unknown / unhandled function families → undecided, let fallback run.
	return Undecided
}
