// Copyright 2022 PingCAP, Inc.
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

package funcdep

// Theory to Practice
//
// For more rigorous examination of functional dependencies and their
// interaction with various SQL operators, see the following Master's Thesis:
//
//   Norman Paulley, Glenn. (2000).
//   Exploiting Functional Dependence in Query Optimization.
//   https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf

// TODO: Add the RFC design.

// NOTE 1.
// when handling Lax FD, we don't care the null value in the dependency, which means
// as long as null-attribute coverage of the determinant can make a Lax FD as strict one.

// The definition of "lax" used in the paper differs from the definition used by this
// library. For a lax dependency A~~>B, the paper allows this set of rows:
//
//	a  b
//	-------
//	1  1
//	1  NULL
//
//	This alternate definition is briefly covered in section 2.5.3.2 of the paper (see definition
//	2.19). The reason for this change is to allow a lax dependency to be upgraded to a strict
//	dependency more readily, needing only the determinant columns to be not-null rather than
//  both determinant and dependant columns.
//
// This is on the condition that, for definite values of determinant of a Lax FD, it won't
// have two same definite dependant value. That's true, because there is no way can derive
// to this kind of FD.
//
// Even in our implementation of outer join, the only way to produce duplicate definite
// determinant is the join predicate. But for now, we only maintain the equivalence and
// some strict FD of it.
//
//   t(a,b) left join t1(c,d,e) on t.a = t1.c and b=1
//  a  b  |  c     d     e
//  ------+----------------
//  1  1  |  1    NULL   1
//  1  2  | NULL  NULL  NULL
//  2  1  | NULL  NULL  NULL
//
// Actually it's possible, the lax FD {a} -> {c} can be derived but not that useful. we only
// maintain the {c} ~> {a} for existence after outer join. Besides, there two Cond-FD should
// be preserved waiting for be visible again once with the null-reject on the condition of
// null constraint columns. (see below)
//
// NOTE 2.
// When handle outer join, it won't produce lax FD with duplicate definite determinant values and
// different dependency values.
//
// In implementation，we come across some lax FD dependent on null-reject of some other cols. For
// example.
//   t(a,b) left join t1(c,d,e) on t.a = t1.c and b=1
//  a  b  |  c     d     e
//  ------+----------------
//  1  1  |  1    NULL   1
//  1  2  | NULL  NULL  NULL
//  2  1  | NULL  NULL  NULL
//
// here constant FD {} -> {b} won't be existed after the outer join is done. Notice null-constraint
// {c,d,e} -| {c,d,e}, this FD should be preserved and will be visible again when some null-reject
// predicate take effect on the null-constraint cols.
//
// It's same for strict equivalence {t.a} = {t1.c}. Notice there are no lax equivalence here, because
// left side couldn't be guaranteed to be definite or null. like a=2 here. Let's collect all of this
// on-condition FD down, correspondent with a null-constraints column set, name it as Cond-FD.
//
// lax equivalencies are theoretically possible, but it won't be constructed from an outer join unless
// t already has a constant FD in column `a` here before outer join take a run. So the lax equivalence
// has some pre-conditions as you see, and it couldn't cover the case shown above. Let us do it like a
// Cond-FD does.
//
// The FD constructed from the join predicate should be considered as Cond-FD. Here like equivalence of
// {a} == {c} and constant FD {b} = 1 (if the join condition is e=1, it's here too). We can say that for
// every matched row, this FDs is valid, while for the other rows, the inner side are supplied of null
// rows. So this FDs are stored as ncEdges with nc condition of all inner table cols.
//
// We introduced invisible FD with null-constraint column to solve the problem above named as Cond-FD.
// For multi embedded left join, we take the following case as an example.
//    a,b         c,d,e
// 	-----------+-----------
//   1    2    |    1  1  1
// 	 2    2    |
//  -----------+-----------
//
//  left join on (a=c) res:
//   a   b    c     e     e
//  -------------------------
//   1   2    1     1     1
//   2   2 +- null null  null -+
//         |                   |
//         +-------------------+
//                              \
//                               \
//  the Cond-FD are < a=c with {c,d,e} > the latter is as null constraint cols
//
//    e,f
//  -----------------------
//   1   2
//   2   2
//   3   3
//  -----------------------
//
//  left join on (e=a) res:
//   e   f    a     b      c    d    e
//  -----------------------------------
//   1   2    1     2      1    1    1
//   2   2    2     2  +- null null null --+---------------> Cond-FD are <a=c with {c,d,e}> still exists.
//   3   3 +-null null |  null null null   |---+
//         |           +-------------------+   |
//         +-----------------------------------+-----------> New Cond-FD are <e=a with {a,b,c,d,e}> occurs.
//
//
// the old Cond-FD with null constraint columns set {c,d,e} is preserved cause new appended cols are all null too.
// the new Cond-FD with null constraint columns set {a,b,c,d,e} are also meaningful, even if the null-reject column
// is one of {c,d,e} which may reduce one of the matched row out of the result, the equivalence {a}={e} still exist.
//
// Provide that the result of the first left join is like:
//  left join on (a=c) res:
//   a     b    c     e     e
//  ---------------------------
//   1     2    1     1     1
//   null  2  null  null  null
//
//  THEN: left join on (e=a) res:
//   e   f    a     b     c    d    e
//  ---------------------------------
//   1   2    1     2     1    1    1
//   2   2    null null null null null
//   3   3    null null null null null
//
//  Even like that, the case of old Cond-FD and new Cond-FD are existed too. Seems the null-constraint column set of
//  old Cond-FD {c,d,e} can be expanded as {a,b,c,d,e} visually, but we couldn't derive the inference of the join predicate
//  (e=a). The null-reject of column `a` couldn't bring the visibility to the old Cond-FD theoretically, it just happened
//  to refuse that row with a null value in column a.
//
// Think about adding one more row in first left join result.
//
//  left join on (a=c) res:
//   a     b    c     d     e
//  ---------------------------
//   1     2    1     1     1
//   null  2  null  null  null
//   3     3  null  null  null
//
//  THEN: left join on (e=a) res:
//   e   f    a     b     c    d    e
//  ---------------------------------
//   1   2    1     2     1    1    1
//   2   2    null null null null null
//   3   3    3     3   null null null
//
//  Conclusion:
//  As you see that's right we couldn't derive the inference of the join predicate (e=a) to expand old Cond-FD's nc
//  {c,d,e} as {a,b,c,d,e}. So the rule for Cond-FD is quite simple, just keep the old ncEdge from right, appending
//  the new ncEdges in current left join.
//
//  If the first left join result is in the outer side of the second left join, just keep the ncEdge from left as well,
//  appending the new ncEdges in current left join.
//
//  For a inner join, both side of the join result won't be appended with null-supplied rows, so we can simply collect
//  the ncEdges from both join side together.
//
// NOTE 3.
// Under left outer join Q = S * T on (P)
// We say predicate P is null-rejected on x when P(x) is TRUE. P(x) eval to false or unknown whenever x is Null or P(x) can
// guarantee x to be definite. Note: if x is a set, p(x) will evaluate to true if any x ∈ X cannot be Null. (one not null
// can bring p(x) to true)
//
// Strict functional dependencies:
// 1: Any strict functional dependency f: {x} -> {y} that held in S continues to hold in Q.
// 2: Any strict functional dependency f: {x} -> {y} than held in T may continue to hold in Q if:
//      the predicate P(x) is null-rejected (null-reject of determinant)
//      there exists a strict equivalence e: X == Y that held in T (determinant and dependant are both null)
// 3: If predicate P can produce f: {x} -> {y}, and xy ⊆ a(T) and P(x) is null-rejected, then f holds in Q.
// 4: If predicate P can produce f: {x} -> {y}, and x ⊆ a(P) ∩ S and y ⊆ T, and P(y) is null-rejected, then {a(P) ∩ S} -> {y} holds in Q. (P(y) null-rejected is not necessary).
// 5: The newly-constructed tuple identifier consist of both key from S and T, {l(s) ∪ l(T)} -> {l(Q)}.
//
// Lax functional dependencies:
// 1: Any lax functional dependency f: {x} ~> {y} that held in S continues to hold in Q.
// 2: Any lax functional dependency f: {x} ~> {y} that held in T continues to hold in Q.
// 3: Any strict functional dependency f: {x} -> {y} that held in T will hold as a lax functional dependency {x} ~> {y} in Q if P(x) is NOT null-rejected.
// 4: If predicate P would have produced either the functional dependency {x} -> {y} or {x} ~> {y} and {xy} ∩ a(T) is not empty, then {x} ~> {y} in Q.
//
// Strict Equivalence:
// 1: Any strict equivalence e: {x} == {y} that held in S continues to hold in Q.
// 2: Any strict equivalence e: {x} == {y} that held in T continues to hold in Q.
// 3: If there exists a lax equivalence {x} ~= {y} and xy ⊆ T, once P(x) and P(y) are null-rejected, then it can be strengthened and saved in Q.
// 4: If predicate P can produce e: {x} == {y} and xy ⊆ T, then it holds in Q.
//
// Constant FD:
// In our implementation, constant FD is a special kind of strict functional dependency.
// When discuss the inference rule of constant FD, we can utilize some rule from strict functional dependency above, adding some special consideration.
// 1: Any constant functional dependency f: {} -> {y} that held in S continues to hold in Q.
// 2: Any constant functional dependency f: {} -> {y} that held in T may be decomposed to multi strict FD first:
//       1: if y1 ⊆ y and y1 are null constant (come from y1 is null), then constant FD {} -> {y1} saved.
//       2: for other y, a cond-fd are preserved with null-constraint cols as T.
// 3: Any constant function dependency f: {} -> {y} that produced from predicate P:
//       1: for part of y ⊆ T, do it like what 2 says.
//       2: for part of y ⊆ S, do it like a cond-fd with null-constraint cols as T.
