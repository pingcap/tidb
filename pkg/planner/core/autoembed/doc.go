// Copyright 2026 PingCAP, Inc.
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

// Package autoembed proves whether a vector expression consumed by a
// VEC_EMBED_* function originates from compatible catalog-generated
// EMBED_TEXT columns. Resolution walks from the consumer's logical-plan column
// toward catalog roots and returns model and option metadata only when every
// possible source satisfies the package's closed-world propagation policy.
//
// A transparent alias preserves one exact value path. For example:
//
//	CREATE TABLE docs (
//	  text TEXT,
//	  vec VECTOR(3) GENERATED ALWAYS AS
//	    (EMBED_TEXT('mock/json', text, '{"plus":0.2}')) STORED
//	);
//	SELECT VEC_EMBED_L2_DISTANCE(v, 'hello')
//	FROM (SELECT vec AS v FROM docs) AS p;
//
// Resolution proves Projection(v <- docs.vec) followed by DataSource(docs.vec),
// so expression rewriting may use the catalog model and options for the query
// text. Casts, scalar functions, and unrecognized logical operators do not
// preserve this proof.
//
// Internally, every lookup has three states. Outside means the current plan
// does not own the column namespace. Unproven means the plan owns the column
// but cannot prove compatible auto-embedding provenance. Proven means both
// ownership and AutoEmbedInfo are established. Unproven deliberately claims
// the namespace so lookup cannot fall through to an unrelated source.
//
// A USING or NATURAL join illustrates why namespace claims and consensus are
// separate. In this query:
//
//	SELECT VEC_EMBED_L2_DISTANCE(vec, 'hello')
//	FROM docs JOIN plain_vectors USING (vec);
//
// the Join owns the coalesced vec output. Resolution succeeds only when both
// source columns prove equal auto-embedding metadata and compatible vector
// types. If plain_vectors.vec is ordinary or incompatible, the result is
// Unproven rather than Outside, and the rewrite is rejected.
//
// Before planning, the existing Preprocess traversal classifies whether the
// statement AST contains one of the four VEC_EMBED_* consumers. Only a
// complete traversal with no consumer is Absent; bypassed, incomplete, or
// dynamic ASTs are Unknown and preserve lineage conservatively. Runtime view
// and plan-digest ASTs can monotonically upgrade the current build state.
//
// A constant-false selection or zero-row limit can discard its logical input
// before an outer expression is rewritten. For Present and Unknown builds,
// PlanBuilder records the exact empty-Dual pointer and discarded input in a
// builder-local sidecar. Resolve first claims the Dual's public namespace and
// may then consult that source only for provenance. The sidecar is cleared
// when the outer Build returns and is never visible to optimization or
// execution. Correlated inputs remain in the visible plan tree because
// general correlated-column discovery cannot consult the sidecar.
//
// INSERT ... SELECT additionally uses a statement-local SourceSnapshot, but
// only when an ON DUPLICATE assignment contains a consumer (or cannot be
// classified reliably). SnapshotSource records source-output proofs before
// optimization. Resolve then applies source and target namespace precedence
// while preserving ambiguity and fail-closed behavior.
package autoembed
