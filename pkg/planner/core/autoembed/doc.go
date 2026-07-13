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
// INSERT ... SELECT additionally uses a statement-local SourceSnapshot.
// SnapshotSource records source-output proofs before optimization can replace
// an empty source with TableDual. Resolve then applies source and target
// namespace precedence while preserving ambiguity and fail-closed behavior.
package autoembed
