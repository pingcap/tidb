// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package source

import (
	"go/types"
	"strings"
	"time"
)

// Limit deep completion results because in most cases there are too many
// to be useful.
const MaxDeepCompletions = 3

// deepCompletionState stores our state as we search for deep completions.
// "deep completion" refers to searching into objects' fields and methods to
// find more completion candidates.
type deepCompletionState struct {
	// maxDepth limits the deep completion search depth. 0 means
	// disabled and -1 means unlimited.
	maxDepth int

	// chain holds the traversal path as we do a depth-first search through
	// objects' members looking for exact type matches.
	chain []types.Object

	// chainNames holds the names of the chain objects. This allows us to
	// save allocations as we build many deep completion items.
	chainNames []string

	// highScores tracks the highest deep candidate scores we have found
	// so far. This is used to avoid work for low scoring deep candidates.
	highScores [MaxDeepCompletions]float64

	// candidateCount is the count of unique deep candidates encountered
	// so far.
	candidateCount int
}

// push pushes obj onto our search stack.
func (s *deepCompletionState) push(obj types.Object) {
	s.chain = append(s.chain, obj)
	s.chainNames = append(s.chainNames, obj.Name())
}

// pop pops the last object off our search stack.
func (s *deepCompletionState) pop() {
	s.chain = s.chain[:len(s.chain)-1]
	s.chainNames = s.chainNames[:len(s.chainNames)-1]
}

// chainString joins the chain of objects' names together on ".".
func (s *deepCompletionState) chainString(finalName string) string {
	s.chainNames = append(s.chainNames, finalName)
	chainStr := strings.Join(s.chainNames, ".")
	s.chainNames = s.chainNames[:len(s.chainNames)-1]
	return chainStr
}

// isHighScore returns whether score is among the top MaxDeepCompletions
// deep candidate scores encountered so far. If so, it adds score to
// highScores, possibly displacing an existing high score.
func (s *deepCompletionState) isHighScore(score float64) bool {
	// Invariant: s.highScores is sorted with highest score first. Unclaimed
	// positions are trailing zeros.

	// First check for an unclaimed spot and claim if available.
	for i, deepScore := range s.highScores {
		if deepScore == 0 {
			s.highScores[i] = score
			return true
		}
	}

	// Otherwise, if we beat an existing score then take its spot and scoot
	// all lower scores down one position.
	for i, deepScore := range s.highScores {
		if score > deepScore {
			copy(s.highScores[i+1:], s.highScores[i:])
			s.highScores[i] = score
			return true
		}
	}

	return false
}

func (c *completer) inDeepCompletion() bool {
	return len(c.deepState.chain) > 0
}

// shouldPrune returns whether we should prune the current deep
// candidate search to reduce the overall search scope. The
// maximum search depth is reduced gradually as we use up our
// completionBudget.
func (c *completer) shouldPrune() bool {
	if !c.inDeepCompletion() {
		return false
	}

	c.deepState.candidateCount++

	// Check our remaining budget every 1000 candidates.
	if c.deepState.candidateCount%1000 == 0 {
		spent := float64(time.Since(c.startTime)) / float64(completionBudget)

		switch {
		case spent >= 0.90:
			// We are close to exhausting our budget. Disable deep completions.
			c.deepState.maxDepth = 0
		case spent >= 0.75:
			// We are running out of budget, reduce max depth again.
			c.deepState.maxDepth = 2
		case spent >= 0.5:
			// We have used half our budget, reduce max depth again.
			c.deepState.maxDepth = 3
		case spent >= 0.25:
			// We have used a good chunk of our budget, so start limiting our search.
			// By default the search depth is unlimited, so this limit, while still
			// generous, is normally a huge reduction in search scope that will result
			// in our search completing very soon.
			c.deepState.maxDepth = 4
		}
	}

	if c.deepState.maxDepth >= 0 {
		return len(c.deepState.chain) >= c.deepState.maxDepth
	}

	return false
}

// deepSearch searches through obj's subordinate objects for more
// completion items.
func (c *completer) deepSearch(obj types.Object) {
	if c.deepState.maxDepth == 0 {
		return
	}

	// If we are definitely completing a struct field name, deep completions
	// don't make sense.
	if c.wantStructFieldCompletions() && c.enclosingCompositeLiteral.inKey {
		return
	}

	// Don't search into type names.
	if isTypeName(obj) {
		return
	}

	// Don't search embedded fields because they were already included in their
	// parent's fields.
	if v, ok := obj.(*types.Var); ok && v.Embedded() {
		return
	}

	// Push this object onto our search stack.
	c.deepState.push(obj)

	switch obj := obj.(type) {
	case *types.PkgName:
		c.packageMembers(obj)
	default:
		// For now it is okay to assume obj is addressable since we don't search beyond
		// function calls.
		c.methodsAndFields(obj.Type(), true)
	}

	// Pop the object off our search stack.
	c.deepState.pop()
}
