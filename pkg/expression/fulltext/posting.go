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

package fulltext

import (
	"slices"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
)

// This file evaluates a compiled boolean query against an inverted index
// instead of against one document at a time. The index is abstracted as a
// PostingSource that opens the posting list of a term: the rows containing the
// term, each with the term's positions in that row's document. The query tree
// is mirrored by a tree of streams: a term reads its posting list, a phrase
// intersects its terms' lists and aligns positions, a group intersects or
// unions its children and subtracts the prohibited ones. Every stream yields
// handles in ascending order without duplicates, which is what lets the
// intersections and unions run as merges instead of materialising sets.
//
// The result is exactly the set of rows the query matches, given an index
// built with the analyzer the query was compiled with, and a caller that
// keeps the original MATCH as a residual filter loses nothing by doing so.

// Posting is one entry of a posting list: a row containing the term, and the
// term's positions in the row's document.
type Posting struct {
	Handle    kv.Handle
	Positions []int
}

// PostingCursor reads a posting list.
type PostingCursor interface {
	// Next returns the next posting, or false when the list is exhausted.
	Next() (Posting, bool, error)
	Close() error
}

// PostingSource opens the posting lists of an inverted index.
type PostingSource interface {
	// Term opens the postings of one term, in ascending handle order.
	Term(term string) (PostingCursor, error)
	// Prefix opens the postings of every term that starts with prefix. They
	// come grouped by term, in ascending handle order within a term, and a
	// handle may appear once per term.
	Prefix(prefix string) (PostingCursor, error)
}

// PostingSeeker is implemented by a PostingCursor that can skip ahead in its
// posting list more cheaply than reading every posting in between. The
// intersections use it to let the rarest list drive: a list that only has to
// confirm a handle another list proposes jumps to it instead of reading the
// postings before it.
type PostingSeeker interface {
	// Seek returns the first posting whose handle is not below target,
	// consuming it and every posting before it, or false when the list has
	// none. A cursor on a handle at or past target returns its next posting.
	Seek(target kv.Handle) (Posting, bool, error)
}

// PostingMemoryConsumer is implemented by a PostingSource that is charged for
// the postings a search holds in memory: a prefix collects the rows of every
// term it covers before it yields the first, since they come grouped by term
// and not in handle order. The bytes are released, as a negative charge, when
// the stream holding them is closed.
type PostingMemoryConsumer interface {
	ConsumeMemory(bytes int64)
}

// PostingIterator yields the rows a query admits, in ascending handle order
// without duplicates.
type PostingIterator interface {
	Next() (kv.Handle, bool, error)
	Close() error
}

// OpenPostings evaluates the query against the index behind src and returns
// the rows it matches.
func (q *Query) OpenPostings(src PostingSource) (PostingIterator, error) {
	if q == nil || q.root == nil || q.matchesNothing {
		return emptyStream{}, nil
	}
	stream, err := openPostingStream(q.root, src)
	if err != nil {
		return nil, err
	}
	return handleIterator{stream: stream}, nil
}

// postingStream is a stream of postings in ascending handle order without
// duplicate handles. The positions carried by a posting are the ones the
// node contributes: a term's positions, or a phrase's start positions.
type postingStream interface {
	next() (Posting, bool, error)
	// seek is next for the first posting whose handle is not below target:
	// the postings before it are consumed without being returned.
	seek(target kv.Handle) (Posting, bool, error)
	close() error
}

type handleIterator struct {
	stream postingStream
}

func (it handleIterator) Next() (kv.Handle, bool, error) {
	posting, ok, err := it.stream.next()
	if err != nil || !ok {
		return nil, false, err
	}
	return posting.Handle, true, nil
}

func (it handleIterator) Close() error {
	return it.stream.close()
}

type emptyStream struct{}

func (emptyStream) next() (Posting, bool, error)          { return Posting{}, false, nil }
func (emptyStream) seek(kv.Handle) (Posting, bool, error) { return Posting{}, false, nil }
func (emptyStream) close() error                          { return nil }
func (emptyStream) Next() (kv.Handle, bool, error)        { return nil, false, nil }
func (emptyStream) Close() error                          { return nil }

func openPostingStream(node queryNode, src PostingSource) (postingStream, error) {
	switch n := node.(type) {
	case nil, neverNode:
		return emptyStream{}, nil
	case termNode:
		cursor, err := src.Term(n.token)
		if err != nil {
			return nil, err
		}
		return cursorStream{cursor: cursor}, nil
	case prefixNode:
		return openPrefixStream(n.prefix, src)
	case phraseNode:
		return openPhraseStream(n, src)
	case groupNode:
		return openGroupStream(n, src)
	default:
		return nil, errors.Errorf("unsupported fulltext query node %T", node)
	}
}

func openChildren(nodes []queryNode, src PostingSource) ([]postingStream, error) {
	streams := make([]postingStream, 0, len(nodes))
	for _, node := range nodes {
		stream, err := openPostingStream(node, src)
		if err != nil {
			_ = closeStreams(streams)
			return nil, err
		}
		streams = append(streams, stream)
	}
	return streams, nil
}

func closeStreams(streams []postingStream) error {
	var firstErr error
	for _, stream := range streams {
		if err := stream.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type cursorStream struct {
	cursor PostingCursor
}

func (s cursorStream) next() (Posting, bool, error) { return s.cursor.Next() }
func (s cursorStream) close() error                 { return s.cursor.Close() }

func (s cursorStream) seek(target kv.Handle) (Posting, bool, error) {
	if seeker, ok := s.cursor.(PostingSeeker); ok {
		return seeker.Seek(target)
	}
	for {
		posting, ok, err := s.cursor.Next()
		if err != nil || !ok || posting.Handle.Compare(target) >= 0 {
			return posting, ok, err
		}
	}
}

// prefixStream replays the rows of a prefix, which were collected in memory
// and sorted. Only the handles are kept, as int64s when the table has int
// handles: a prefix carries no positions, and a common prefix can cover most
// of the table.
type prefixStream struct {
	ints    []int64
	handles []kv.Handle
	i       int
	mem     PostingMemoryConsumer
	charged int64
}

// prefixHandleOverhead approximates what a common handle costs beyond its
// encoded bytes: the interface in the slice and the handle's own fields.
const prefixHandleOverhead = 64

func (s *prefixStream) add(handle kv.Handle) {
	if s.handles == nil && handle.IsInt() {
		oldCap := cap(s.ints)
		s.ints = append(s.ints, handle.IntValue())
		s.charge(int64(cap(s.ints)-oldCap) * 8)
		return
	}
	if s.handles == nil && len(s.ints) > 0 {
		// Every handle of a table is of one kind, so this does not happen;
		// keep the rows already collected should it ever.
		for _, v := range s.ints {
			s.handles = append(s.handles, kv.IntHandle(v))
		}
		s.charge(-int64(cap(s.ints)) * 8)
		s.ints = nil
	}
	s.handles = append(s.handles, handle)
	s.charge(prefixHandleOverhead + int64(len(handle.Encoded())))
}

func (s *prefixStream) charge(bytes int64) {
	if s.mem == nil || bytes == 0 {
		return
	}
	s.charged += bytes
	s.mem.ConsumeMemory(bytes)
}

func (s *prefixStream) sortAndCompact() {
	if s.handles == nil {
		slices.Sort(s.ints)
		s.ints = slices.Compact(s.ints)
		return
	}
	slices.SortFunc(s.handles, func(a, b kv.Handle) int { return a.Compare(b) })
	s.handles = slices.CompactFunc(s.handles, func(a, b kv.Handle) bool { return a.Equal(b) })
}

func (s *prefixStream) len() int {
	if s.handles == nil {
		return len(s.ints)
	}
	return len(s.handles)
}

func (s *prefixStream) at(i int) kv.Handle {
	if s.handles == nil {
		return kv.IntHandle(s.ints[i])
	}
	return s.handles[i]
}

func (s *prefixStream) next() (Posting, bool, error) {
	if s.i >= s.len() {
		return Posting{}, false, nil
	}
	handle := s.at(s.i)
	s.i++
	return Posting{Handle: handle}, true, nil
}

func (s *prefixStream) seek(target kv.Handle) (Posting, bool, error) {
	if s.handles == nil && target.IsInt() {
		s.i += sort.Search(len(s.ints)-s.i, func(j int) bool { return s.ints[s.i+j] >= target.IntValue() })
	} else {
		s.i += sort.Search(s.len()-s.i, func(j int) bool { return s.at(s.i+j).Compare(target) >= 0 })
	}
	return s.next()
}

func (s *prefixStream) close() error {
	s.charge(-s.charged)
	s.ints, s.handles = nil, nil
	return nil
}

// openPrefixStream reads every term with the prefix. The postings of
// different terms are not in one handle order, so they are collected and
// sorted, with a row that contains several matching terms kept once. The
// memory they take is charged to the source when it accounts for memory, so
// that the statement's memory quota can stop a prefix that covers too much.
func openPrefixStream(prefix string, src PostingSource) (postingStream, error) {
	cursor, err := src.Prefix(prefix)
	if err != nil {
		return nil, err
	}
	s := &prefixStream{}
	s.mem, _ = src.(PostingMemoryConsumer)
	for {
		posting, ok, err := cursor.Next()
		if err != nil {
			_ = cursor.Close()
			_ = s.close()
			return nil, err
		}
		if !ok {
			break
		}
		s.add(posting.Handle)
	}
	if err := cursor.Close(); err != nil {
		_ = s.close()
		return nil, err
	}
	s.sortAndCompact()
	return s, nil
}

// phraseStream intersects the posting lists of a phrase's terms and keeps the
// rows where the terms' positions line up with the phrase's offsets. The
// positions it yields are the phrase's start positions.
type phraseStream struct {
	tokens  []string
	offsets []int
	// One stream per distinct token; index maps each phrase position to it.
	streams []postingStream
	index   []int
	current []Posting
	valid   []bool
	started bool
}

func openPhraseStream(n phraseNode, src PostingSource) (postingStream, error) {
	if len(n.tokens) == 0 || len(n.tokens) != len(n.offsets) {
		return emptyStream{}, nil
	}
	s := &phraseStream{tokens: n.tokens, offsets: n.offsets, index: make([]int, len(n.tokens))}
	seen := make(map[string]int, len(n.tokens))
	for i, token := range n.tokens {
		if j, ok := seen[token]; ok {
			s.index[i] = j
			continue
		}
		cursor, err := src.Term(token)
		if err != nil {
			_ = s.close()
			return nil, err
		}
		seen[token] = len(s.streams)
		s.index[i] = len(s.streams)
		s.streams = append(s.streams, cursorStream{cursor: cursor})
	}
	s.current = make([]Posting, len(s.streams))
	s.valid = make([]bool, len(s.streams))
	return s, nil
}

func (s *phraseStream) advance(i int) error {
	posting, ok, err := s.streams[i].next()
	if err != nil {
		return err
	}
	s.current[i], s.valid[i] = posting, ok
	return nil
}

func (s *phraseStream) seekTo(i int, target kv.Handle) error {
	posting, ok, err := s.streams[i].seek(target)
	if err != nil {
		return err
	}
	s.current[i], s.valid[i] = posting, ok
	return nil
}

func (s *phraseStream) next() (Posting, bool, error) {
	return s.seek(nil)
}

// seek yields the first phrase match at or after target, or the next one
// when target is nil. The streams hold one posting of lookahead each.
func (s *phraseStream) seek(target kv.Handle) (Posting, bool, error) {
	if err := startOrSeek(&s.started, s.streams, s.current, s.valid, target, s.advance, s.seekTo); err != nil {
		return Posting{}, false, err
	}
	for {
		// Align every stream on the largest current handle.
		target, ok, err := alignStreams(s.streams, s.current, s.valid, s.seekTo)
		if err != nil || !ok {
			return Posting{}, false, err
		}
		starts := s.matchPositions()
		for i := range s.streams {
			if err := s.advance(i); err != nil {
				return Posting{}, false, err
			}
		}
		if len(starts) > 0 {
			return Posting{Handle: target, Positions: starts}, true, nil
		}
	}
}

// matchPositions returns the start positions at which every token sits at its
// offset, for the row all streams are aligned on.
func (s *phraseStream) matchPositions() []int {
	anchor := -1
	for i := range s.tokens {
		positions := s.current[s.index[i]].Positions
		if len(positions) == 0 {
			return nil
		}
		if anchor < 0 || len(positions) < len(s.current[s.index[anchor]].Positions) {
			anchor = i
		}
	}
	anchorPositions := s.current[s.index[anchor]].Positions
	candidates := make([]int, len(anchorPositions))
	for i, position := range anchorPositions {
		candidates[i] = position - s.offsets[anchor]
	}
	for i := range s.tokens {
		if i == anchor || len(candidates) == 0 {
			continue
		}
		candidates = intersectPhraseStarts(candidates, s.current[s.index[i]].Positions, s.offsets[i])
	}
	return candidates
}

func (s *phraseStream) close() error { return closeStreams(s.streams) }

// startOrSeek fills the lookahead of streams on the first call and, when
// target is not nil, moves every stream still below target up to it.
func startOrSeek(started *bool, streams []postingStream, current []Posting, valid []bool, target kv.Handle,
	advance func(int) error, seekTo func(int, kv.Handle) error) error {
	if !*started {
		*started = true
		for i := range streams {
			var err error
			if target == nil {
				err = advance(i)
			} else {
				err = seekTo(i, target)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	if target == nil {
		return nil
	}
	for i := range streams {
		if valid[i] && current[i].Handle.Compare(target) < 0 {
			if err := seekTo(i, target); err != nil {
				return err
			}
		}
	}
	return nil
}

// alignStreams moves streams until all of them sit on the same handle, and
// returns it. It reports false once any stream is exhausted, since no further
// handle can be common to all. A stream behind the largest current handle
// seeks to it rather than reading the postings in between, so the stream
// with the fewest postings sets the pace and a common term's list is read
// only around the rows a rarer one proposes.
func alignStreams(streams []postingStream, current []Posting, valid []bool, seekTo func(int, kv.Handle) error) (kv.Handle, bool, error) {
	for {
		var target kv.Handle
		for i := range streams {
			if !valid[i] {
				return nil, false, nil
			}
			if target == nil || current[i].Handle.Compare(target) > 0 {
				target = current[i].Handle
			}
		}
		aligned := true
		for i := range streams {
			if current[i].Handle.Compare(target) < 0 {
				if err := seekTo(i, target); err != nil {
					return nil, false, err
				}
			}
			if !valid[i] {
				return nil, false, nil
			}
			if current[i].Handle.Compare(target) != 0 {
				aligned = false
			}
		}
		if aligned {
			return target, true, nil
		}
	}
}

// andStream intersects its children by handle.
type andStream struct {
	streams []postingStream
	current []Posting
	valid   []bool
	started bool
}

func (s *andStream) advance(i int) error {
	posting, ok, err := s.streams[i].next()
	if err != nil {
		return err
	}
	s.current[i], s.valid[i] = posting, ok
	return nil
}

func (s *andStream) seekTo(i int, target kv.Handle) error {
	posting, ok, err := s.streams[i].seek(target)
	if err != nil {
		return err
	}
	s.current[i], s.valid[i] = posting, ok
	return nil
}

func (s *andStream) next() (Posting, bool, error) {
	return s.seek(nil)
}

func (s *andStream) seek(target kv.Handle) (Posting, bool, error) {
	if err := startOrSeek(&s.started, s.streams, s.current, s.valid, target, s.advance, s.seekTo); err != nil {
		return Posting{}, false, err
	}
	target, ok, err := alignStreams(s.streams, s.current, s.valid, s.seekTo)
	if err != nil || !ok {
		return Posting{}, false, err
	}
	for i := range s.streams {
		if err := s.advance(i); err != nil {
			return Posting{}, false, err
		}
	}
	return Posting{Handle: target}, true, nil
}

func (s *andStream) close() error { return closeStreams(s.streams) }

// orStream unions its children by handle.
type orStream struct {
	streams []postingStream
	current []Posting
	valid   []bool
	started bool
}

func (s *orStream) advance(i int) error {
	posting, ok, err := s.streams[i].next()
	if err != nil {
		return err
	}
	s.current[i], s.valid[i] = posting, ok
	return nil
}

func (s *orStream) seekTo(i int, target kv.Handle) error {
	posting, ok, err := s.streams[i].seek(target)
	if err != nil {
		return err
	}
	s.current[i], s.valid[i] = posting, ok
	return nil
}

func (s *orStream) next() (Posting, bool, error) {
	return s.seek(nil)
}

func (s *orStream) seek(target kv.Handle) (Posting, bool, error) {
	if err := startOrSeek(&s.started, s.streams, s.current, s.valid, target, s.advance, s.seekTo); err != nil {
		return Posting{}, false, err
	}
	var smallest kv.Handle
	for i := range s.streams {
		if s.valid[i] && (smallest == nil || s.current[i].Handle.Compare(smallest) < 0) {
			smallest = s.current[i].Handle
		}
	}
	if smallest == nil {
		return Posting{}, false, nil
	}
	for i := range s.streams {
		if s.valid[i] && s.current[i].Handle.Compare(smallest) == 0 {
			if err := s.advance(i); err != nil {
				return Posting{}, false, err
			}
		}
	}
	return Posting{Handle: smallest}, true, nil
}

func (s *orStream) close() error { return closeStreams(s.streams) }

// exceptStream yields the rows of positive that are not in negative. The
// negative stream only has to answer whether it holds each row positive
// proposes, so it seeks to that row.
type exceptStream struct {
	positive, negative postingStream
	negCurrent         Posting
	negValid, started  bool
}

func (s *exceptStream) next() (Posting, bool, error) {
	posting, ok, err := s.positive.next()
	if err != nil || !ok {
		return Posting{}, false, err
	}
	return s.filter(posting)
}

func (s *exceptStream) seek(target kv.Handle) (Posting, bool, error) {
	posting, ok, err := s.positive.seek(target)
	if err != nil || !ok {
		return Posting{}, false, err
	}
	return s.filter(posting)
}

// filter returns posting, or the first posting of positive after it, that
// negative does not hold.
func (s *exceptStream) filter(posting Posting) (Posting, bool, error) {
	for {
		if !s.started || (s.negValid && s.negCurrent.Handle.Compare(posting.Handle) < 0) {
			s.started = true
			neg, ok, err := s.negative.seek(posting.Handle)
			if err != nil {
				return Posting{}, false, err
			}
			s.negCurrent, s.negValid = neg, ok
		}
		if !s.negValid || s.negCurrent.Handle.Compare(posting.Handle) != 0 {
			return posting, true, nil
		}
		var ok bool
		var err error
		posting, ok, err = s.positive.next()
		if err != nil || !ok {
			return Posting{}, false, err
		}
	}
}

func (s *exceptStream) close() error {
	return closeStreams([]postingStream{s.positive, s.negative})
}

// openGroupStream mirrors groupNode.match: every required child must hold;
// with no required child, at least one optional child must; no prohibited
// child may.
func openGroupStream(n groupNode, src PostingSource) (postingStream, error) {
	var positive postingStream
	switch {
	case len(n.must) > 0:
		streams, err := openChildren(n.must, src)
		if err != nil {
			return nil, err
		}
		positive = newAndStream(streams)
	case len(n.should) > 0:
		streams, err := openChildren(n.should, src)
		if err != nil {
			return nil, err
		}
		positive = newOrStream(streams)
	default:
		return emptyStream{}, nil
	}
	if len(n.mustNot) == 0 {
		return positive, nil
	}
	streams, err := openChildren(n.mustNot, src)
	if err != nil {
		_ = positive.close()
		return nil, err
	}
	return &exceptStream{positive: positive, negative: newOrStream(streams)}, nil
}

func newAndStream(streams []postingStream) postingStream {
	if len(streams) == 1 {
		return streams[0]
	}
	return &andStream{streams: streams, current: make([]Posting, len(streams)), valid: make([]bool, len(streams))}
}

func newOrStream(streams []postingStream) postingStream {
	if len(streams) == 1 {
		return streams[0]
	}
	return &orStream{streams: streams, current: make([]Posting, len(streams)), valid: make([]bool, len(streams))}
}
