package set

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type HashSet[T comparable] map[T]struct{}

func NewHashSet[T comparable]() HashSet[T] {
	return make(HashSet[T])
}

func (s *HashSet[T]) Add(v T) bool {
	prevLen := len(*s)
	(*s)[v] = struct{}{}
	return prevLen != len(*s)
}

// private version of Add which doesn't return a value
func (s *HashSet[T]) add(v T) {
	(*s)[v] = struct{}{}
}

func (s *HashSet[T]) Cardinality() int {
	return len(*s)
}

func (s *HashSet[T]) Clear() {
	*s = HashSet[T]()
}

func (s *HashSet[T]) Clone() HashSet[T] {
	clonedSet := make(HashSet[T], s.Cardinality())
	for elem := range *s {
		clonedSet.add(elem)
	}
	return &clonedSet
}

func (s *HashSet[T]) Contains(v ...T) bool {
	for _, val := range v {
		if _, ok := (*s)[val]; !ok {
			return false
		}
	}
	return true
}

// private version of Contains for a single element v
func (s *HashSet[T]) contains(v T) bool {
	_, ok := (*s)[v]
	return ok
}

func (s *HashSet[T]) Difference(other Set[T]) Set[T] {
	o := other.(*HashSet[T])

	diff := HashSet[T]()
	for elem := range *s {
		if !o.contains(elem) {
			diff.add(elem)
		}
	}
	return &diff
}

func (s *HashSet[T]) Each(cb func(T) bool) {
	for elem := range *s {
		if cb(elem) {
			break
		}
	}
}

func (s *HashSet[T]) Equal(other Set[T]) bool {
	o := other.(*HashSet[T])

	if s.Cardinality() != other.Cardinality() {
		return false
	}
	for elem := range *s {
		if !o.contains(elem) {
			return false
		}
	}
	return true
}

func (s *HashSet[T]) Intersect(other Set[T]) Set[T] {
	o := other.(*HashSet[T])

	intersection := HashSet[T]()
	// loop over smaller set
	if s.Cardinality() < other.Cardinality() {
		for elem := range *s {
			if o.contains(elem) {
				intersection.add(elem)
			}
		}
	} else {
		for elem := range *o {
			if s.contains(elem) {
				intersection.add(elem)
			}
		}
	}
	return &intersection
}

func (s *HashSet[T]) IsProperSubset(other Set[T]) bool {
	return s.Cardinality() < other.Cardinality() && s.IsSubset(other)
}

func (s *HashSet[T]) IsProperSuperset(other Set[T]) bool {
	return s.Cardinality() > other.Cardinality() && s.IsSuperset(other)
}

func (s *HashSet[T]) IsSubset(other Set[T]) bool {
	o := other.(*HashSet[T])
	if s.Cardinality() > other.Cardinality() {
		return false
	}
	for elem := range *s {
		if !o.contains(elem) {
			return false
		}
	}
	return true
}

func (s *HashSet[T]) IsSuperset(other Set[T]) bool {
	return other.IsSubset(s)
}

func (s *HashSet[T]) Iter() <-chan T {
	ch := make(chan T)
	go func() {
		for elem := range *s {
			ch <- elem
		}
		close(ch)
	}()

	return ch
}

func (s *HashSet[T]) Iterator() *Iterator[T] {
	iterator, ch, stopCh := newIterator[T]()

	go func() {
	L:
		for elem := range *s {
			select {
			case <-stopCh:
				break L
			case ch <- elem:
			}
		}
		close(ch)
	}()

	return iterator
}

// TODO: how can we make this properly , return T but can't return nil.
func (s *HashSet[T]) Pop() (v T, ok bool) {
	for item := range *s {
		delete(*s, item)
		return item, true
	}
	return
}

func (s *HashSet[T]) Remove(v T) {
	delete(*s, v)
}

func (s *HashSet[T]) String() string {
	items := make([]string, 0, len(*s))

	for elem := range *s {
		items = append(items, fmt.Sprintf("%v", elem))
	}
	return fmt.Sprintf("Set{%s}", strings.Join(items, ", "))
}

func (s *HashSet[T]) SymmetricDifference(other HashSet[T]) HashSet[T] {
	o := other.(*HashSet[T])

	sd := HashSet[T]()
	for elem := range *s {
		if !o.contains(elem) {
			sd.add(elem)
		}
	}
	for elem := range *o {
		if !s.contains(elem) {
			sd.add(elem)
		}
	}
	return &sd
}

func (s *HashSet[T]) ToSlice() []T {
	keys := make([]T, 0, s.Cardinality())
	for elem := range *s {
		keys = append(keys, elem)
	}

	return keys
}

func (s *HashSet[T]) Union(other Set[T]) Set[T] {
	o := other.(*HashSet[T])

	n := s.Cardinality()
	if o.Cardinality() > n {
		n = o.Cardinality()
	}
	unionedSet := make(HashSet[T], n)

	for elem := range *s {
		unionedSet.add(elem)
	}
	for elem := range *o {
		unionedSet.add(elem)
	}
	return &unionedSet
}

// MarshalJSON creates a JSON array from the set, it marshals all elements
func (s *HashSet[T]) MarshalJSON() ([]byte, error) {
	items := make([]string, 0, s.Cardinality())

	for elem := range *s {
		b, err := json.Marshal(elem)
		if err != nil {
			return nil, err
		}

		items = append(items, string(b))
	}

	return []byte(fmt.Sprintf("[%s]", strings.Join(items, ","))), nil
}

// UnmarshalJSON recreates a set from a JSON array, it only decodes
// primitive types. Numbers are decoded as json.Number.
func (s *HashSet[T]) UnmarshalJSON(b []byte) error {
	var i []any

	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	err := d.Decode(&i)
	if err != nil {
		return err
	}

	for _, v := range i {
		switch t := v.(type) {
		case T:
			s.add(t)
		default:
			// anything else must be skipped.
			continue
		}
	}

	return nil
}
