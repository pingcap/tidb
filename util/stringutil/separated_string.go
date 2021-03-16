package stringutil

import (
	"fmt"
	"strings"
)

const (
	defaultScopeStackSize = 2
)

//SeparatedString helps build separated string
type SeparatedString struct {
	strings.Builder
	separators   []string
	initialized  []bool
	totalScope   uint8
	currentScope uint8
}

// StartScope enters an inner scope that use newSeparator as new separator
func (s *SeparatedString) StartScope(newSeparator string) *SeparatedString {
	s.initialized[s.currentScope] = true
	s.currentScope++
	if s.currentScope == s.totalScope {
		s.totalScope++
		s.separators = append(s.separators, newSeparator)
		s.initialized = append(s.initialized, false)
	}
	s.separators[s.currentScope] = newSeparator
	s.initialized[s.currentScope] = false
	return s
}

// EndScope quits an inner scope
func (s *SeparatedString) EndScope() *SeparatedString {
	if s.currentScope == 0 {
		return s
	}
	s.currentScope--
	return s
}

// StartNext writes separator if next one is not the first item
func (s *SeparatedString) StartNext() *SeparatedString {
	if !s.initialized[s.currentScope] {
		s.initialized[s.currentScope] = true
		return s
	}
	_, _ = s.WriteString(s.separators[s.currentScope])
	return s
}

// Append calls StartNext and writes substring
func (s *SeparatedString) Append(substring string) *SeparatedString {
	s.StartNext()
	_, _ = s.WriteString(substring)
	return s
}

// AppendF calls StartNext and write formatted string
func (s *SeparatedString) AppendF(format string, a ...interface{}) *SeparatedString {
	s.StartNext()
	_, _ = fmt.Fprintf(s, format, a...)
	return s
}

// NewSeparatedString create a new separated string with default scope stack size (2)
func NewSeparatedString(separator string) *SeparatedString {
	separators := make([]string, defaultScopeStackSize)
	separators[0] = separator
	return &SeparatedString{
		separators:  separators,
		initialized: make([]bool, defaultScopeStackSize),
		totalScope:  defaultScopeStackSize,
	}
}
