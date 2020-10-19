package collate

type generalZhCICollator struct {
}

// Collator interface, no implements now.
func (g generalZhCICollator) Compare(a, b string) int {
	panic("implement me")
}

// Collator interface, no implements now.
func (g generalZhCICollator) Key(str string) []byte {
	panic("implement me")
}

// Collator interface, no implements now.
func (g generalZhCICollator) Pattern() WildcardPattern {
	panic("implement me")
}
