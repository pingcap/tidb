package collate

type zhPinyinTiDBASCS struct {
}

// Collator interface, no implements now.
func (g zhPinyinTiDBASCS) Compare(a, b string) int {
	panic("implement me")
}

// Collator interface, no implements now.
func (g zhPinyinTiDBASCS) Key(str string) []byte {
	panic("implement me")
}

// Collator interface, no implements now.
func (g zhPinyinTiDBASCS) Pattern() WildcardPattern {
	panic("implement me")
}
