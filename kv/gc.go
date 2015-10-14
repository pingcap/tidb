package kv

type GC interface {
	OnGet(k Key)
	OnSet(k Key)
	Do(k Key)
}
