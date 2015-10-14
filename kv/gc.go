package kv

import "time"

type GCPolicy struct {
	MaxRetainVersions int
	TriggerInterval   time.Duration
}

type GC interface {
	OnGet(k Key)
	OnSet(k Key)
	OnDelete(k Key)
	Compact(ctx interface{}, k Key) error
}
