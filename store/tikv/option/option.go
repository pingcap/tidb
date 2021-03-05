package option

import (
	"github.com/pingcap/tidb/kv"
)

type Setter interface {
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option. Only ReplicaRead is supported for snapshot
	SetOption(opt kv.Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt kv.Option)
}

type Getter interface {
	GetOption(opt kv.Option) interface{}
}
