package storeutil

import (
	"go.uber.org/atomic"
)

// StoreLimit will update from config reload and global variable set.
var StoreLimit atomic.Uint64
