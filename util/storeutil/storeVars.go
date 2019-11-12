package storeutil

import (
	atomic2 "go.uber.org/atomic"
)

// StoreLimit will update from config reload and global variable set.
var StoreLimit atomic2.Uint64
