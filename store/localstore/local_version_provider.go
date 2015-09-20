package localstore

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/kv"
)

// LocalVersioProvider uses local timestamp for version.
type LocalVersioProvider struct {
	mu              sync.Mutex
	lastTimeStampTs int64
	n               int64
}

// GetCurrentVer implements the VersionProvider's GetCurrentVer interface.
func (l *LocalVersioProvider) GetCurrentVer() (kv.Version, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := (time.Now().UnixNano() / int64(time.Millisecond)) << 18
	if l.lastTimeStampTs == ts {
		l.n++
		return kv.Version{uint64(ts + l.n)}, nil
	}
	l.lastTimeStampTs = ts
	l.n = 0
	return kv.Version{uint64(ts)}, nil
}
