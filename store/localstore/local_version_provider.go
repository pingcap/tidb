package localstore

import (
	"errors"
	"sync"
	"time"

	"github.com/pingcap/tidb/kv"
)

// ErrOverflow is the error returned by CurrentVersion, it describes if
// there're too many versions allocations in a very short period of time, ID
// may conflict.
var ErrOverflow = errors.New("overflow when allocating new version")

// LocalVersionProvider uses local timestamp for version.
type LocalVersionProvider struct {
	mu              sync.Mutex
	lastTimeStampTS uint64
	n               uint64
}

const (
	timePrecisionOffset = 18
)

// CurrentVersion implements the VersionProvider's GetCurrentVer interface.
func (l *LocalVersionProvider) CurrentVersion() (kv.Version, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var ts uint64
	ts = uint64((time.Now().UnixNano() / int64(time.Millisecond)) << timePrecisionOffset)
	if l.lastTimeStampTS == uint64(ts) {
		l.n++
		if l.n >= 1<<timePrecisionOffset {
			return kv.Version{}, ErrOverflow
		}
		return kv.Version{Ver: ts + l.n}, nil
	}
	l.lastTimeStampTS = ts
	l.n = 0
	return kv.Version{Ver: ts}, nil
}

func localVersionToTimestamp(ver kv.Version) uint64 {
	return ver.Ver >> timePrecisionOffset
}
