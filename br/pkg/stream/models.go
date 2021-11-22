package stream

import (
	"path"
	"strconv"
)

const (
	// The commmon prefix of keys involved by the stream backup.
	streamKeyPrefix    = "/tidb/br-stream"
	taskInfoPath       = "/info"
	taskCheckpointPath = "/checkpoint"
	taskStatusPath     = "/status"
)

// TaskOf returns the path of tasks.
func TaskOf(name string) string {
	return path.Join(streamKeyPrefix, taskInfoPath, name)
}

// CheckpointOf returns the checkpoint of some task, in some region of some store.
func CheckpointOf(task string, store uint64, region uint64) string {
	return path.Join(streamKeyPrefix, task, strconv.FormatUint(region, 10), strconv.FormatUint(store, 10))
}

// Range presents a range for backup.
// NOTE: Maybe we can marshal it as `[start_key, end_key]`?
type Range struct {
	StartKey []byte `json:"start_key"`
	EndKey   []byte `json:"end_key"`
}

// TaskInfo presents a stream backup task.
type TaskInfo struct {
	// The backend URL, see the `storage` package for more details.
	StorageURL string `json:"storage_url"`
	// The last timestamp of the task has been updated.
	// This is a simple solution for infrequent config changing:
	// When we watched a config change(via polling or etcd watching),
	// We perform a incremental scan between [last_update_ts, now),
	// for filling the diff data during conf changing.
	// NOTE:
	// The current implementation scan [0, now) for every task ranges newly added,
	// So this field is reserved for future usage.
	LastUpdateTS string `json:"last_update_ts"`
	// The timestamp range for the backup task.
	StartTS uint64 `json:"start_ts"`
	EndTS   uint64 `json:"end_ts"`
	// The ranges of the task.
	TaskRanges []Range `json:"task_ranges"`
}
