package stream

import (
	"bytes"
	"encoding/binary"
	"path"
	"strings"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/kv"
)

const (
	// The commmon prefix of keys involved by the stream backup.
	streamKeyPrefix    = "/tidb/br-stream"
	taskInfoPath       = "/info"
	taskCheckpointPath = "/checkpoint"
	taskRangesPath     = "/ranges"
	taskPausePath      = "/pause"
)

// TaskOf returns the path of tasks.
// Normally it would be <prefix>/info/<task-name(string)> -> <task(protobuf)>
func TaskOf(name string) string {
	return path.Join(streamKeyPrefix, taskInfoPath, name)
}

// RangesOf returns the path prefix for some task.
// Normally it would be <prefix>/ranges/<task-name(string)>
func RangesOf(name string) string {
	return path.Join(streamKeyPrefix, taskRangesPath, name)
}

// RangeKeyOf returns the path for ranges of some task.
// Normally it would be <prefix>/ranges/<task-name(string)>/<start-key(binary)> -> <end-key(binary)>
func RangeKeyOf(name string, startKey []byte) string {
	// We cannot use path.Join if the start key contains patterns like [0x2f, 0x2f](//).
	return strings.TrimSuffix(RangesOf(name), "/") + "/" + string(startKey)
}

func writeUint64(buf *bytes.Buffer, num uint64) {
	items := [8]byte{}
	binary.BigEndian.PutUint64(items[:], num)
	buf.Write(items[:])
}

func encodeUint64(num uint64) []byte {
	items := [8]byte{}
	binary.BigEndian.PutUint64(items[:], num)
	return items[:]
}

// CheckpointOf returns the checkpoint of some task, in some region of some store.
// All binary u64 are encoded as big endian.
// Normally it would be <prefix>/checkpoint/<store-id(binary-u64)>/<region-id(binary-u64)> -> <next-backup-ts(binary-u64)>
func CheckpointOf(task string, store uint64, region uint64) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.TrimSuffix(path.Join(streamKeyPrefix, task), "/"))
	buf.WriteRune('/')
	writeUint64(buf, store)
	buf.WriteRune('/')
	writeUint64(buf, region)
	return buf.String()
}

// CheckpointsOf returns the checkpoint prefix of some store.
// Normally it would be <prefix>/checkpoint/<store-id(binary-u64)>.
func CheckPointsOf(task string, store uint64) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.TrimSuffix(path.Join(streamKeyPrefix, task), "/"))
	buf.WriteRune('/')
	writeUint64(buf, store)
	buf.WriteRune('/')
	return buf.String()
}

// Pause returns the path for pausing the task.
func Pause(task string) string {
	return path.Join(streamKeyPrefix, task, taskPausePath)
}

// Ranges is a vector of [start_key, end_key) pairs.
type Ranges = []Range
type Range = kv.KeyRange

// TaskInfo is a task info with extra information.
// When returning by a query, the `Ranges` and `Pausing` field may be lazily fetched.
type TaskInfo struct {
	backuppb.StreamBackupTaskInfo
	Ranges  Ranges
	Pausing bool
}
