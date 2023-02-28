package example

import (
	"github.com/pingcap/tidb/distribute_framework/dispatcher"
	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/distribute_framework/scheduler"
)

// simpleNumberGTaskMeta is a simple implementation of GlobalTaskMeta.
type simpleNumberGTaskMeta struct {
}

func (g *simpleNumberGTaskMeta) Serialize() []byte {
	return []byte{}
}

func (g *simpleNumberGTaskMeta) GetType() proto.TaskType {
	return proto.TaskTypeNumber
}

func (g *simpleNumberGTaskMeta) GetConcurrency() uint64 {
	return 4
}

type NumberExampleHandle struct {
}

func (n NumberExampleHandle) Progress(d *dispatcher.Dispatcher, gTask *proto.Task) (subTasks []*proto.Subtask, err error) {
	switch gTask.State {
	case proto.TaskStatePending:
		gTask.State = proto.TaskStateNumberExampleStep1
		subTasks = append(subTasks, &proto.Subtask{})
	case proto.TaskStateNumberExampleStep1:
		gTask.State = proto.TaskStateNumberExampleStep2
		subTasks = append(subTasks, &proto.Subtask{})
	case proto.TaskStateNumberExampleStep2:
		gTask.State = proto.TaskStateSucceed
	}
	return subTasks, nil
}

func (n NumberExampleHandle) HandleError(d *dispatcher.Dispatcher, gTask *proto.Task, receive string) error {
	// Don't handle not.
	return nil
}

func init() {
	scheduler.RegisterSubtaskExectorConstructor(
		proto.TaskTypeExample,
		func(subtask *proto.Subtask) scheduler.SubtaskExecutor {
			return &ExampleSubtaskExecutor{subtask: subtask}
		},
	)
	dispatcher.RegisterTaskDispatcherHandle(proto.TaskTypeExample, NumberExampleHandle{})
}
