package example

import (
	"errors"
	"math/rand"

	"go.uber.org/zap"

	"github.com/pingcap/tidb/distribute_framework/dispatcher"
	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/util/logutil"
)

type NumberExampleHandle struct {
}

var mockTiDBIDList = []string{"id1", "id2", "id3"}

func assignRandomTiDB() string {
	return mockTiDBIDList[rand.Intn(3)]
}

func (n NumberExampleHandle) Progress(d dispatcher.Dispatch, gTask *proto.Task, fromPending bool) (finished bool, subTasks []*proto.Subtask, err error) {
	if fromPending {
		gTask.Step = proto.StepInit
	}
	switch gTask.Step {
	case proto.StepInit:
		gTask.Step = proto.StepOne
		gTask.Concurrency = 4
		for i := 0; i < 10; i++ {
			subTasksM := proto.SimpleNumberSTaskMeta{Numbers: make([]int, 0, 10)}
			for j := 0; j < 10; j++ {
				subTasksM.Numbers = append(subTasksM.Numbers, j)
			}
			subTasks = append(subTasks, &proto.Subtask{Meta: &subTasksM, SchedulerID: assignRandomTiDB()})
			logutil.BgLogger().Info("new sub task", zap.Any("sche id", subTasks[len(subTasks)-1].SchedulerID))
		}
		logutil.BgLogger().Info("progress step init")
	case proto.StepOne:
		gTask.Step = proto.StepTwo
		gTask.Concurrency = 6
		for i := 0; i < 10; i++ {
			subTasksM := proto.SimpleNumberSTaskMeta{Numbers: make([]int, 0, 10)}
			for j := 0; j < 10; j++ {
				subTasksM.Numbers = append(subTasksM.Numbers, j)
			}
			subTasks = append(subTasks, &proto.Subtask{Meta: &subTasksM, SchedulerID: assignRandomTiDB()})
		}
		logutil.BgLogger().Info("progress step one")
	case proto.StepTwo:
		logutil.BgLogger().Info("progress step two")
		return true, nil, nil
	default:
		return false, nil, errors.New("unknown step")
	}
	return false, subTasks, nil
}

func (n NumberExampleHandle) HandleError(d dispatcher.Dispatch, gTask *proto.Task, receive string) (finished bool, subtasks []*proto.Subtask, err error) {
	// Don't handle not.
	return true, nil, nil
}

func init() {
	dispatcher.RegisterGTaskFlowHandle(proto.TaskTypeExample, NumberExampleHandle{})
}
