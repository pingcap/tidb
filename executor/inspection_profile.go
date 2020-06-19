package executor

import (
	"context"
	"fmt"
	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
	"time"
)

type metricNode struct {
	name          string
	children      []*metricNode
	value         int64
	unit int64
	childrenValue int64
}

type metricValueType int

const (
	metricValueSum      metricValueType = 1
	metricValueQuantile metricValueType = 2
)

type profileBuilder struct {
	sctx sessionctx.Context
	samples     []*profile.Sample
	locations   []*profile.Location
	functions   []*profile.Function
	queue       []*metricNode
	idMap       map[string]uint64
	idAllocator uint64
	valueTp     metricValueType
	quantile    float64
	start       time.Time
	end         time.Time
}

func (pb *profileBuilder) getMetricValue(n *metricNode) (int64, error) {
	var query string
	format := "2006-01-02 15:04:05"
	pb.start = pb.start.In(time.UTC)
	pb.end = pb.end.In(time.UTC)
	queryCondition := fmt.Sprintf("where time >= '%v' and time <= '%v'", pb.start.Format(format), pb.end.Format(format))
	switch pb.valueTp {
	case metricValueQuantile:
		query = fmt.Sprintf("select avg(value) from `metrics_schema`.`%v_duration` %v and quantile = %v",
			n.name, queryCondition, pb.quantile)
	case metricValueSum:
		fallthrough
	default:
		query = fmt.Sprintf("select sum(value) from `metrics_schema`.`%v_total_time` %v", n.name, queryCondition)
	}
	rows, _, err := pb.sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(context.Background(), query)
	//fmt.Printf("sql: %v  \n%v\n",query, err)
	if err != nil {
		return 0,err
	}
	if len(rows)==0 || rows[0].Len() != 1 {
		return 0,nil
	}

	//fmt.Printf("sql: %v  \n%v\n",query, rows[0].GetFloat64(0))
	if n.unit != 0 {
		return int64(rows[0].GetFloat64(0)*float64(n.unit)), nil
	}
	return int64(rows[0].GetFloat64(0)*float64(time.Second)), nil
}


func (pb *profileBuilder) getNodeID(n *metricNode) uint64 {
	if id, ok := pb.idMap[n.name]; ok {
		return id
	}
	id := pb.idAllocator
	pb.idAllocator++
	pb.idMap[n.name] = id
	return id
}

func (pb *profileBuilder) genLocation(n *metricNode) *profile.Location {
	id := pb.getNodeID(n)
	fn := &profile.Function{
		ID:   id,
		Name: n.name,
	}
	return &profile.Location{ID: id, Line: []profile.Line{{Function: fn}}}
}

func (pb *profileBuilder) genLocations(n *metricNode) []*profile.Location {
	loc := pb.genLocation(n)
	locations := []*profile.Location{loc}
	buf := []string{n.name}
	for i := len(pb.queue) - 1; i >= 0; i-- {
		locations = append(locations, pb.genLocation(pb.queue[i]))
		buf = append(buf,pb.queue[i].name)
	}
	fmt.Printf("loc: %v\n\n", buf)
	return locations
}

func (pb *profileBuilder) genTiDBQueryTree() *metricNode {
	tikvGRPC := &metricNode{
		name: "tikv_grpc_message",
	}
	tidbExecute := &metricNode{
		name: "tidb_execute",
		children: []*metricNode{
			{
				name: "pd_start_tso_wait",
			},
			{
				name: "tidb_kv_backoff",
			},
			{
				name: "tidb_kv_request",
				children: []*metricNode{
					tikvGRPC,
				},
			},
		},
	}
	queryTime := &metricNode{
		name: "tidb_query",
		children: []*metricNode{
			{
				name: "tidb_get_token",
				unit: int64(time.Second/10e5),
			},
			{
				name: "tidb_parse",
			},
			{
				name: "tidb_compile",
			},
			tidbExecute,
		},
	}

	return queryTime
}

func NewProfileBuilder(sctx sessionctx.Context) *profileBuilder {
	return &profileBuilder{
		sctx: sctx,
		idMap:       make(map[string]uint64),
		idAllocator: uint64(1),
		start: time.Now().Add(-time.Minute*10),
		end: time.Now(),
		valueTp: metricValueQuantile,
		quantile: 0.999,
	}
}

func (pb *profileBuilder) Collect() error {
	err :=pb.addMetricTree(pb.genTiDBQueryTree())
	if err != nil {
		return err
	}

	return nil
}

func (pb *profileBuilder) Build() *profile.Profile {
	return &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "cpu", Unit: "nanoseconds"},
		},
		TimeNanos:     int64(time.Now().Nanosecond()),
		DurationNanos: int64(time.Second * 100),
		Period:        1,
		Sample:        pb.samples,
		Location:      pb.locations,
		Function:      pb.functions,
	}
}

func (pb *profileBuilder) addMetricTree(root *metricNode) error {
	stack := []*metricNode{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		value,err := pb.getMetricValue(n)
		if err != nil {
			return err
		}
		n.value=value

		loc := pb.genLocation(n)
		pb.locations = append(pb.locations, loc)
		pb.functions = append(pb.functions, loc.Line[0].Function)
		if len(n.children) > 0 {
			pb.queue = append(pb.queue, n)
			for _, child := range n.children {
				stack = append(stack, child)
			}
		} else {
			pb.samples = append(pb.samples, &profile.Sample{
				Value:    []int64{n.value},
				Location: pb.genLocations(n),
			})
			//fmt.Printf("append child: %v,%v -----\n\n", n.name, n.value)
			pb.appendParent()
		}
	}
	if len(pb.queue) != 0 {
		panic(fmt.Sprintf("%v------\n", pb.queue))
	}
	return nil
}

func (pb *profileBuilder) appendParent() {
	if len(pb.queue) == 0 {
		return
	}
	parent := pb.queue[len(pb.queue)-1]
	parent.childrenValue += parent.children[len(parent.children)-1].value
	//fmt.Printf("---------------add child: %v,%v -----\n\n", parent.name, parent.childrenValue)
	parent.children = parent.children[:len(parent.children)-1]
	if len(parent.children) > 0 {
		return
	}
	pb.queue = pb.queue[:len(pb.queue)-1]

	//fmt.Printf("-----------------------------------append parent %v, %v, %v ---\n", parent.name, parent.value, parent.childrenValue)
	if parent.value > parent.childrenValue {
		pb.samples = append(pb.samples, &profile.Sample{
			Value:    []int64{parent.value - parent.childrenValue},
			Location: pb.genLocations(parent),
		})
	}
	pb.appendParent()
}
