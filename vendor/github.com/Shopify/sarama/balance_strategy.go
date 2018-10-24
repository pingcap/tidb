package sarama

import (
	"math"
	"sort"
)

// BalanceStrategyPlan is the results of any BalanceStrategy.Plan attempt.
// It contains an allocation of topic/partitions by memberID in the form of
// a `memberID -> topic -> partitions` map.
type BalanceStrategyPlan map[string]map[string][]int32

// Add assigns a topic with a number partitions to a member.
func (p BalanceStrategyPlan) Add(memberID, topic string, partitions ...int32) {
	if len(partitions) == 0 {
		return
	}
	if _, ok := p[memberID]; !ok {
		p[memberID] = make(map[string][]int32, 1)
	}
	p[memberID][topic] = append(p[memberID][topic], partitions...)
}

// --------------------------------------------------------------------

// BalanceStrategy is used to balance topics and partitions
// across memebers of a consumer group
type BalanceStrategy interface {
	// Name uniquely identifies the strategy.
	Name() string

	// Plan accepts a map of `memberID -> metadata` and a map of `topic -> partitions`
	// and returns a distribution plan.
	Plan(members map[string]ConsumerGroupMemberMetadata, topics map[string][]int32) (BalanceStrategyPlan, error)
}

// --------------------------------------------------------------------

// BalanceStrategyRange is the default and assigns partitions as ranges to consumer group members.
// Example with one topic T with six partitions (0..5) and two members (M1, M2):
//   M1: {T: [0, 1, 2]}
//   M2: {T: [3, 4, 5]}
var BalanceStrategyRange = &balanceStrategy{
	name: "range",
	coreFn: func(plan BalanceStrategyPlan, memberIDs []string, topic string, partitions []int32) {
		step := float64(len(partitions)) / float64(len(memberIDs))

		for i, memberID := range memberIDs {
			pos := float64(i)
			min := int(math.Floor(pos*step + 0.5))
			max := int(math.Floor((pos+1)*step + 0.5))
			plan.Add(memberID, topic, partitions[min:max]...)
		}
	},
}

// BalanceStrategyRoundRobin assigns partitions to members in alternating order.
// Example with topic T with six partitions (0..5) and two members (M1, M2):
//   M1: {T: [0, 2, 4]}
//   M2: {T: [1, 3, 5]}
var BalanceStrategyRoundRobin = &balanceStrategy{
	name: "roundrobin",
	coreFn: func(plan BalanceStrategyPlan, memberIDs []string, topic string, partitions []int32) {
		for i, part := range partitions {
			memberID := memberIDs[i%len(memberIDs)]
			plan.Add(memberID, topic, part)
		}
	},
}

// --------------------------------------------------------------------

type balanceStrategy struct {
	name   string
	coreFn func(plan BalanceStrategyPlan, memberIDs []string, topic string, partitions []int32)
}

// Name implements BalanceStrategy.
func (s *balanceStrategy) Name() string { return s.name }

// Balance implements BalanceStrategy.
func (s *balanceStrategy) Plan(members map[string]ConsumerGroupMemberMetadata, topics map[string][]int32) (BalanceStrategyPlan, error) {
	// Build members by topic map
	mbt := make(map[string][]string)
	for memberID, meta := range members {
		for _, topic := range meta.Topics {
			mbt[topic] = append(mbt[topic], memberID)
		}
	}

	// Sort members for each topic
	for topic, memberIDs := range mbt {
		sort.Sort(&balanceStrategySortable{
			topic:     topic,
			memberIDs: memberIDs,
		})
	}

	// Assemble plan
	plan := make(BalanceStrategyPlan, len(members))
	for topic, memberIDs := range mbt {
		s.coreFn(plan, memberIDs, topic, topics[topic])
	}
	return plan, nil
}

type balanceStrategySortable struct {
	topic     string
	memberIDs []string
}

func (p balanceStrategySortable) Len() int { return len(p.memberIDs) }
func (p balanceStrategySortable) Swap(i, j int) {
	p.memberIDs[i], p.memberIDs[j] = p.memberIDs[j], p.memberIDs[i]
}
func (p balanceStrategySortable) Less(i, j int) bool {
	return balanceStrategyHashValue(p.topic, p.memberIDs[i]) < balanceStrategyHashValue(p.topic, p.memberIDs[j])
}

func balanceStrategyHashValue(vv ...string) uint32 {
	h := uint32(2166136261)
	for _, s := range vv {
		for _, c := range s {
			h ^= uint32(c)
			h *= 16777619
		}
	}
	return h
}
