package autoanalyze

import "time"

type analyzeItem struct {
	tid                 int64
	LastAnalyzeTS       int64
	LastAnalyzeDuration time.Duration
	isPartition         bool
}

func newAnalyzeItem(tid int64, lastAnalyzeTS int64, lastAnalyzeDuration time.Duration, isPartition bool) *analyzeItem {
	return &analyzeItem{
		tid:                 tid,
		LastAnalyzeTS:       lastAnalyzeTS,
		LastAnalyzeDuration: lastAnalyzeDuration,
		isPartition:         isPartition,
	}
}

func (a *analyzeItem) Less(b *analyzeItem) bool {
	if a.LastAnalyzeTS < b.LastAnalyzeTS {
		return true
	}

	if a.LastAnalyzeDuration < b.LastAnalyzeDuration {
		return true
	}

	if a.isPartition != b.isPartition {
		if !a.isPartition && b.isPartition {
			return true
		}
	}
	return a.tid > b.tid
}

type analyzeProrityQueue []*analyzeItem

func (a *analyzeProrityQueue) Len() int {
	return len(*a)
}

func (a *analyzeProrityQueue) Less(i, j int) bool {
	return (*a)[i].Less((*a)[j])
}

func (a *analyzeProrityQueue) Swap(i, j int) {
	(*a)[i], (*a)[j] = (*a)[j], (*a)[i]
}

func (a *analyzeProrityQueue) Push(x any) {
	*a = append(*a, x.(*analyzeItem))
}

func (a *analyzeProrityQueue) Pop() any {
	old := *a
	n := len(old)
	x := old[n-1]
	*a = old[0 : n-1]
	return x
}
