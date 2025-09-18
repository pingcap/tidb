package core_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// mock

type simplePlan struct {
	dbName string
	tbl    string
	offset int
}

func (p *simplePlan) Name() string      { return p.tbl }
func (p *simplePlan) DBName() string    { return p.dbName }
func (p *simplePlan) SelectOffset() int { return p.offset }
func newSimplePlan(db, tbl string, offset int) *simplePlan {
	return &simplePlan{dbName: db, tbl: tbl, offset: offset}
}

// simplified HintedTable
type simpleHintedTable struct {
	DBName       string
	TblName      string
	SelectOffset int
}

// simplified findPlanByHintedTable
func findPlanByHintedTable(plans []*simplePlan, hintedTbl *simpleHintedTable) (*simplePlan, int) {
	for i, p := range plans {
		if (hintedTbl.DBName == p.DBName() || hintedTbl.DBName == "*") &&
			hintedTbl.TblName == p.Name() &&
			hintedTbl.SelectOffset == p.SelectOffset() {
			return p, i
		}
	}
	return nil, -1
}

// simplified generateLeadingPlan
func generateLeadingPlan(curJoinGroup []*simplePlan, hintTables []simpleHintedTable, hasOuterJoin bool) (bool, *simplePlan, []*simplePlan) {
	if len(hintTables) == 0 {
		return false, nil, curJoinGroup
	}

	leadingPlans := []*simplePlan{}
	remaining := make([]*simplePlan, len(curJoinGroup))
	copy(remaining, curJoinGroup)

	for _, ht := range hintTables {
		p, idx := findPlanByHintedTable(remaining, &ht)
		if p == nil {
			return false, nil, nil
		}
		leadingPlans = append(leadingPlans, p)
		remaining = append(remaining[:idx], remaining[idx+1:]...)
	}

	// mock join
	result := leadingPlans[0]
	for i := 1; i < len(leadingPlans); i++ {
		result = &simplePlan{
			dbName: "",
			tbl:    "join(" + result.tbl + "," + leadingPlans[i].tbl + ")",
			offset: 0,
		}
	}

	return true, result, remaining
}

// ---------------- Tests ----------------

func TestFindPlanByHintedTable_Basic(t *testing.T) {
	plans := []*simplePlan{
		newSimplePlan("testdb", "t1", 0),
		newSimplePlan("testdb", "t2", 1),
	}

	h := &simpleHintedTable{DBName: "testdb", TblName: "t2", SelectOffset: 1}
	p, idx := findPlanByHintedTable(plans, h)
	require.NotNil(t, p)
	require.Equal(t, 1, idx)

	// DBName = "*"
	h = &simpleHintedTable{DBName: "*", TblName: "t1", SelectOffset: 0}
	p, idx = findPlanByHintedTable(plans, h)
	require.NotNil(t, p)
	require.Equal(t, 0, idx)

	// can't find any
	h = &simpleHintedTable{DBName: "xx", TblName: "yy"}
	p, idx = findPlanByHintedTable(plans, h)
	require.Nil(t, p)
	require.Equal(t, -1, idx)
}

func TestGenerateLeadingPlan_Basic(t *testing.T) {
	group := []*simplePlan{
		newSimplePlan("testdb", "a", 0),
		newSimplePlan("testdb", "b", 0),
	}
	hints := []simpleHintedTable{
		{DBName: "testdb", TblName: "a", SelectOffset: 0},
		{DBName: "testdb", TblName: "b", SelectOffset: 0},
	}

	ok, plan, remain := generateLeadingPlan(group, hints, false)
	require.True(t, ok)
	require.NotNil(t, plan)
	require.Equal(t, "join(a,b)", plan.tbl)
	require.Len(t, remain, 0)
}

func TestGenerateLeadingPlan_TableNotFound(t *testing.T) {
	group := []*simplePlan{
		newSimplePlan("testdb", "a", 0),
	}
	hints := []simpleHintedTable{
		{DBName: "testdb", TblName: "x", SelectOffset: 0},
	}

	ok, plan, remain := generateLeadingPlan(group, hints, false)
	require.False(t, ok)
	require.Nil(t, plan)
	require.Nil(t, remain)
}

func TestGenerateLeadingPlan_EmptyHints(t *testing.T) {
	group := []*simplePlan{
		newSimplePlan("testdb", "a", 0),
	}
	hints := []simpleHintedTable{}

	ok, plan, remain := generateLeadingPlan(group, hints, false)
	require.False(t, ok)
	require.Nil(t, plan)
	require.Len(t, remain, 1)
}
