package ddl_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPaginateListSortedSourceDoneJobs(t *testing.T) {
	ddl.MySQLDB = "mysql2"
	t.Cleanup(func() {
		ddl.MySQLDB = "mysql"
	})

	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	t.Cleanup(func() {
		tk.MustExec("drop database if exists mysql2")
	})
	tk.MustExec("create database mysql2")
	tk.MustExec("create table mysql2.gc_delete_range_done like mysql.gc_delete_range_done")

	tk.MustExec(`insert into mysql2.gc_delete_range_done
		(job_id, element_id, start_key, end_key, ts) values
		(100, 1, 'k001', 'k002', 1),
		(101, 10, 'k003', 'k004', 2),
		(101, 20, 'k005', 'k006', 3),
		(102, -1, 'k007', 'k008', 4),
		(103, -100, 'k009', 'k010', 5),
		(105, 5, 'k011', 'k012', 6),
		(105, 2, 'k013', 'k014', 7),
		(105, 4, 'k015', 'k016', 8),
		(105, 1, 'k017', 'k018', 9),
		(105, 6, 'k019', 'k020', 10),
		(105, 0, 'k021', 'k022', 11),
		(105, 3, 'k023', 'k024', 12),
		(106, 0, 'k025', 'k026', 13)`)
	jobs, err := ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		0,
		200,
		1024,
		100,
	)
	require.NoError(t, err)
	require.Len(t, jobs, 13)
	expectedJobIDs := []int64{100, 101, 101, 102, 103, 105, 105, 105, 105, 105, 105, 105, 106}
	expectedStartKeys := map[string]struct{}{
		"k001": {}, "k003": {}, "k005": {}, "k007": {}, "k009": {},
		"k011": {}, "k013": {}, "k015": {}, "k017": {}, "k019": {},
		"k021": {}, "k023": {}, "k025": {},
	}
	expectedEndKeys := map[string]struct{}{
		"k002": {}, "k004": {}, "k006": {}, "k008": {}, "k010": {},
		"k012": {}, "k014": {}, "k016": {}, "k018": {}, "k020": {},
		"k022": {}, "k024": {}, "k026": {},
	}
	gotStartKeys := make(map[string]struct{}, len(jobs))
	gotEndKeys := make(map[string]struct{}, len(jobs))
	for i, job := range jobs {
		require.Equal(t, expectedJobIDs[i], job.DDLJobID)
		gotStartKeys[string(job.StartKey)] = struct{}{}
		gotEndKeys[string(job.EndKey)] = struct{}{}
	}
	require.Equal(t, expectedStartKeys, gotStartKeys)
	require.Equal(t, expectedEndKeys, gotEndKeys)

	// check small page size

	for pageSize := 1; pageSize <= 5; pageSize++ {
		jobs2, err2 := ddl.PaginateListSortedSourceDoneJobs(
			ctx,
			sess.NewSession(tk.Session()),
			0,
			200,
			pageSize,
			100,
		)
		require.NoError(t, err2)
		require.Equal(t, jobs, jobs2)
	}

	// check stop on gc safe point

	jobs2, err2 := ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		100,
		105,
		10000,
		6,
	)
	require.NoError(t, err2)
	expected := []ddl.UnsafeDestroyJob{
		{100, []byte("k001"), []byte("k002")},
		{101, []byte("k003"), []byte("k004")},
		{101, []byte("k005"), []byte("k006")},
		{102, []byte("k007"), []byte("k008")},
		{103, []byte("k009"), []byte("k010")},
	}
	require.Equal(t, expected, jobs2)

	// check range

	jobs2, err2 = ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		100,
		107,
		6,
		100,
	)
	require.NoError(t, err2)
	require.Equal(t, jobs, jobs2)

	jobs2, err2 = ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		102,
		105,
		6,
		100,
	)
	require.NoError(t, err2)
	expected = []ddl.UnsafeDestroyJob{
		{102, []byte("k007"), []byte("k008")},
		{103, []byte("k009"), []byte("k010")},
	}
	require.Equal(t, expected, jobs2)

	jobs2, err2 = ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		105,
		106,
		7,
		100,
	)
	require.NoError(t, err2)
	require.Len(t, jobs2, 7)
	for _, job := range jobs2 {
		require.EqualValues(t, 105, job.DDLJobID)
	}

	jobs2, err2 = ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		0,
		10,
		8,
		100,
	)
	require.NoError(t, err2)
	require.Len(t, jobs2, 0)

	jobs2, err2 = ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		200,
		210,
		9,
		100,
	)
	require.NoError(t, err2)
	require.Len(t, jobs2, 0)

	jobs2, err2 = ddl.PaginateListSortedSourceDoneJobs(
		ctx,
		sess.NewSession(tk.Session()),
		300,
		200,
		9,
		100,
	)
	require.NoError(t, err2)
	require.Len(t, jobs2, 0)
}

func TestPaginateListSourceQueuingJobIDs(t *testing.T) {
	ddl.MySQLDB = "mysql2"
	t.Cleanup(func() {
		ddl.MySQLDB = "mysql"
	})

	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	t.Cleanup(func() {
		tk.MustExec("drop database if exists mysql2")
	})
	tk.MustExec("create database mysql2")
	tk.MustExec("create table mysql2.gc_delete_range like mysql.gc_delete_range")

	tk.MustExec(`insert into mysql2.gc_delete_range
		(job_id, element_id, start_key, end_key, ts) values
		(100, 1, 'k001', 'k002', 1),
		(101, 10, 'k003', 'k004', 2),
		(101, 20, 'k005', 'k006', 3),
		(102, -1, 'k007', 'k008', 4),
		(103, -100, 'k009', 'k010', 5),
		(105, 5, 'k011', 'k012', 6),
		(105, 2, 'k013', 'k014', 7),
		(105, 4, 'k015', 'k016', 8),
		(105, 1, 'k017', 'k018', 9),
		(105, 6, 'k019', 'k020', 10),
		(105, 0, 'k021', 'k022', 11),
		(105, 3, 'k023', 'k024', 12),
		(106, 0, 'k025', 'k026', 13)`)

	jobIDSet, err := ddl.PaginateListSourceQueuingJobIDs(
		ctx,
		sess.NewSession(tk.Session()),
		0,
		200,
		1024,
	)
	require.NoError(t, err)
	expectedJobIDs := map[int64]struct{}{
		100: {}, 101: {}, 102: {}, 103: {}, 105: {}, 106: {},
	}
	require.Equal(t, expectedJobIDs, jobIDSet)

	// check small page size

	for pageSize := 1; pageSize <= 5; pageSize++ {
		jobIDSet2, err2 := ddl.PaginateListSourceQueuingJobIDs(
			ctx,
			sess.NewSession(tk.Session()),
			0,
			200,
			pageSize,
		)
		require.NoError(t, err2)
		require.Equal(t, jobIDSet, jobIDSet2)
	}

	// check range

	jobIDSet2, err2 := ddl.PaginateListSourceQueuingJobIDs(
		ctx,
		sess.NewSession(tk.Session()),
		100,
		107,
		6,
	)
	require.NoError(t, err2)
	require.Equal(t, jobIDSet, jobIDSet2)

	jobIDSet2, err2 = ddl.PaginateListSourceQueuingJobIDs(
		ctx,
		sess.NewSession(tk.Session()),
		102,
		105,
		6,
	)
	require.NoError(t, err2)
	expected := map[int64]struct{}{
		102: {}, 103: {},
	}
	require.Equal(t, expected, jobIDSet2)

	jobIDSet2, err2 = ddl.PaginateListSourceQueuingJobIDs(
		ctx,
		sess.NewSession(tk.Session()),
		105,
		106,
		7,
	)
	require.NoError(t, err2)
	expected = map[int64]struct{}{
		105: {},
	}
	require.Equal(t, expected, jobIDSet2)

	jobIDSet2, err2 = ddl.PaginateListSourceQueuingJobIDs(
		ctx,
		sess.NewSession(tk.Session()),
		0,
		10,
		8,
	)
	require.NoError(t, err2)
	require.Len(t, jobIDSet2, 0)

	jobIDSet2, err2 = ddl.PaginateListSourceQueuingJobIDs(
		ctx,
		sess.NewSession(tk.Session()),
		200,
		210,
		9,
	)
	require.NoError(t, err2)
	require.Len(t, jobIDSet2, 0)

	jobIDSet2, err2 = ddl.PaginateListSourceQueuingJobIDs(
		ctx,
		sess.NewSession(tk.Session()),
		300,
		200,
		9,
	)
	require.NoError(t, err2)
	require.Len(t, jobIDSet2, 0)
}
