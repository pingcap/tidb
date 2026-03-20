package ddl

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

type mockPDCli struct {
	pd.Client
	lastCalledKey   string
	lastCalledValue string
}

func (c *mockPDCli) SetLogReplMetadata(_ context.Context, key string, value string) error {
	c.lastCalledKey = key
	c.lastCalledValue = value
	return nil
}

func TestRunJobs(t *testing.T) {
	ctx := context.Background()
	mockCli := &mockPDCli{}
	worker := newMockUnsafeDestroyWorker(ctx, mockCli)

	jobs := []UnsafeDestroyJob{
		{100, []byte("k001"), []byte("k002")},
		{101, []byte("k003"), []byte("k004")},
		{102, []byte("k005"), []byte("k006")},
		{102, []byte("k007"), []byte("k008")},
		{102, []byte("k009"), []byte("k010")},
		{102, []byte("k011"), []byte("k012")},
		{102, []byte("k013"), []byte("k014")},
		{102, []byte("k015"), []byte("k016")},
		{102, []byte("k017"), []byte("k018")},
		{102, []byte("k019"), []byte("k020")},
		{102, []byte("k021"), []byte("k022")},
		{102, []byte("k023"), []byte("k024")},
		{102, []byte("k025"), []byte("k026")},
		{102, []byte("k027"), []byte("k028")},
		{103, []byte("k029"), []byte("k030")},
		{105, []byte("k031"), []byte("k032")},
		{105, []byte("k033"), []byte("k034")},
	}

	collected := [][2][]byte{}
	collectedMu := sync.Mutex{}

	DoUnsafeDestroyRangeRequest = func(ctx context.Context,
		startKey []byte,
		endKey []byte,
		pdCli pd.Client,
		tikvStore tikv.Storage,
		uuid string,
	) error {
		collectedMu.Lock()
		collected = append(collected, [2][]byte{startKey, endKey})
		collectedMu.Unlock()
		return nil
	}

	err := worker.runJobs(jobs)
	require.NoError(t, err)

	require.Len(t, collected, 17)
	slices.SortFunc(collected, func(a, b [2][]byte) int {
		return bytes.Compare(a[0], b[0])
	})

	i := 1
	for _, job := range collected {
		expectedStart := fmt.Sprintf("k%03d", i)
		expectedEnd := fmt.Sprintf("k%03d", i+1)
		require.Equal(t, expectedStart, string(job[0]))
		require.Equal(t, expectedEnd, string(job[1]))
		i += 2
	}

	require.Equal(t, unsafeDestroyNextDDLJobIDKey, mockCli.lastCalledKey)
	require.Equal(t, "106", mockCli.lastCalledValue)
}
