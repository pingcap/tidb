package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func TestLightningCommonMetricsUnregisteredOnCleanup(t *testing.T) {
	for jobID, m := range tidbmetrics.GetRegisteredJob() {
		tidbmetrics.UnregisterLightningCommonMetricsForDDL(jobID, m)
	}
	require.Empty(t, tidbmetrics.GetRegisteredJob())

	jobID1 := int64(9000001)
	r := &readIndexStepExecutor{job: &model.Job{ID: jobID1}}
	r.metric = tidbmetrics.RegisterLightningCommonMetricsForDDL(jobID1)
	require.Contains(t, tidbmetrics.GetRegisteredJob(), jobID1)
	require.NoError(t, r.Cleanup(context.Background()))
	require.NotContains(t, tidbmetrics.GetRegisteredJob(), jobID1)

	jobID2 := int64(9000002)
	c := &cloudImportExecutor{job: &model.Job{ID: jobID2}}
	c.metric = tidbmetrics.RegisterLightningCommonMetricsForDDL(jobID2)
	require.Contains(t, tidbmetrics.GetRegisteredJob(), jobID2)
	require.NoError(t, c.Cleanup(context.Background()))
	require.NotContains(t, tidbmetrics.GetRegisteredJob(), jobID2)
}
