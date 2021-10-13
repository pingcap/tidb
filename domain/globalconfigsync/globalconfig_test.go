package globalconfigsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGlobalConfigSyncer(t *testing.T) {
	syncer := NewGlobalConfigSyncer(nil)
	syncer.Notify("a", "b")
	require.Equal(t, len(syncer.NotifyCh), 1)
	entry := <-syncer.NotifyCh
	require.Equal(t, entry.name, "a")
	require.Equal(t, true, syncer.needUpdate(entry))
	syncer.updateCache(entry)
	require.Equal(t, false, syncer.needUpdate(entry))
	entry.name = "c"
	require.Equal(t, true, syncer.needUpdate(entry))
	err := syncer.StoreGlobalConfig(context.Background(), entry)
	require.NoError(t, err)
}
