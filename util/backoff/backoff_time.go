package backoff

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	WaitScatterRegionFinishBackoff = 120000
)

// BackoffTime is used for session to set back off time(in ms).
type BackoffTime struct {
	waitScatterRegionFinish int
}

func (b *BackoffTime) GetWaitScatterRegionFinishBackoff() int {
	if b.waitScatterRegionFinish > 0 {
		return b.waitScatterRegionFinish
	}
	return WaitScatterRegionFinishBackoff
}

func (b *BackoffTime) SetWaitScatterRegionFinishBackoff(t int) {
	if t > 0 {
		b.waitScatterRegionFinish = t
	}
}
