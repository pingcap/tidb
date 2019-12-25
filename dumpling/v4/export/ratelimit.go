package export

type rateLimit struct {
	token chan struct{}
}

func newRateLimit(n int) *rateLimit {
	return &rateLimit{
		token: make(chan struct{}, n),
	}
}

func (r *rateLimit) getToken() {
	r.token <- struct{}{}
}

func (r *rateLimit) putToken() {
	select {
	case <-r.token:
	default:
		panic("put a redundant token")
	}
}
