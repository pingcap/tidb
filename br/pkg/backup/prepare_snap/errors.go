package preparesnap

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
)

func convertErr(err *errorpb.Error) error {
	if err == nil {
		return nil
	}
	return errors.New(err.Message)
}

func errLeaseExpired() error {
	return errors.New("the lease has expired")
}

func unsupported() error {
	return errors.New("the lease has expired")
}

func retryLimitExceeded() error {
	return errors.New("the limit of retrying exceeded")
}
