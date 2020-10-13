package dbterror

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/assert"
)

func TestDBError(t *testing.T) {
	original := errors.RedactLogEnabled
	errors.RedactLogEnabled = true
	defer func() { errors.RedactLogEnabled = original }()

	c := ErrClass{}
	err := c.NewStd(errno.ErrDupEntry).GenWithStackByArgs("sensitive", "data")
	assert.Contains(t, err.Error(), "?")
	assert.NotContains(t, err.Error(), "sensitive")
	assert.NotContains(t, err.Error(), "data")
}
