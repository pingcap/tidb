// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"regexp"
)

var (
	passwordPatterns = `(password[\s]*=[\s]*(\\")?)(.*?)((\\")?\\n)`

	passwordRegexp *regexp.Regexp
)

func init() {
	passwordRegexp = regexp.MustCompile(passwordPatterns)
}

// HideSensitive replace password with ******.
func HideSensitive(input string) string {
	output := passwordRegexp.ReplaceAllString(input, "$1******$4")
	return output
}
