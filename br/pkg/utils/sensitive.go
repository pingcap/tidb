package utils

import (
	"regexp"
)

var (
	passwordPatterns = `(password = (\\")?)(.*?)((\\")?\\n)`

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
