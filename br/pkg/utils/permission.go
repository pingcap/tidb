package utils

import "strings"

var (
	ioNotFoundMsg       = "notfound"
	permissionDeniedMsg = "permissiondenied"
)

// messageIsNotFoundStorageError checks whether the message returning from TiKV is "NotFound" storage I/O error
func messageIsNotFoundStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, "io") && strings.Contains(msgLower, ioNotFoundMsg)
}

// MessageIsPermissionDeniedStorageError checks whether the message returning from TiKV is "PermissionDenied" storage I/O error
func messageIsPermissionDeniedStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, permissionDeniedMsg)
}
