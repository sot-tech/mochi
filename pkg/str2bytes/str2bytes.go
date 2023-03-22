// Package str2bytes provides fast, but unsafe functions to convert string to []byte
// or vice versa.
package str2bytes

import "unsafe"

// StringToBytes converts string to slice of bytes
// without allocation. Note, that returned slice
// must NOT be modified, since strings in Go are
// immutable.
// See unsafe.Slice.
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// BytesToString converts slice of bytes to string
// without allocation.
// See unsafe.String
func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
