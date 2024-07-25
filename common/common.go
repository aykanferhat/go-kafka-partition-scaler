package common

import "unsafe"

type EndFunc func()

func ToByte(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
