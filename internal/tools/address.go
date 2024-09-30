package tools

import "regexp"

var nullAddrRegexp = regexp.MustCompile("^0x[0]+$")

func IsNullAddress(addr string) bool {
	return nullAddrRegexp.Match([]byte(addr))
}
