package bitcask

import (
	"time"
)

func Tstamp() int32 {
	return int32(time.Since(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))/time.Second)
}
