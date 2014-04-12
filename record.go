package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
	//"fmt"
)

type Record struct {
	crc    uint32
	tstamp int32
	ksz    uint32
	vsz    uint32
	flags  int32
	ver    int32
	key    string
	value  []byte
}

func NewRecord(key string, value []byte, flags, ver int32) *Record {
	crc := uint32(0)
	var ksz, vsz uint32
	ksz = uint32(len(key))
	vsz = uint32(len(value))
	
	t0 := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	tstamp := int32(time.Now().Sub(t0))
	
	return &Record{crc, tstamp, ksz, vsz, flags, ver, key, value}
}

// Record format, |crc32|tstamp|ksz|vsz|flags|ver|  key  |   value   |
func (r *Record) Encode() []byte {
	record_len := r.ksz + r.vsz + 24
	b := make([]byte, record_len)
	binary.LittleEndian.PutUint32(b[4:8], uint32(r.tstamp))
	binary.LittleEndian.PutUint32(b[8:12], r.ksz)
	binary.LittleEndian.PutUint32(b[12:16], r.vsz)
	binary.LittleEndian.PutUint32(b[16:20], uint32(r.flags))
	binary.LittleEndian.PutUint32(b[20:24], uint32(r.ver))

	copy(b[24:24+r.ksz], []byte(r.key))
	copy(b[24+r.ksz:], r.value)
	//at last, crc check
	r.crc = crc32.ChecksumIEEE(b[4:])
	binary.LittleEndian.PutUint32(b[0:4], r.crc)

	return b
}
