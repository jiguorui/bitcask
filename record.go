// bitcask: Eric Brewer-inspired key/value store, in Golang
//
// Copyright (c) 2014 Ji-Guorui<jiguorui@gmail.com>. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
	"errors"
)

type Record struct {
	buf []byte
}

func MakeRecord(key string, value []byte) (*Record, uint32, error) {
	var crc uint32
	var tstamp int32
	var ksz, vsz uint32
	var flags, ver int32

	// key should not be empty
	if len(key) <= 0 {
		return nil, 0, errors.New("Invalid key.")
	}

	// is it neccesary here?
	if len(value) < 0 {
		return nil, 0, errors.New("Invalid value bytes.")
	}

	tstamp = getTimestamp()
	ksz = uint32(len(key))
	vsz = uint32(len(value))
	flags = 0
	ver = 0

	buflen := ksz + vsz + 24
	buf := make([]byte, buflen)

	binary.LittleEndian.PutUint32(buf[4:8], uint32(tstamp))
	binary.LittleEndian.PutUint32(buf[8:12], ksz)
	binary.LittleEndian.PutUint32(buf[12:16], vsz)
	binary.LittleEndian.PutUint32(buf[16:20], uint32(flags))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(ver))

	copy(buf[24:24 + ksz], []byte(key))
	copy(buf[24 + ksz:], value)
	//at last, make crc and put it in
	crc = crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return &Record{buf}, buflen, nil
}

func (r *Record) GetBuf() []byte {
	return r.buf
}

func GetKeySize(buf []byte) uint32 {
	ksz := binary.LittleEndian.Uint32(buf[8:12])
	return ksz
}

func GetValueSize(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf[12:16])
}

func getTimestamp() int32 {
 	t0 := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
 	return int32(time.Now().Sub(t0))
}
