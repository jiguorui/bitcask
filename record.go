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
	"errors"
	"hash/crc32"
	"time"
)

type RecordHeader struct {
	Crc uint32
	Tstamp int32
	Ksz, Vsz uint32
	Flags, Ver int32	
}

type Record struct {
	Header RecordHeader
	Key string
	Value []byte
}

func MakeRecord(key string, value []byte, ver int32) (*Record) {
	//Just set crc to 0 here.
	Crc := uint32(0)
	Tstamp := getTimestamp()
	Ksz := uint32(len(key))
	Vsz := uint32(len(value))
	Flags := int32(0)
	Ver := ver
	Header := RecordHeader{Crc, Tstamp, Ksz, Vsz, Flags, Ver}
	return &Record{Header, key, value}
}

func (r *Record) Encode() ([]byte, error) {
	if r == nil {
		return []byte(""), ErrInvalid
	}

	buflen := r.Header.Ksz + r.Header.Vsz + 24
	buf := make([]byte, buflen)

	binary.LittleEndian.PutUint32(buf[4:8], uint32(r.Header.Tstamp))
	binary.LittleEndian.PutUint32(buf[8:12], r.Header.Ksz)
	binary.LittleEndian.PutUint32(buf[12:16], r.Header.Vsz)
	binary.LittleEndian.PutUint32(buf[16:20], uint32(r.Header.Flags))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(r.Header.Ver))

	copy(buf[24:24+r.Header.Ksz], []byte(r.Key))
	copy(buf[24+r.Header.Ksz:], r.Value)
	//at last, make crc and put it in
	r.Header.Crc = crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], r.Header.Crc)

	return buf, nil
}

func DecodeRecordHeader(buf []byte) (*RecordHeader, error) {
	if len(buf) != 24 {
		return nil, errors.New("invalid buffer")
	}

	crc    := binary.LittleEndian.Uint32(buf[0:4])
	tstamp := binary.LittleEndian.Uint32(buf[4:8])
	ksz    := binary.LittleEndian.Uint32(buf[8:12])
	vsz    := binary.LittleEndian.Uint32(buf[12:16])
	flags  := binary.LittleEndian.Uint32(buf[16:20])
	ver    := binary.LittleEndian.Uint32(buf[20:24])

	return &RecordHeader{crc, int32(tstamp), ksz, vsz, int32(flags), int32(ver)}, nil
}

func (rh *RecordHeader) GetTotalSize() (int, error) {
	if rh == nil {
		return 0, ErrInvalid
	}

	return int(rh.Ksz + rh.Vsz + 24), nil
}

func getTimestamp() int32 {
	t0 := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	return int32(time.Now().Sub(t0))
}
