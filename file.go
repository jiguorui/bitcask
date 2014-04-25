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
	"os"
	"math"
	//"fmt"
)

const (
	recordHeaderSize int = 24
)

var emptyKey string = ""
var emptyValue []byte = []byte("")

type File struct {
	path    string
	file_id int
	wfile   *os.File
	rfile   *os.File
}

var ErrInvalid = errors.New("invalid argument")

func OpenFile(path string, id int) (*File, error) {

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, errors.New("File is not exist.")
	}

	wf, err := os.OpenFile(path, os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}

	rf, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	f := &File{path, id, wf, rf}

	return f, nil
}

//write key/value to file and return (offset, size, err)
func (f *File) Write(key string, value []byte, ver int32) (uint32, uint32, error) {
	if f == nil {
		return 0, 0, ErrInvalid
	}

	// be sure append(?) and get offset
	offset_, err := f.wfile.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, 0, err
	}

	// any problem here ?
	offset := uint32(offset_)

	crc := uint32(0)
	tstamp := int32(0)

	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	flags := int32(0)
	//ver
	totalSize := recordHeaderSize + int(keySize) + int(valueSize)

	buf := make([]byte, totalSize)
	//crc, tstamp, ksz, vsz, flags, version
	binary.LittleEndian.PutUint32(buf[4:8], uint32(tstamp))
	binary.LittleEndian.PutUint32(buf[8:12], keySize)
	binary.LittleEndian.PutUint32(buf[12:16], valueSize)
	binary.LittleEndian.PutUint32(buf[16:20], uint32(flags))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(ver))

	copy(buf[24:24+keySize], []byte(key))
	copy(buf[24+keySize:], value)
	//at last, make crc and put it in
	crc = crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	n, err := f.wfile.Write(buf)

	// what if n < totalSize ?
	return offset, uint32(n), err
}

func (f *File) Read(offset, size uint32) (key string, value []byte, err error) {
	if f == nil {
		return emptyKey, emptyValue, ErrInvalid
	}

	_, err = f.rfile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return emptyKey, emptyValue, err
	}

	buf := make([]byte, size)
	n, err := f.rfile.Read(buf)
	if err != nil {
		return emptyKey, emptyValue, err
	}

	if n < int(size) {
		return emptyKey, emptyValue, errors.New("read not complete.")
	}

	crc := binary.LittleEndian.Uint32(buf[0:4])
	crc_ := crc32.ChecksumIEEE(buf[4:])
	if crc != crc_ {
		return emptyKey, emptyValue, errors.New("crc check failed.") 
	}

	//tstamp := binary.LittleEndian.Uint32(buf[4:8])
	ksz := binary.LittleEndian.Uint32(buf[8:12])
	//vsz := binary.LittleEndian.Uint32(buf[12:16])
	//flags := binary.LittleEndian.Uint32(buf[16:20])
	//ver := binary.LittleEndian.Uint32(buf[20:24])

	key = string(buf[recordHeaderSize : int(ksz)+recordHeaderSize])
	value = buf[int(ksz)+recordHeaderSize:]

	return key, value, nil
}

func (f *File) Scan(keydir *KeyDir) error {
	if f == nil || keydir == nil {
		return ErrInvalid
	}

	_, err := f.rfile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	for {
		buf := make([]byte, recordHeaderSize)
		n, err := f.rfile.Read(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}

		//crc := binary.LittleEndian.Uint32(buf[0:4])
		tstamp := binary.LittleEndian.Uint32(buf[4:8])
		ksz := binary.LittleEndian.Uint32(buf[8:12])
		vsz := binary.LittleEndian.Uint32(buf[12:16])
		//flags := binary.LittleEndian.Uint32(buf[16:20])
		ver := binary.LittleEndian.Uint32(buf[20:24])

		keybuf := make([]byte, ksz)
		n, err = f.rfile.Read(keybuf)
		if err != nil {
			return err
		}

		var oldver int32
		key := string(keybuf)
		entry, ok, err := keydir.Get(key)
		if ok {
			oldver = entry.Version
		}
		offset_, err := f.rfile.Seek(int64(vsz), os.SEEK_CUR)

		if math.Abs(float64(ver)) > math.Abs(float64(oldver)) {
			totalSz := ksz + vsz + uint32(recordHeaderSize)
			offset := uint32(offset_) - totalSz
			keydir.Put(key, uint32(f.file_id), offset, totalSz, int32(tstamp), int32(ver))
		}
	}

	return nil
}


// Close file
func (f *File) Close() {
	if f == nil {
		return
	}
	f.wfile.Close()
	f.rfile.Close()
}

