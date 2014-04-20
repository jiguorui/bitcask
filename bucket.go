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
	//"bufio"
	"errors"
	"io"
	"os"
	"math"
	//"fmt"
)

type Bucket struct {
	path    string
	file_id int
	wfile   *os.File
	rfile   *os.File
}

var ErrInvalid = errors.New("invalid argument")

func NewBucket(path string, id int) (*Bucket, error) {

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

	return &Bucket{path, id, wf, rf}, nil
}

// Write bytes to file
func (bucket *Bucket) Write(buf []byte) (int32, error) {
	if bucket == nil {
		return 0, ErrInvalid
	}

	buflen := len(buf)
	n, err := bucket.wfile.Write(buf)
	if err != nil {
		return int32(n), err
	}
	//TODO: after write failed, file is dirty, how to do here ?
	if n < buflen {
		return int32(n), errors.New("Write op is not complete.")
	}
	return int32(n), nil
}

// Read bytes form file
func (bucket *Bucket) Read(offset, total_sz uint32) ([]byte, error) {
	if bucket == nil {
		return []byte(""), ErrInvalid
	}

	buf := make([]byte, total_sz)
	o, err := bucket.rfile.Seek(int64(offset), os.SEEK_SET)
	if err != nil || uint32(o) != offset {
		return []byte(""), errors.New("Can't seek the offset.")
	}
	n, err := bucket.rfile.Read(buf)
	if err != nil {
		return buf, err
	}

	if uint32(n) < total_sz {
		return buf, errors.New("Not enough bytes to read.")
	}

	return buf, nil
}

// Get current offset for writting
func (bucket *Bucket) GetWriteOffset() (uint32, error) {
	if bucket == nil {
		return 0, ErrInvalid
	}

	offset, err := bucket.wfile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}
	return uint32(offset), nil
}

// before call, move the file cursor to right position
// return nil means at the file end.
// any error occur, panic!
func (bucket *Bucket) readRecordHeader() *RecordHeader {
	buf := make([]byte, 24)
	n, err := bucket.rfile.Read(buf)
	if err == io.EOF && n == 0 {
		return nil
	}
	// if any error, panic!!!
	if err != nil {
		panic(err.Error())
	}
	if n < 24 {
		panic("Read Header error.")
	}

	rh, err := DecodeRecordHeader(buf)
	if err != nil {
		panic(err.Error())
	}
	return rh
}

// move read cursor by offset from current position
// panic or success.
func (bucket *Bucket) move(offset uint32) int32 {
	offset_, err := bucket.rfile.Seek(int64(offset), os.SEEK_CUR)
	if err != nil {
		panic("Seek file failed.")
	}
	return int32(offset_)
}

// set read cursor position
// panic or success
func (bucket *Bucket) position(pos uint32) uint32 {
	offset, err := bucket.rfile.Seek(int64(pos), os.SEEK_SET)
	if err != nil {
		panic(err.Error())
	}
	return uint32(offset)
}

func (bucket *Bucket) Scan() (*KeyDir, error) {
	if bucket == nil {
		return nil, ErrInvalid
	}

	bucket.position(0)

	keydir := NewKeyDir()
	for {
		var oldver int32
		rh := bucket.readRecordHeader()
		if rh == nil {
			break
		}

		keybuf := make([]byte, rh.Ksz)
		_, err := bucket.rfile.Read(keybuf)
		if err != nil {
			return nil, err
		}
		entry, has, _ := keydir.Get(string(keybuf))
		if has {
			oldver = entry.Ver
		}

		offset := bucket.move(rh.Vsz)
		// check if version is last
		if math.Abs(float64(rh.Ver)) > math.Abs(float64(oldver)) {
			total_sz, _ := rh.GetTotalSize()
			keydir.Set(string(keybuf), uint32(offset)-uint32(total_sz), uint32(total_sz), 0, rh.Ver)
		}

	}
	return keydir, nil
}

func (bucket *Bucket) Merge(path string) error {
	keydir, err := bucket.Scan()
	if err != nil {
		return err
	}

	mf, err := os.OpenFile(path, os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}

	for _, entry := range keydir.GetMap() {
		buf, err := bucket.Read(entry.Offset, entry.Total_size)
		if err != nil {
			return err
		}
		mf.Write(buf)
	}
	mf.Close()
	return nil
}

// Close file
func (bucket *Bucket) Close() {
	if bucket == nil {
		return
	}
	bucket.wfile.Close()
	bucket.rfile.Close()
}
