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

func (bucket *Bucket) Scan() (*KeyDir, error) {
	if bucket == nil {
		return nil, ErrInvalid
	}

	offset, err := bucket.rfile.Seek(0, os.SEEK_SET)
	if err != nil || offset != 0 {
		return nil, errors.New("Seek file to start failed.")
	}

	keydir := NewKeyDir()
	for {
		buf := make([]byte, 24)
		n, err := bucket.rfile.Read(buf)
		if err == io.EOF {
			break
		}
		if n < 24 {
			return nil, errors.New("Scan error.")
		}

		ksz := GetKeySize(buf)
		keybuf := make([]byte, ksz)
		_, err = bucket.rfile.Read(keybuf)
		if err != nil {
			return nil, err
		}

		vsz := GetValueSize(buf)
		if vsz == 0 {
			//Deleted, do not need put it into keydir
			keydir.Delete(string(keybuf))
			continue
		}
		offset, err := bucket.rfile.Seek(int64(vsz), os.SEEK_CUR)
		if err != nil {
			return nil, errors.New("Seek file failed.")
		}

		ver := GetVersion(buf)
		keydir.Set(string(keybuf), uint32(offset)-(ksz+vsz+24), (ksz + vsz + 24), 0, ver)

	}
	return keydir, nil
}

// Close file
func (bucket *Bucket) Close() {
	if bucket == nil {
		return
	}
	bucket.wfile.Close()
	bucket.rfile.Close()
}
