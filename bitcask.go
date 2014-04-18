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
	"strings"
	//"fmt"
	"errors"
)

type Bitcask struct {
	bucket *Bucket
	keydir *KeyDir
}

// Open a existing Bitcask datastore.
func Open(dir string) (*Bitcask, error) {
	s := []string{dir, "001.ar"}
	sep := "/"
	if strings.HasSuffix(dir, "/") {
		sep = ""
	}
	path := strings.Join(s, sep)

	bk, err := NewBucket(path, 1)
	if err != nil {
		return nil, err
	}

	keydir, err := bk.Scan()
	if err != nil {
		return nil, err
	}

	keydir.DebugShow()

	return &Bitcask{bk, keydir}, nil
}

// Local func
func (bc *Bitcask) writeRecord(key string, value []byte, ver int32) (uint32, uint32, error) {
	var r *Record
	var offset, total_sz uint32
	var err error

	r, total_sz, err = MakeRecord(key, value, ver)
	if err != nil {
		goto FAIL
	}

	offset, err = bc.bucket.GetWriteOffset()
	if err != nil {
		goto FAIL
	}

	_, err = bc.bucket.Write(r.GetBuf())
	if err != nil {
		goto FAIL
	}

	return uint32(offset), uint32(total_sz), nil
FAIL:
	offset = uint32(0)
	total_sz = uint32(0)
	return uint32(offset), uint32(total_sz), err
}

// Store a key/value in a Bitcask datastore.
func (bc *Bitcask) Set(key string, value []byte) (int32, error) {
	if bc == nil {
		return int32(0), ErrInvalid
	}

	var oldver, ver int32
	entry, err := bc.keydir.Get(key)
	if err != nil || entry == nil {
		oldver = 0
	} else {
		oldver = entry.Ver
	}
	ver = oldver + 1

	offset, total_sz, err := bc.writeRecord(key, value, ver)
	if err != nil {
		return int32(0), errors.New("write failed.")
	}

	err = bc.keydir.Set(key, uint32(offset), uint32(total_sz), int32(0), int32(ver))
	if err != nil {
		return int32(0), err
	}

	return int32(total_sz), nil
}

// Add a key/value into store only if it is not exists.
func (bc *Bitcask) Add(key string, value []byte) (int32, error) {
	if bc == nil {
		return int32(0), ErrInvalid
	}

	entry, err := bc.keydir.Get(key)
	if err != nil && entry == nil {
		return bc.Set(key, value)
	}
	return 0, errors.New("Add failed: invalid or key exists.")
}

// Get value by key
func (bc *Bitcask) Get(key string) ([]byte, error) {
	if bc == nil {
		return []byte(""), ErrInvalid
	}

	entry, err := bc.keydir.Get(key)
	if err != nil {
		return []byte(""), err
	}
	return bc.bucket.Read(entry.Offset, entry.Total_size)
}

// Delete data by a key
func (bc *Bitcask) Delete(key string) error {
	if bc == nil {
		return ErrInvalid
	}

	entry, err := bc.keydir.Get(key)
	if err != nil && entry == nil {
		return errors.New("No exists.")
	}
	//keydir delete
	bc.keydir.Delete(key)

	//To delete, just set empty value
	_, err = bc.Set(key, []byte(""))
	return err
}

// Close a Bitcask
func (bc *Bitcask) Close() {
	if bc == nil {
		return
	}

	bc.bucket.Close()
}
