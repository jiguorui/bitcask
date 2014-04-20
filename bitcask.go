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

// Open an existing Bitcask datastore.
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
	var buf []byte

	r = MakeRecord(key, value, ver)

	offset, err = bc.bucket.GetWriteOffset()
	if err != nil {
		goto FAIL
	}

	buf, err = r.Encode()
	if err != nil {
		goto FAIL
	}
	_, err = bc.bucket.Write(buf)
	if err != nil {
		goto FAIL
	}

	total_sz = r.Header.Ksz + r.Header.Vsz + 24

	return uint32(offset), uint32(total_sz), nil
FAIL:
	offset = uint32(0)
	total_sz = uint32(0)
	return uint32(offset), uint32(total_sz), err
}

// Store a key/value in a Bitcask datastore.
func (bc *Bitcask) Put(key string, value []byte) (int32, error) {
	if bc == nil {
		return int32(0), ErrInvalid
	}

	// empty value means delete
	to_delete := len(value) == 0

	var oldver, ver int32
	entry, ok, err := bc.keydir.Get(key)
	if err != nil {
		return int32(0), err
	}
	if ok {
		oldver = entry.Ver
	}
	if oldver < 0 {
		if to_delete {
			return 0, errors.New("has been deleted.")
		}
		ver = 1 - oldver
	} else {
		ver = oldver + 1
		if to_delete {
			ver = 0 - ver
		}
	}

	offset, total_sz, err := bc.writeRecord(key, value, ver)
	if err != nil {
		return int32(0), errors.New("write failed.")
	}

	// if not delete op, add to keydir
	if len(value) > 0 {
		err = bc.keydir.Set(key, uint32(offset), uint32(total_sz), int32(0), int32(ver))
		if err != nil {
			return int32(0), err
		}
	}
	return int32(total_sz), nil
}

// Get value by key
func (bc *Bitcask) Get(key string) ([]byte, error) {
	if bc == nil {
		return []byte(""), ErrInvalid
	}

	entry, has, err := bc.keydir.Get(key)
	if has {
		if entry.Ver > 0 {
			return bc.bucket.Read(entry.Offset, entry.Total_size)			
		}
	}
	if err != nil {
		return []byte(""), err
	}
	return []byte(""), errors.New("not found.")

}

// Delete data by a key
func (bc *Bitcask) Delete(key string) error {
	if bc == nil {
		return ErrInvalid
	}

	_, has, err := bc.keydir.Get(key)

	//To delete, just set empty value
	if has {
		_, err := bc.Put(key, []byte(""))
		return err
	}

	return err
}

// Close a Bitcask
func (bc *Bitcask) Close() {
	if bc == nil {
		return
	}

	bc.bucket.Close()
}
