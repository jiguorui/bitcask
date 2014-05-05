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
	//"math"
)

const (
	maxFileSize uint32 = 0x01000000
	maxFileNum uint32 = 5
)

type Bitcask struct {
	files       []*File
	active_fid int
	keydir      *KeyDir
}

// Open an existing Bitcask datastore.
func Open(dir string) (*Bitcask, error) {
	fnames := []string{"001.data", "002.data", "003.data", "004.data", "005.data"}
	sep := "/"
	if strings.HasSuffix(dir, "/") {
		sep = ""
	}

	var active_fid int
	files := make([]*File, 0)

	for i := 0; i < len(fnames); i++ {
		s := []string{dir, fnames[i]}
		path := strings.Join(s, sep)

		f, err := OpenFile(path, i)
		if err != nil {
			return nil, err
		}
		files = append(files, f)

		// find active file
		sz, err := f.Size()
		if err == nil && sz < maxFileSize {
			active_fid = 0
			continue
		}
	}

	keydir := NewKeyDir()
	for i := 0; i < len(files); i++ {
		files[i].Scan(keydir)
	}

	return &Bitcask{files, active_fid, keydir}, nil
}

func (bc *Bitcask) Put(key string, value []byte) (int, error) {
	if bc == nil {
		return 0, ErrInvalid
	}

	tstamp := Tstamp()
	offset, size, err := bc.files[bc.active_fid].Write(key, value, tstamp)
	if err != nil {
		err = bc.files[bc.active_fid].Unwrite()
		return 0, err
	}

	err = bc.keydir.Put(key, uint32(bc.active_fid), offset, size, tstamp)
	if offset > maxFileSize {
		bc.active_fid ++
	}
	return int(size), err
}

func (bc *Bitcask) Get(key string) ([]byte, error) {
	if bc == nil {
		return emptyValue, ErrInvalid
	}

	entry, ok, err := bc.keydir.Get(key)
	if err != nil {
		return emptyValue, err
	}
	if ok {
		_, value, err := bc.files[entry.FileId].Read(entry.Offset, entry.TotalSize)
		if err != nil {
			return emptyValue, err
		}

		return value, nil
	}
	return emptyValue, errors.New("not found")
}

// Close a Bitcask
func (bc *Bitcask) Close() {
	if bc == nil {
		return
	}
	cnt := len(bc.files)
	for i := 0; i < cnt; i++ {
		bc.files[i].Close()
	}
}
