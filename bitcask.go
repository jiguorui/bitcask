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
	files       []*File
	active_file *File
}

// Open an existing Bitcask datastore.
func Open(dir string) (*Bitcask, error) {
	fnames := []string{"001.data", "002.data"}
	sep := "/"
	if strings.HasSuffix(dir, "/") {
		sep = ""
	}

	files := make([]*File, 0)
	for i := 0; i < len(fnames); i++ {
		s := []string{dir, fnames[i]}
		path := strings.Join(s, sep)

		f, err := OpenFile(path, i+1)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}

	return &Bitcask{files, files[0]}, nil
}

func (bc *Bitcask) Put(key string, value []byte) (int32, error) {
	if bc == nil {
		return int32(0), ErrInvalid
	}

	return bc.active_file.Put(key, value)
}

// Now the code here is not good enough
func (bc *Bitcask) Get(key string) ([]byte, error) {
	if bc == nil {
		return []byte(""), ErrInvalid
	}

	c := make(chan int)
	b := make([][]byte, 0)
	cnt := len(bc.files)

	var i int
	for i = 0; i < cnt; i++ {
		go func() {

			b1, err := bc.files[i].Get(key)
			if err == nil {
				b = append(b, b1)
			}
			c <- 1
		}()
	}
	for i = 0; i < cnt; i++ {
		<-c
	}
	for i := 0; i < len(b); i++ {
		if len(b[i]) > 0 {
			return b[i], nil
		}
	}
	return []byte(""), errors.New("get failed")
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
	//bc.bucket.Close()
}
