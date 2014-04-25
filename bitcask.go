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
	//"strings"
	//"fmt"
	//"errors"
	//"math"
)

type Bitcask struct {
	files       []*File
	active_file *File
	keydir *KeyDir
}

// // Open an existing Bitcask datastore.
// func Open(dir string) (*Bitcask, error) {
// 	fnames := []string{"002.data", "003.data", "004.data"}
// 	sep := "/"
// 	if strings.HasSuffix(dir, "/") {
// 		sep = ""
// 	}

// 	files := make([]*File, 0)
// 	var active_file *File
// 	for i := 0; i < len(fnames); i++ {
// 		s := []string{dir, fnames[i]}
// 		path := strings.Join(s, sep)

// 		f, err := OpenFile(path, i+1)
// 		if err != nil {
// 			return nil, err
// 		}
// 		files = append(files, f)

// 		// find active file
// 		sz, err := f.Size()
// 		if err == nil && sz < 0xffff {
// 			active_file = f
// 			continue
// 		}
// 	}

// 	// check if has a active file
// 	if active_file == nil {
// 		return nil, errors.New("active file not found.")
// 	}

// 	keydir := NewKeyDir()
// 	for i := 0; i < len(files); i++ {
// 		files[i].Scan(keydir)
// 	}

// 	return &Bitcask{files, active_file}, nil
// }

// func (bc *Bitcask) Put(key string, value []byte) (int32, error) {
// 	if bc == nil {
// 		return int32(0), ErrInvalid
// 	}

// 	var oldver, ver int32
// 	entry, ok, err := f.keydir.Get(key)
// 	if err != nil {
// 		return int32(0), err
// 	}
// 	if ok {
// 		oldver = entry.Ver
// 	}
// 	if oldver < 0 {
// 		ver = 1 - oldver
// 	} else {
// 		ver = oldver + 1
// 	}

// 	offset, total_sz, err := f.WriteRecord(key, value, ver)
// 	if err != nil {
// 		return int32(0), errors.New("write failed.")
// 	}

// 	// keydir
// 	err = f.keydir.Set(key, uint32(offset), uint32(total_sz), int32(0), int32(ver))
// 	if err != nil {
// 		return int32(0), err
// 	}

// 	return int32(total_sz), nil
// }

// // Now the code here is not good enough
// func (bc *Bitcask) Get(key string) ([]byte, error) {
// 	if bc == nil {
// 		return []byte(""), ErrInvalid
// 	}


// 	// c := make(chan int)
// 	// b := make([][]byte, 0)
// 	// cnt := len(bc.files)

// 	// var i int
// 	// for i = 0; i < cnt; i++ {
// 	// 	go func() {

// 	// 		b1, err := bc.files[i].Get(key)
// 	// 		if err == nil {
// 	// 			b = append(b, b1)
// 	// 		}
// 	// 		c <- 1
// 	// 	}()
// 	// }
// 	// for i = 0; i < cnt; i++ {
// 	// 	<-c
// 	// }
// 	// for i := 0; i < len(b); i++ {
// 	// 	if len(b[i]) > 0 {
// 	// 		return b[i], nil
// 	// 	}
// 	// }
// 	// return []byte(""), errors.New("get failed")
// }

// func (bc *Bitcask) Delete(key string) (error) {
// 	if bc == nil {
// 		return ErrInvalid
// 	}

// 	v, err := bc.getVersion(key)
// 	if err != nil {
// 		return err
// 	}

// 	if v < 0 {
// 		return errors.New("Has been deleted.")
// 	}

// 	v = -1 - v
// 	_, err = bc.active_file.Put(key, []byte("Tombstone"), v)
// 	return err
// }

// func (bc *Bitcask) getVersion(key string) (int32, error) {
// 	cnt := len(bc.files)
// 	c := make(chan int32)

// 	var version int32

// 	var i int
// 	for i = 0; i < cnt; i++ {
// 		go func() {
// 			v, _ := bc.files[i].GetVersion(key)
// 			c <- v
// 		}()
// 	}
// 	for i = 0; i < cnt; i++ {
// 		v := <-c
// 		if math.Abs(float64(version)) < math.Abs(float64(v)) {
// 			version = v
// 		}
// 	}

// 	return version, nil
// }

// func (bc *Bitcask) getHint(key string) (int, *KeyEntry, error) {
// 	cnt := len(bc.files)
// 	c := make(chan int)
// 	hints := make([]*KeyEntry, 0)

// 	var i int
// 	for i = 0; i < cnt; i++ {
// 		go func() {
// 			hint, _ := bc.files[i].GetHint(key)
// 			hints = append(hints, hint)
// 			c <- 1
// 		}()
// 	}
// 	for i = 0; i < cnt; i++ {
// 		<-c
// 	}

// 	var version int32
// 	var fid int
// 	for i = 0; i < cnt; i++ {
// 		if hints[i] != nil {
// 			if math.Abs(float64(version)) < math.Abs(hints[i].Ver) {
// 				version = hints[i].Ver
// 				fid = i + 1
// 			}
// 		}
// 	}
// 	if fid > 0 {
// 		return fid - 1, hints[fid-1], nil
// 	}
// 	return 0, nil, errors.New("not found")

// }

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
