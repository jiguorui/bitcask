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

type File struct {
	path    string
	file_id int
	wfile   *os.File
	rfile   *os.File
	keydir  *KeyDir
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

	f := &File{path, id, wf, rf, nil}
	f.keydir, _ = f.scan()

	return f, nil
}

// Get record bytes by key
func (f *File) Get(key string) ([]byte, error) {
	if f == nil {
		return []byte(""), ErrInvalid
	}

	entry, has, err := f.keydir.Get(key)
	if has {
		if entry.Ver > 0 {
			return f.read(entry.Offset, entry.Total_size)			
		}
	}
	if err != nil {
		return []byte(""), err
	}
	return []byte(""), errors.New("not found.")
}

// Put key/value to file and update keydir
func (f *File) Put(key string, value []byte) (int32, error) {
	if f == nil {
		return int32(0), ErrInvalid
	}

	var oldver, ver int32
	entry, ok, err := f.keydir.Get(key)
	if err != nil {
		return int32(0), err
	}
	if ok {
		oldver = entry.Ver
	}
	if oldver < 0 {
		ver = 1 - oldver
	} else {
		ver = oldver + 1
	}

	offset, total_sz, err := f.writeRecord(key, value, ver)
	if err != nil {
		return int32(0), errors.New("write failed.")
	}

	// keydir
	err = f.keydir.Set(key, uint32(offset), uint32(total_sz), int32(0), int32(ver))
	if err != nil {
		return int32(0), err
	}

	return int32(total_sz), nil
}

// Merge the file
func (f *File) Merge(path string) error {

	mf, err := os.OpenFile(path, os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer mf.Close()

	for _, entry := range f.keydir.GetMap() {
		if entry.Ver > 0 {
			buf, err := f.read(entry.Offset, entry.Total_size)
			if err != nil {
				return err
			}
			mf.Write(buf)
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

// local func, write
func (f *File) write(buf []byte) (int32, error) {
	if f == nil {
		return 0, ErrInvalid
	}

	buflen := len(buf)
	n, err := f.wfile.Write(buf)
	if err != nil {
		return int32(n), err
	}
	//TODO: after write failed, file is dirty, how to do here ?
	if n < buflen {
		return int32(n), errors.New("Write op is not complete.")
	}
	return int32(n), nil
}

// local func, read
func (f *File) read(offset, total_sz uint32) ([]byte, error) {
	if f == nil {
		return []byte(""), ErrInvalid
	}

	buf := make([]byte, total_sz)
	o, err := f.rfile.Seek(int64(offset), os.SEEK_SET)
	if err != nil || uint32(o) != offset {
		return []byte(""), errors.New("Can't seek the offset.")
	}
	n, err := f.rfile.Read(buf)
	if err != nil {
		return buf, err
	}

	if uint32(n) < total_sz {
		return buf, errors.New("Not enough bytes to read.")
	}

	return buf, nil
}

// Get current offset for writting
func (f *File) getWriteOffset() (uint32, error) {
	if f == nil {
		return 0, ErrInvalid
	}

	offset, err := f.wfile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}
	return uint32(offset), nil
}

// before call, move the file cursor to right position
// return nil means at the file end.
// any error occur, panic!
func (f *File) readRecordHeader() *RecordHeader {
	buf := make([]byte, 24)
	n, err := f.rfile.Read(buf)
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
func (f *File) move(offset uint32) int32 {
	offset_, err := f.rfile.Seek(int64(offset), os.SEEK_CUR)
	if err != nil {
		panic("Seek file failed.")
	}
	return int32(offset_)
}

// set read cursor position
// panic or success
func (f *File) position(pos uint32) uint32 {
	offset, err := f.rfile.Seek(int64(pos), os.SEEK_SET)
	if err != nil {
		panic(err.Error())
	}
	return uint32(offset)
}

func (f *File) writeRecord(key string, value []byte, ver int32) (uint32, uint32, error) {
	var r *Record
	var offset, total_sz uint32
	var err error
	var buf []byte

	r = MakeRecord(key, value, ver)

	offset, err = f.getWriteOffset()
	if err != nil {
		goto FAIL
	}

	buf, err = r.Encode()
	if err != nil {
		goto FAIL
	}
	_, err = f.write(buf)
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

func (f *File) scan() (*KeyDir, error) {
	if f == nil {
		return nil, ErrInvalid
	}

	f.position(0)

	keydir := NewKeyDir()
	for {
		var oldver int32
		rh := f.readRecordHeader()
		if rh == nil {
			break
		}

		keybuf := make([]byte, rh.Ksz)
		_, err := f.rfile.Read(keybuf)
		if err != nil {
			return nil, err
		}
		entry, has, _ := keydir.Get(string(keybuf))
		if has {
			oldver = entry.Ver
		}

		offset := f.move(rh.Vsz)
		// check if version is last
		if math.Abs(float64(rh.Ver)) > math.Abs(float64(oldver)) {
			total_sz, _ := rh.GetTotalSize()
			keydir.Set(string(keybuf), uint32(offset)-uint32(total_sz), uint32(total_sz), 0, rh.Ver)
		}

	}
	return keydir, nil
}

