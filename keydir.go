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
	//"errors"
//	"fmt"
)

type KeyEntry struct {
	FileId     uint32
	Offset     uint32
	TotalSize uint32
	Tstamp int32
	Version int32
}

type KeyDir struct {
	map_ map[string]KeyEntry
}

func NewKeyDir() *KeyDir {
	map_ := make(map[string]KeyEntry)
	return &KeyDir{map_}
}

func (dir *KeyDir) Put(key string, fileId, offset, totalSize uint32, tstamp, version int32) (int32, error) {
	if dir == nil {
		return 0, ErrInvalid
	}

	// var oldver int32
	// entry1, ok := dir.map_[key]
	// if ok {
	// 	oldver = entry1.Version
	// }

	// if oldver < 0 {
	// 	if version < 0 {
	// 		return 0, errors.New("Has been deleted.")
	// 	}
	// 	version = 1 - oldver
	// } else {
	// 	if version < 0 {
	// 		version = -1 - oldver
	// 	} else {
	// 		version = oldver + 1
	// 	}
	// }

	entry := KeyEntry{fileId, offset, totalSize, tstamp, version}
	dir.map_[key] = entry

	return version, nil
}

func (dir *KeyDir) Get(key string) (*KeyEntry, bool, error) {
	if dir == nil {
		return nil, false, ErrInvalid
	}

	entry, ok := dir.map_[key]

	if ok {
		return &entry, ok, nil
	}

	//when we has no key in map, we do not return an error
	return nil, false, nil
}

// func (dir *KeyDir) Delete(key string) error {
// 	if dir == nil {
// 		return ErrInvalid
// 	}

// 	delete(dir.map_, key)

// 	return nil
// }

// func (dir *KeyDir) GetMap() map[string]KeyEntry {
// 	return dir.map_
// }

// func (dir *KeyDir) DebugShow() {
// 	for _, entry := range dir.map_ {
// 		fmt.Printf("%v\n", entry)
// 	}
// }
