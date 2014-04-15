package bitcask

import (
	"errors"
)

type KeyEntry struct {
	Key string
	Total_size uint32
	Offset     uint32
	Tstamp     int32
	Ver int32
}

type KeyDir struct {
	entrys []KeyEntry
}

func NewKeyDir() (*KeyDir) {
	var entrys []KeyEntry
	return &KeyDir{entrys}
}

func (dir *KeyDir) Set(key string, total_sz, offset uint32, tstamp, ver int32) error {
	if dir == nil {
		return ErrInvalid
	}

	entry := KeyEntry{key, total_sz, offset, tstamp, ver}
	dir.entrys = append(dir.entrys, entry)
	return nil
}

func (dir *KeyDir) Get(key string) (*KeyEntry, error) {
	if dir == nil {
		return nil, ErrInvalid
	}

	sz := len(dir.entrys)
	for i := 0; i < sz; i++ {
		if key == dir.entrys[i].Key {
			return &(dir.entrys[i]), nil
		}
	}
	return nil, errors.New("not exists.")
}
