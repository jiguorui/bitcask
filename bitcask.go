package bitcask

import (
	"strings"
	"fmt"
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

	return &Bitcask{bk, keydir}, nil
}

// Store a key/value in a Bitcask datastore.
func (bc *Bitcask) Set(key string, value []byte) (int32, error) {
	r, total_sz, err := MakeRecord(key, value)
	if err != nil {
		return 0, err
	}
	offset, _ := bc.bucket.GetWriteOffset()
	bc.keydir.Set(key, uint32(total_sz), uint32(offset), int32(0), int32(0))
	return bc.bucket.Write(r.GetBuf())
}

// Get value by key
func (bc *Bitcask) Get(key string) ([]byte, error) {
	entry, err := bc.keydir.Get(key)
	if err != nil {
		return []byte(""), err
	}
	fmt.Printf("%d,%d\n", entry.Total_size, entry.Offset)
	return bc.bucket.Read(entry.Total_size, entry.Offset)
}

// Close a Bitcask
func (bc *Bitcask) Close() {
	bc.bucket.Close()
}
