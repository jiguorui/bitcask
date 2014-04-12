package bitcask

import (
	//"bufio"
	"errors"
	//"fmt"
	"os"
)

type Bucket struct {
	path    string
	file_id int
	file    *os.File
}

// Bucket is a file to archive records.
// |record|record| ... |record|
func NewBucket(path string, id int) (*Bucket, error) {

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, errors.New("File is not exist.")
	}

	f, err := os.OpenFile(path, os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &Bucket{path, id, f}, nil
}

// Write a record to file
func (bk *Bucket) WriteRecord(r *Record) error {
	_, err := bk.file.Write(r.Encode())
	if err != nil {
		return err
	}
	//if n != r
	return nil
}

// Close file
func (bk *Bucket) Close() {
	bk.file.Close()
}
