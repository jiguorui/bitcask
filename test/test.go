package main

import (
	"fmt"
	"github.com/jiguorui/bitcask"
)

func main() {
	fmt.Printf("Hello, bitcast\n")
	bucket, err := bitcask.NewBucket("data001.arc", 1)
	if err != nil {
		fmt.Println(err)
		return 
	}
	defer bucket.Close()
	r := bitcask.NewRecord("abcdef", []byte("ghijklmnopqrstuvwxyz"), 0x02, 1)
	//fmt.Printf("%v\n", r.Encode())
	for i := 0; i < 100; i++ {
		bucket.WriteRecord(r)
	}
	//bucket.WriteRecord(r)
	//bucket.WriteRecord(r)
	fmt.Printf("%d\n", bucket)
}
