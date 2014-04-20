package main

import (
	"fmt"
	"github.com/jiguorui/bitcask"
)

func test_bucket() {
	bucket, err := bitcask.NewBucket("001.ar", 1)
	if err != nil {
		fmt.Println(err)
	}

	defer bucket.Close()

	bucket.Merge("001.ar.data")
}

func test_keydir() {
	kd := bitcask.NewKeyDir()
	kd.Set("abc", uint32(16), uint32(0), int32(0), int32(0))

	e, _, _:= kd.Get("abc")
	fmt.Printf("%d\n", e.Total_size)

}

func test_bitcask() {
	bc, err := bitcask.Open(".")
	if err != nil {
		fmt.Println(err)
		return 
	}
	defer bc.Close()

	for i := 0; i < 100; i++ {
		s := fmt.Sprintf("key:%d", i)
		_, err = bc.Put(s, []byte("Hello, world."))
		if err != nil {
			fmt.Println(err)
			continue
		}

		// if i % 2 == 0 {
		//err = bc.Delete(s)
		 if err != nil {
		 	fmt.Println(err)
		 	continue
		 }
			
		// }
	
		b, err := bc.Get(s)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if len(b) > 24 {
			//fmt.Printf("%s,%d\n", bitcask.StringForTest(b), bitcask.GetVersion(b))
		}
	}
}

func main() {
	//test_bucket()
	//test_keydir()
	//test_bitcask()
	test_bucket()
	//a := make(map[string]string,100)
	//a["abc"] = "ddd"
	//fmt.Printf("%s\n", a["abc"])
	//fmt.Printf("%s\n", a["abcd"])
	// fmt.Printf("Hello, bitcast\n")
	// bc, err := bitcask.Open(".")
	// if err != nil {
	// 	fmt.Println(err)
	// 	return 
	// }
	// defer bc.Close()

	// bc.Put("abc", []byte("defghijklmnopqrstuvwxyz"))

	// fmt.Printf("%d\n", bitcask)
}
