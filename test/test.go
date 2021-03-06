package main

import (
	"fmt"
	"github.com/jiguorui/bitcask"
)

func test_file() {
	f, err := bitcask.OpenFile("002.data", 1)
	if err != nil {
		fmt.Println(err)
	}

	defer f.Close()
	offset, size, err := f.Write("abc", []byte("defghi"), 1)
	//b, _ := f.Get("abc")
	f.Unwrite()
	fmt.Println(err)
	k, v, err := f.Read(offset, size)
	fmt.Printf("%s, %s, %v\n", k, v, err)
	//bucket.Merge("001.ar.data")
}

// func test_keydir() {
// 	kd := bitcask.NewKeyDir()
// 	kd.Set("abc", uint32(16), uint32(0), int32(0), int32(0))

// 	e, _, _:= kd.Get("abc")
// 	fmt.Printf("%d\n", e.Total_size)

// }

func test_time() {
	fmt.Printf("time stamp, %d\n", bitcask.Tstamp())
}

 func test_bitcask() {
 	bc, err := bitcask.Open(".")
 	if err != nil {
 		fmt.Println(err)
 		return
 	}
 	defer bc.Close()

 	bc.Put("abcde", []byte("fghijklmnop"))
 	v, e := bc.Get("abcde")
 	fmt.Printf("%s,%v\n", v, e)

//  	for i := 0; i < 1000; i++ {
// 		s := fmt.Sprintf("key:%d", i)
// 		err := bc.Delete(s)//, []byte("dkjkjksfjkk"))
// 		if err != nil {
// 			fmt.Println(err)
// 			continue
// 		}
// 		//fmt.Printf("%v\n", b)
// }
// 		// if i % 2 == 0 {
// 		//err = bc.Delete(s)
// 		 if err != nil {
// 		 	fmt.Println(err)
// 		 	continue
// 		 }

// 		// }

// 		b, err := bc.Get(s)
// 		if err != nil {
// 			fmt.Println(err)
// 			continue
// 		}
// 		if len(b) > 24 {
// 			//fmt.Printf("%s,%d\n", bitcask.StringForTest(b), bitcask.GetVersion(b))
// 		}
// 	}
// }
}
func main() {
	//test_bucket()
	//test_keydir()
	//test_bitcask()
	//test_file()
	test_time()
	test_bitcask()
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

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// package main

// import (
// 	"log"
// 	"net/http"
// 	"time"
// )

// const (
// 	numPollers     = 2                // number of Poller goroutines to launch
// 	pollInterval   = 60 * time.Second // how often to poll each URL
// 	statusInterval = 10 * time.Second // how often to log status to stdout
// 	errTimeout     = 10 * time.Second // back-off timeout on error
// )

// var urls = []string{
// 	"http://www.google.com/",
// 	"http://golang.org/",
// 	"http://blog.golang.org/",
// }

// // State represents the last-known state of a URL.
// type State struct {
// 	url    string
// 	status string
// }

// // StateMonitor maintains a map that stores the state of the URLs being
// // polled, and prints the current state every updateInterval nanoseconds.
// // It returns a chan State to which resource state should be sent.
// func StateMonitor(updateInterval time.Duration) chan<- State {
// 	updates := make(chan State)
// 	urlStatus := make(map[string]string)
// 	ticker := time.NewTicker(updateInterval)
// 	go func() {
// 		for {
// 			select {
// 			case <-ticker.C:
// 				logState(urlStatus)
// 			case s := <-updates:
// 				urlStatus[s.url] = s.status
// 			}
// 		}
// 	}()
// 	return updates
// }

// // logState prints a state map.
// func logState(s map[string]string) {
// 	log.Println("Current state:")
// 	for k, v := range s {
// 		log.Printf(" %s %s", k, v)
// 	}
// }

// // Resource represents an HTTP URL to be polled by this program.
// type Resource struct {
// 	url      string
// 	errCount int
// }

// // Poll executes an HTTP HEAD request for url
// // and returns the HTTP status string or an error string.
// func (r *Resource) Poll() string {
// 	resp, err := http.Head(r.url)
// 	if err != nil {
// 		log.Println("Error", r.url, err)
// 		r.errCount++
// 		return err.Error()
// 	}
// 	r.errCount = 0
// 	return resp.Status
// }

// // Sleep sleeps for an appropriate interval (dependent on error state)
// // before sending the Resource to done.
// func (r *Resource) Sleep(done chan<- *Resource) {
// 	time.Sleep(pollInterval + errTimeout*time.Duration(r.errCount))
// 	done <- r
// }

// func Poller(in <-chan *Resource, out chan<- *Resource, status chan<- State) {
// 	for r := range in {
// 		s := r.Poll()
// 		status <- State{r.url, s}
// 		out <- r
// 	}
// }

// func main() {
// 	// Create our input and output channels.
// 	pending, complete := make(chan *Resource), make(chan *Resource)

// 	// Launch the StateMonitor.
// 	status := StateMonitor(statusInterval)

// 	// Launch some Poller goroutines.
// 	for i := 0; i < numPollers; i++ {
// 		go Poller(pending, complete, status)
// 	}

// 	// Send some Resources to the pending queue.
// 	go func() {
// 		for _, url := range urls {
// 			pending <- &Resource{url: url}
// 		}
// 	}()

// 	for r := range complete {
// 		go r.Sleep(pending)
// 	}
// }

// package main

// import (
// 	"fmt"
// 	"time"
// )

// func run1() {
// 	for i := 0; i < 10; i++ {
// 		time.Sleep(time.Second)
// 		fmt.Printf("run1 %d\n", i)
// 	}
// }

// func run2() {
// 	for i := 0; i < 15; i++ {
// 		time.Sleep(time.Second)
// 		fmt.Printf("run2 %d\n", i)
// 	}
// }

// func main() {
// 	c := make(chan int, 2)
// 	go func() {
// 		run1()
// 		c <- 1
// 	}()
// 	go func() {
// 		run2()
// 		c <- 1
// 	}()
// 	<-c
// 	<-c
// }
