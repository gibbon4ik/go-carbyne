package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"
)

var (
	workers    int
	count      int
	delay      int
	remoteip   string
	localip    string
	prefix     string
	remoteaddr *net.UDPAddr
	localaddr  *net.UDPAddr
	wg         sync.WaitGroup
)

func main() {
	var err error

	flag.IntVar(&workers, "w", 1, "Number of workers")
	flag.IntVar(&count, "c", 1000, "Number of metrics sent by each worker")
	flag.IntVar(&delay, "d", 0, "Time in us between packets")
	flag.StringVar(&remoteip, "r", "127.0.0.1:2023", "Send packets on host:port")
	flag.StringVar(&localip, "l", "", "Send packets from host:port")
	flag.StringVar(&prefix, "prefix", "test", "Metrics prefix")
	flag.Parse()

	if remoteip == "" {
		panic("No remote ip")
	}

	remoteaddr, err = net.ResolveUDPAddr("udp", remoteip)
	if err != nil {
		panic(err)
	}

	if localip != "" {
		localaddr, err = net.ResolveUDPAddr("udp", localip)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go sender(&wg)
	}
	wg.Wait()
}

func sender(wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := net.DialUDP("udp", localaddr, remoteaddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	val := math.Floor(rand.Float64()*100.0) + 10.0
	tm := time.Now().Unix()
	m1 := []byte(fmt.Sprintf("%s %g %d\n%s %g %d\n", prefix+".avg.aaa", val, tm, prefix+".count.total", 1.0, tm))
	for i := 0; i < count; i++ {
		_, err := conn.Write(m1)
		if err != nil {
			fmt.Println(err)
		}
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Microsecond)
		}
	}
	if delay < 10 {
		time.Sleep(time.Second)
	}
}
