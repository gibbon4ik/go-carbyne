package main

import (
	"flag"
	"fmt"
	"net"
	"os"
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
	metrics    []byte
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

	tail := flag.Args()
	if len(tail) == 0 {
		panic("usage: " + os.Args[0] + " [-options] key value [key value ...]")
	}
	if len(tail)%2 != 0 {
		panic("uneven arguments number")
	}

	tm := time.Now().Unix()
	for i := 0; i < len(tail); i += 2 {
		s := fmt.Sprintf("%s %s %d\n", tail[i], tail[i+1], tm)
		metrics = append(metrics, s...)
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

	for i := 0; i < count; i++ {
		_, err := conn.Write(metrics)
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
