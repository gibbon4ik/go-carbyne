package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	workers    int
	count      int
	delay      int
	remoteip   string
	localip    string
	filename   string
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
	flag.StringVar(&filename, "f", "", "File name with metrics")
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

	if filename != "" {
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go replay(&wg, filename)
		}
		wg.Wait()
		os.Exit(0)
	}

	tail := flag.Args()
	if len(tail) == 0 {
		panic("usage: " + os.Args[0] + " [-options] [key value ...]")
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

	if delay > 0 {
		i := count
		for _ = range time.Tick(time.Microsecond * time.Duration(delay)) {
			_, err := conn.Write(metrics)
			if err != nil {
				fmt.Println(err)
			}
			i--
			if i == 0 {
				break
			}
		}
	} else {
		for i := 0; i < count; i++ {
			_, err := conn.Write(metrics)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	if delay < 10 {
		time.Sleep(time.Second)
	}
}

func replay(wg *sync.WaitGroup, filename string) {
	defer wg.Done()

	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	conn, err := net.DialUDP("udp", localaddr, remoteaddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	sc := bufio.NewScanner(f)
	buf := ""
	buflen := 0
	strl := 10
	for sc.Scan() {
		tm := strconv.FormatInt(time.Now().Unix(), 10)

		line := sc.Text() // GET the line string
		llen := len(line)
		line = line[0:llen-strl] + tm + "\n"
		llen++
		if buflen+llen > 1470 {
			_, err := conn.Write([]byte(buf))
			if err != nil {
				fmt.Println(err)
			}
			if delay > 0 {
				time.Sleep(time.Duration(delay) * time.Microsecond)
			}
			buf = line
			buflen = llen
			continue
		}
		buf += line
		buflen += llen
	}

}
