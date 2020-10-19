package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type metricval struct {
	value float64
	count uint32
	tm    int64
}

type metrics map[string]metricval

const (
	maxworkers = 64
	maxmtu     = 1462
)

var (
	Version    = "0.1"
	Service    = "go-carbyn"
	workers    int
	interval   int
	listen     string
	remote     string
	remoteaddr *net.UDPAddr
	localaddr  *net.UDPAddr
	sumchannel = make(chan metrics, maxworkers)
	err        error
	run        bool = true
)

func main() {
	flag.IntVar(&workers, "w", 1, "Number of workers")
	flag.StringVar(&listen, "l", ":2023", "Listen on host:port")
	flag.StringVar(&remote, "r", "", "Send udp to host:port")
	flag.IntVar(&interval, "i", 60, "Interval is seconds between aggregate data dump")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()

	if listen == "" {
		panic("Listen address not set!")
	}

	localaddr, err = net.ResolveUDPAddr("udp", listen)
	if err != nil {
		panic(err)
	}
	if workers < 1 || workers > maxworkers {
		panic("Bad workers number")
	}

	if remote == "" {
		panic("Remote address not set!")
	}

	remoteaddr, err = net.ResolveUDPAddr("udp", remote)
	if err != nil {
		panic(err)
	}

	//runtime.GOMAXPROCS(workers + 1)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		run = false
	}()

	fmt.Fprintln(os.Stderr, "Start", Service, Version)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go listener(listen, &wg, i)
	}
	wg.Add(1)
	go aggregator(&wg)
	wg.Wait()
	fmt.Fprintln(os.Stderr, "Stop", Service, Version)
}

func setsocketoptions(network string, address string, c syscall.RawConn) error {

	var fn = func(s uintptr) {
		setErr := syscall.SetsockoptInt(int(s), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		if setErr != nil {
			fmt.Fprintf(os.Stderr, "setsockopt: %s", setErr)
		}
		_, getErr := syscall.GetsockoptInt(int(s), syscall.SOL_SOCKET, unix.SO_REUSEPORT)
		if getErr != nil {
			fmt.Fprintf(os.Stderr, "getsockopt: %s", getErr)
		}
		//fmt.Printf("value of SO_REUSEPORT option is: %d\n", int(val))
	}
	if err := c.Control(fn); err != nil {
		return err
	}
	return nil
}

func addpoint(hash metrics, key string, value float64, tm int64, count uint32) {
	if val, ok := hash[key]; ok {
		val.value += value
		val.tm += tm
		val.count += count
		hash[key] = val
	} else {
		hash[key] = metricval{value: value, count: count, tm: tm}
	}
}

func listener(listen string, wg *sync.WaitGroup, num int) {
	defer wg.Done()
	listenconfig := &net.ListenConfig{Control: setsocketoptions}
	conn, err := listenconfig.ListenPacket(context.Background(), "udp", listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Listen error", err)
		return
	}
	defer conn.Close()
	conn.(*net.UDPConn).SetReadBuffer(1024 * 1024)
	buffer := make([]byte, 1500)
	metrhash := make(metrics)
	nextdump := time.Now().Add(time.Second).Unix()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	for run {
		// sent metrics to aggregator every second
		if time.Now().Unix() >= nextdump {
			conn.SetReadDeadline(time.Now().Add(time.Second))
			nextdump = time.Now().Add(time.Second).Unix()
			if lastlen := len(metrhash); lastlen > 0 {
				sumchannel <- metrhash
				metrhash = make(metrics, lastlen)
			}
		}

		n, _, err := conn.ReadFrom(buffer)
		if err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				fmt.Fprintf(os.Stderr, "udp read error %s", err)
			}
			continue
		}

		for _, s := range bytes.Split(buffer[0:n], []byte("\n")) {
			if len(s) == 0 {
				continue
			}
			list := bytes.SplitN(s, []byte(" "), 3)
			if len(list) < 3 {
				continue
			}
			key := string(list[0])
			value, err := strconv.ParseFloat(string(list[1]), 64)
			if err != nil {
				continue
			}
			tm, err := strconv.Atoi(string(list[2]))
			if err != nil {
				continue
			}
			addpoint(metrhash, key, value, int64(tm), 1)
		}
	}
	sumchannel <- metrhash
}

func aggregator(wg *sync.WaitGroup) {
	allmetrics := make(metrics)
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	defer wg.Done()
	for run {
		select {
		case hash := <-sumchannel:
			for key, v := range hash {
				addpoint(allmetrics, key, v.value, v.tm, v.count)
			}
		case <-ticker.C:
			go dumper(allmetrics)
			lastlen := len(allmetrics)
			allmetrics = make(metrics, lastlen)
		}
	}
	ticker.Stop()
	dumper(allmetrics)
}

func dumper(mhash metrics) {
	conn, err := net.DialUDP("udp", nil, remoteaddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buffer := make([]byte, 0, 1500)
	allhash := make(metrics)
	var s string
	for key, v := range mhash {
		if strings.Contains(key, ".count.") {
			s = fmt.Sprintln(key, v.value, v.tm/int64(v.count))
			//fmt.Println(key, v.value, v.tm/int64(v.count))
			if pos := strings.LastIndex(key, "."); pos >= 0 {
				addpoint(allhash, key[0:pos+1]+"all", v.value, v.tm, v.count)
			}
		} else {
			s = fmt.Sprintln(key, v.value/float64(v.count), v.tm/int64(v.count))
			//fmt.Println(key, v.value/float64(v.count), v.tm/int64(v.count))
		}
		if len(buffer)+len(s) > maxmtu {
			conn.Write(buffer)
			buffer = buffer[:0]
		}
		buffer = append(buffer, s...)
	}
	for key, v := range allhash {
		//fmt.Println(key, v.value, v.tm/int64(v.count))
		s = fmt.Sprintln(key, v.value, v.tm/int64(v.count))
		if len(buffer)+len(s) > maxmtu {
			conn.Write(buffer)
			buffer = buffer[:0]
		}
		buffer = append(buffer, s...)
	}
	if len(buffer) > 0 {
		conn.Write(buffer)
	}
}
