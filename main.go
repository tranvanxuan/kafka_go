package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	// startServer()
	fmt.Println(os.Args)
	if os.Args[1] == "server" {
		spawnServer()
	} else {
		clientConnect(os.Args[2])
	}
}

// initialize server in goroutine, so we can run multiple server in different port
func spawnServer() {
	var wg sync.WaitGroup
	wg.Go(func() {
		startServer("10001")
	})
	wg.Go(func() {
		startServer("10002")
	})
	wg.Wait()
}

func startServer(port string) {
	ln, _ := net.Listen("tcp", fmt.Sprintf(":%s", port))
	conn, _ := ln.Accept() // Block until connection is accepted
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		// get header de biet length of data
		header, _ := streamRw.ReadByte()
		// get header byte to know the length of data, then peek data from buffer
		data, _ := streamRw.Peek(int(header))
		fmt.Printf("Data from client: %s\n", data)

		if strings.TrimSpace(string(data)) == "bye" {
			break
		}

		streamRw.Discard(int(header))
		time.Sleep(2000 * time.Millisecond)

		// Write to client
		newData := fmt.Sprintf("Received from client: %s", string(data))
		streamRw.WriteByte(byte(len(newData)))
		streamRw.WriteString(newData)
		streamRw.Flush()
	}

	conn.Close()
}

func clientConnect(port string) {
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%s", port))

	for {
		// read from stdin
		rd := bufio.NewReader(os.Stdin)
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		fmt.Printf("Send to server: %s\n", line)

		// Write to server
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		// Add header to data // [0 1 2 3 4 ... 1024]
		streamRw.WriteByte(byte(len(line)))
		// Write data to buffer
		streamRw.WriteString(line)
		// sent to network
		streamRw.Flush()

		// if input is bye, break loop
		if strings.TrimSpace(line) == "bye" {
			break
		}

		// Read
		header, _ := streamRw.ReadByte()
		data, _ := streamRw.Peek(int(header))
		fmt.Printf("Data from server: %s\n", data)
		streamRw.Discard(int(header))
	}

	conn.Close()
}
