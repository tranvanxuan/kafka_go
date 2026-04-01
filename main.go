package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	// startServer()
	fmt.Println(os.Args)
	if os.Args[1] == "server" {
		startServer()
	} else {
		clientConnect()
	}
}

func startServer() {
	ln, _ := net.Listen("tcp", ":1234")
	conn, _ := ln.Accept() // Block until connection is accepted
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// get header de biet length of data
	header, _ := streamRw.ReadByte()

	// get header byte to know the length of data, then peek data from buffer
	data, _ := streamRw.Peek(int(header))
	fmt.Printf("Data from client: %s\n", data)
	streamRw.Discard(int(header))

	time.Sleep(2000 * time.Millisecond)

	// Write to client
	streamRw.WriteByte(byte(len(data)))
	streamRw.WriteString(string(data))
	streamRw.Flush()

	conn.Close()
}

func clientConnect() {
	conn, _ := net.Dial("tcp", ":1234")
	// read from stdin
	rd := bufio.NewReader(os.Stdin)
	line, err := rd.ReadString('\n')
	if err != nil {
		return
	}
	fmt.Printf("Send to server: %s\n", line)

	// Write to server
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	// Add header to data
	streamRw.WriteByte(byte(len(line)))
	// Write data to buffer
	streamRw.WriteString(line)
	// sent to network
	streamRw.Flush()

	conn.Close()
}
