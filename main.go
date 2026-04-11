package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	// startServer()
	fmt.Println(os.Args)
	if os.Args[1] == "server" {
		var broker = Broker{}
		err := broker.startBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	} else {
		clientConnectTCPAndEcho(10000)
	}
}

func writeEchoToStream(streamRw *bufio.ReadWriter, data string) error {
	var err error
	err = streamRw.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	err = streamRw.WriteByte(ECHO)
	if err != nil {
		return err
	}
	_, err = streamRw.WriteString(data)
	if err != nil {
		return err
	}
	err = streamRw.Flush()
	if err != nil {
		return err
	}
	return nil
}

func clientConnectTCPAndEcho(port int) {
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", port))
	fmt.Printf("Connected to server at port %v\n", port)
	// Read input from stdin and write to stream.
	rd := bufio.NewReader(os.Stdin)
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	line, err := rd.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return
		} else {
			// Probably panic here
		}
	}
	fmt.Printf("Sent to server: %s\n", line)
	writeEchoToStream(streamRw, strings.Trim(line, "\n"))

	// Try to read back from the stream
	header, err := streamRw.ReadByte()
	if header == 0 || err != nil {
		return
	}
	data, _ := streamRw.Peek(int(header)) // Read exactly n bytes
	fmt.Printf("Receive message from server: %s\n", data)
	streamRw.Discard(int(header)) // Throw n bytes away
}
