package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Producer struct {
}

// Connect to Broker to send register
func (b *Producer) sendPortDataToBroker(port int16) error {
	var err error
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", BrokerPort))
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	portStr := fmt.Sprintf("%d", port)
	message := Message{
		PREG: &portStr,
	}
	err = writeMessageToStream(streamRw, message)
	if err != nil {
		panic(err)
	}

	// Try to read back from the stream
	resp, err := readMessageFromStream(streamRw)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Receive response from broker: %v\n", *resp.rPREG)
	return nil
}

func (b *Producer) startProducerServer(port int16) error {
	var err error

	err = b.sendPortDataToBroker(port)
	if err != nil {
		panic(err)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}

	conn, _ := ln.Accept() // Block until connection is accepted
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	rd := bufio.NewReader(os.Stdin)

	for {

		// read from stdin
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				// Probably panic here
			}
		}

		// write ECHO
		message := Message{ECHO: &line}
		err = writeMessageToStream(streamRw, message)
		if err != nil {
			break
		}

		// Try to read back from the stream
		resp, err := readMessageFromStream(streamRw)
		if err != nil {
			break
		}

		fmt.Printf("Receive message from broker: %s\n", *resp.rECHO)
	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
