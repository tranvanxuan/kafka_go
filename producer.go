package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Producer struct {
	port    uint16
	topicID uint16
}

// Connect to Broker to send register
func (p *Producer) sendPortDataToBroker() error {
	var err error
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", BrokerPort))
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	pRegMsg := ProducerRegisterMessage{
		port:    p.port,
		topicID: p.topicID,
	}
	fmt.Printf("pRegMsg: port=%d, topicID=%d\n", pRegMsg.port, pRegMsg.topicID)
	message := Message{
		PREG: &pRegMsg,
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

func (p *Producer) startProducerServer(port int16) error {
	var err error

	err = p.sendPortDataToBroker()
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

		// write ProducerConsumeMessage
		message := Message{ProducerConsumeMessage: []byte(line)}
		err = writeMessageToStream(streamRw, message)
		if err != nil {
			break
		}

		// Try to read back from the stream
		resp, err := readMessageFromStream(streamRw)
		if err != nil {
			break
		}

		fmt.Printf("Receive ProducerConsumeMessage from broker: %d\n", *resp.rProducerConsumeMessage)
	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
