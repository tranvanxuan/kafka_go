package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

const BrokerPort = 10000

type Broker struct {
}

func (b *Broker) startBrokerServer() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", BrokerPort))
	if err != nil {
		panic(err)
	}
	for {
		conn, _ := ln.Accept() // Block until can
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		var err error
		parseMessage, err := readMessageFromStream(streamRw)
		if err != nil {
			return err
		}

		// Process
		if parseMessage != nil {
			resp, err := b.processBrokerMessage(parseMessage)
			if err != nil {
				return err
			}

			// Write it back
			err = writeMessageToStream(streamRw, *resp)
			if err != nil {
				return err
			}
		}

		err = conn.Close()
		if err != nil {
			return err
		}
	}
}

// Process:
// - Call inner process function for each message type
// - Response correct Message
func (b *Broker) processBrokerMessage(message *Message) (*Message, error) {
	if message.ECHO != nil {
		resp, err := b.processEchoMessage(message.ECHO)
		if err != nil {
			return nil, err
		}
		return &Message{rECHO: &resp}, nil
	}
	if message.PREG != nil {
		resp, err := b.processProducerRegisterMessage(message.PREG)
		if err != nil {
			return nil, err
		}
		return &Message{rPREG: resp}, nil
	}
	return nil, nil
}

func (b *Broker) processEchoMessage(echoMessage *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *echoMessage), nil
}

func (b *Broker) processProducerRegisterMessage(pREGMessage *string) (*byte, error) {
	port, err := strconv.ParseInt(*pREGMessage, 10, 32)
	if err != nil {
		return nil, err
	}
	go func() {
		conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", port))
		fmt.Printf("Connected to server at port %v\n", port)
		// Read input from stdin and write to stream.
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			parsedMessage, err := readMessageFromStream(streamRw)

			if parsedMessage == nil || err != nil {
				panic(err)
			}
			// Process something here
			resp, err := b.processBrokerMessage(parsedMessage)
			if err != nil {
				panic(err)
			}
			err = writeMessageToStream(streamRw, *resp)
			if err != nil {
				panic(err)
			}
		}
	}()
	var resp byte = 0
	return &resp, err
}
