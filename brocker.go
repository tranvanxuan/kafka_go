package main

import (
	"bufio"
	"fmt"
	"net"
)

const BrokerPort = 10000
const (
	ECHO = 1
	// Other message types
)

type Message struct {
	ECHO *string
	// Other type here...
	A *uint8
	B *string
}

func readFromStream(streamRw bufio.ReadWriter) ([]byte, error) {
	var err error
	// Read
	header, err := streamRw.ReadByte()
	if err != nil {
		return nil, err
	}

	// where is data [] byte
	//
	data, err := streamRw.Peek(int(header))
	if err != nil {
		return nil, err
	}

	_, err = streamRw.Discard(int(header))
	if err != nil {
		return nil, err
	}

	return data, err

}

func writeToStream(streamRw bufio.ReadWriter, data string) error {
	var err error
	// Write
	err = streamRw.WriteByte(byte(len(data)))
	if err != nil {
		return nil
	}

	_, err = streamRw.WriteString(data)
	if err != nil {
		return nil
	}

	err = streamRw.Flush()
	if err != nil {
		return nil
	}
	return nil
}

type Broker struct {
}

func (b *Broker) startBrokerServer() error {
	ln, _ := net.Listen("tcp", fmt.Sprintf(":%d", BrokerPort))
	for {
		conn, _ := ln.Accept() // Block until connection is accepted
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		// read
		data, err := readFromStream(*streamRw)
		if err != nil {
			return err
		}

		// Process
		parseMessage := b.parseBrokerMessage(data)
		if parseMessage != nil {
			resp, err := b.processBrokerMessage(parseMessage)
			if err != nil {
				return err
			}

			// write to back
			err = writeToStream(*streamRw, resp)
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

func (b *Broker) parseBrokerMessage(message []byte) *Message {
	switch message[0] {
	case ECHO:
		var st = string(message[1:])
		return &Message{ECHO: &st}
	default:
		return nil
	}
}

func (b *Broker) processBrokerMessage(message *Message) (string, error) {
	var err error
	var resp string

	if message.ECHO != nil {
		resp = fmt.Sprintf("I have receiver: %s", *message.ECHO)
	}
	return resp, err
}
