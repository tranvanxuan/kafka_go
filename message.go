package main

import (
	"bufio"
	"fmt"
)

const (
	ECHO = 1
	PREG = 2
	// Response
	rECHO = 101
	rPREG = 102
)

type Message struct {
	ECHO *string
	// producer register
	PREG *string
	// Response
	rECHO *string
	rPREG *byte
}

// Message format:
// - stream[0]: size
// -stream[1]: type
// -stream[2..]: message
func readFromStream(streamRw *bufio.ReadWriter) ([]byte, error) {
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

func parseMessage(streamMessage []byte) *Message {
	switch streamMessage[0] {
	case ECHO:
		var st = string(streamMessage[1:])
		return &Message{ECHO: &st}
	case rECHO:
		var st = string(streamMessage[1:])
		return &Message{rECHO: &st}
	case PREG:
		var st = string(streamMessage[1:])
		return &Message{PREG: &st}
	case rPREG:
		var st = streamMessage[1]
		return &Message{rPREG: &st}
	default:
		return nil
	}
}

func readMessageFromStream(streamRw *bufio.ReadWriter) (*Message, error) {
	data, err := readFromStream(streamRw)
	if err != nil {
		return nil, err
	}
	return parseMessage(data), nil
}

func writeDataTOStreamWithType(streamRw *bufio.ReadWriter, mtype byte, data string) error {
	var err error
	// Write length
	err = streamRw.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}

	// Write type
	err = streamRw.WriteByte(mtype)
	if err != nil {
		return err
	}
	// Write data
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

// [7	1	h e l l o o]
func writeMessageToStream(streamRw *bufio.ReadWriter, message Message) error {
	if message.ECHO != nil {
		if err := writeDataTOStreamWithType(streamRw, ECHO, *message.ECHO); err != nil {
			return err
		}
	} else if message.rECHO != nil {
		if err := writeDataTOStreamWithType(streamRw, rECHO, *message.rECHO); err != nil {
			return err
		}
	}
	if message.PREG != nil {
		if err := writeDataTOStreamWithType(streamRw, PREG, *message.PREG); err != nil {
			return err
		}
	}
	if message.rPREG != nil {
		data := fmt.Sprintf("%d", *message.rPREG)
		if err := writeDataTOStreamWithType(streamRw, rPREG, data); err != nil {
			return err
		}
	}
	return nil
}
