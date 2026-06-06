package main

import (
	"bufio"
	"fmt"
)

const (
	ECHO                   = 1
	PREG                   = 2
	ProducerConsumeMessage = 3
	// Response
	rECHO                   = 101
	rPREG                   = 102
	rProducerConsumeMessage = 103
)

type Message struct {
	ECHO *string
	// producer register
	PREG                   *ProducerRegisterMessage
	ProducerConsumeMessage []byte // nil able
	// Response
	rECHO                   *string
	rPREG                   *byte
	rProducerConsumeMessage *byte
}

type ProducerRegisterMessage struct {
	port    uint16
	topicID uint16
}

func (m *ProducerRegisterMessage) fromByte(streamMessage []byte) {
	// first 2 bytes: port
	// next 2 byte: topicID
	// 10000 -> 0 255
	// data[4, 76, ...]
	m.port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1]) // uint16(4) << 8 + uint(76) = 1024 + 76 = 1100
	m.topicID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
}

func (m *ProducerRegisterMessage) toByte() []byte {
	var data [4]byte
	// first 2 bytes: port
	// next 2 byte: topicID
	// example for 1100
	data[0] = byte(m.port >> 8)  // byte(1100 >> 8) = 4
	data[1] = byte(m.port % 255) // byte(1100) = 76
	data[2] = byte(m.topicID >> 8)
	data[3] = byte(m.topicID % 255)
	// data[4, 76, ...]
	return data[:]
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
		p := ProducerRegisterMessage{}
		p.fromByte(streamMessage[1:])
		return &Message{PREG: &p}
	case rPREG:
		var st = streamMessage[1]
		return &Message{rPREG: &st}
	case ProducerConsumeMessage:
		return &Message{ProducerConsumeMessage: streamMessage[1:]}
	case rProducerConsumeMessage:
		var st = streamMessage[1]
		return &Message{rProducerConsumeMessage: &st}
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
		data := string(message.PREG.toByte())
		if err := writeDataTOStreamWithType(streamRw, PREG, data); err != nil {
			return err
		}
	}
	if message.rPREG != nil {
		data := fmt.Sprintf("%d", *message.rPREG)
		if err := writeDataTOStreamWithType(streamRw, rPREG, data); err != nil {
			return err
		}
	}
	if message.ProducerConsumeMessage != nil {
		if err := writeDataTOStreamWithType(streamRw, ProducerConsumeMessage, string(message.ProducerConsumeMessage)); err != nil {
			return err
		}
	}
	if message.rProducerConsumeMessage != nil {
		data := fmt.Sprintf("%d", *message.rProducerConsumeMessage)
		if err := writeDataTOStreamWithType(streamRw, rProducerConsumeMessage, data); err != nil {
			return err
		}
	}
	return nil
}
