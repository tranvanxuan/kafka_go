package main

import (
	"bufio"
	"fmt"
	"net"
)

const BrokerPort = 10000

type Broker struct {
	topics []Topic
}

func (b *Broker) init() {
	b.topics = make([]Topic, 0)
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

func (b *Broker) processProducerRegisterMessage(pREGMessage *ProducerRegisterMessage) (*byte, error) {
	fmt.Printf("p = %v\n", pREGMessage)
	topicID := -1

	for idx, tp := range b.topics {
		if tp.topicID == pREGMessage.topicID {
			topicID = idx
			break
		}
	}

	if topicID == -1 {
		tp := Topic{}
		tp.init(pREGMessage.topicID)
		b.topics = append(b.topics, tp)
		topicID = len(b.topics) - 1
	}

	go func() {
		conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", pREGMessage.port))
		fmt.Printf("Connected to server at port %v\n", pREGMessage.port)
		// Read input from stdin and write to stream.
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			parsedMessage, err := readMessageFromStream(streamRw)

			if parsedMessage == nil || err != nil {
				panic(err)
			}
			if parsedMessage.ProducerConsumeMessage != nil {
				resp, err := b.ProcessProducerConsumeMessage(parsedMessage.ProducerConsumeMessage, topicID)
				if err != nil {
					panic(err)
				}
				err = writeMessageToStream(streamRw, Message{rProducerConsumeMessage: &resp})
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	var resp byte = 0
	return &resp, nil
}

func (b *Broker) ProcessProducerConsumeMessage(pcm []byte, topicID int) (byte, error) {
	b.topics[topicID].mq.push(pcm)
	b.topics[topicID].mq.debug()
	return 0, nil
}
