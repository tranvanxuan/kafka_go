package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

const BrokerPort = 10000

type Broker struct {
	topics   []*Topic
	metaFile *os.File
}

func (b *Broker) init() {
	var err error
	b.metaFile, err = os.OpenFile("broker_metadata.dat", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	size := int(readu32(b.metaFile, 0))
	b.topics = make([]*Topic, 0, size)
	for i := 0; i < size; i += 1 {
		topicID := readu16(b.metaFile, int64(4+i*2))
		tp := &Topic{}
		tp.init(topicID)
		b.topics = append(b.topics, tp)
	}
	fmt.Printf("debug metadata file name = %s: topics = %d\n", b.metaFile.Name(), size)

}

func (b *Broker) store() {
	writeu32(b.metaFile, 0, uint32(len(b.topics)))
	for i, topic := range b.topics {
		writeu16(b.metaFile, int64(4+i*2), topic.topicID)
	}
	fmt.Printf("debug metadata file name = %s: topics = %d\n", b.metaFile.Name(), len(b.topics))
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
		resp, err := b.processProducerRegisterMessage(*message.PREG)
		if err != nil {
			return nil, err
		}
		return &Message{rPREG: resp}, nil
	}
	if message.CREG != nil {
		resp, err := b.processConsumerRegisterMessage(message.CREG)
		if err != nil {
			return nil, err
		}
		return &Message{rCREG: resp}, nil
	}
	return nil, nil
}

func (b *Broker) processProducerRegisterMessage(pRegMessage ProducerRegisterMessage) (*byte, error) {
	fmt.Printf("Broker received pRegMessage: port=%d, topicID=%d\n", pRegMessage.port, pRegMessage.topicID)
	var topic *Topic

	for _, tp := range b.topics {
		if tp.topicID == pRegMessage.topicID {
			topic = tp
			break
		}
	}

	if topic == nil {
		tp := &Topic{}
		tp.init(pRegMessage.topicID)
		b.topics = append(b.topics, tp)
		b.store()
		topic = tp
	}

	go func() {
		conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", pRegMessage.port))
		fmt.Printf("Connected to server at port %v\n", pRegMessage.port)
		// Read input from stdin and write to stream.
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			parsedMessage, err := readMessageFromStream(streamRw)

			if parsedMessage == nil || err != nil {
				panic(err)
			}
			// Process something here
			if parsedMessage.ProducerConsumeMessage != nil {
				resp, err := b.ProcessProducerConsumeMessage(parsedMessage.ProducerConsumeMessage, topic)
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

func (b *Broker) processEchoMessage(echoMessage *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *echoMessage), nil
}

func (b *Broker) ProcessProducerConsumeMessage(pcm []byte, topic *Topic) (byte, error) {
	// If there are no cgroups yet, store in topic's mq
	if len(topic.cgroups) == 0 {
		topic.mq.push(pcm)
		topic.mq.debug()
		return 0, nil
	}

	for _, cg := range topic.cgroups {
		minSize := 100000
		var targetPartitionIdx int = -1
		for idx, partition := range cg.partitions {
			currentSize := partition.queue.size()
			if currentSize < minSize {
				minSize = currentSize
				targetPartitionIdx = idx
			}
		}
		if targetPartitionIdx != -1 {
			targetPartition := cg.partitions[targetPartitionIdx]
			targetPartition.lock.Lock()

			// First, dump all messages from topic's mq to this partition
			for {
				msgFromMQ := topic.mq.pop()
				if msgFromMQ == nil {
					break
				}
				targetPartition.queue.push(msgFromMQ)
			}

			targetPartition.queue.push(pcm)
			fmt.Printf("Put data '%s' to cgroup %d, partition %d\n", string(pcm), cg.groupID, targetPartitionIdx)
			targetPartition.lock.Unlock()
		}
	}

	return 0, nil
}

func (b *Broker) processConsumerRegisterMessage(cRegMessage *ConsumerRegisterMessage) (*byte, error) {
	fmt.Printf("Broker received cRegMessage: port=%d, topicID=%d, groupID=%d\n", cRegMessage.port, cRegMessage.topicID, cRegMessage.groupID)
	var topic *Topic
	for _, tp := range b.topics {
		if tp.topicID == cRegMessage.topicID {
			topic = tp
			break
		}
	}
	if topic == nil {
		tp := &Topic{}
		tp.init(cRegMessage.topicID)
		b.topics = append(b.topics, tp)
		b.store()
		topic = tp
	}
	var cgroup *CGroup
	for _, cg := range topic.cgroups {
		if cg.groupID == cRegMessage.groupID {
			cgroup = cg
			break
		}
	}
	if cgroup == nil {
		cg := &CGroup{}
		cg.init(topic.topicID, cRegMessage.groupID)
		topic.lock.Lock()
		topic.cgroups = append(topic.cgroups, cg)
		topic.store()
		topic.lock.Unlock()
		cgroup = cg
		// Initialize first partition
		if len(cgroup.partitions) == 0 {
			partition := &Partition{}
			partition.init(topic.topicID, cgroup.groupID, 1)
			cgroup.lock.Lock()
			cgroup.partitions = append(cgroup.partitions, partition)
			cgroup.store()
			cgroup.lock.Unlock()
		}
	}
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", cRegMessage.port))
	fmt.Printf("Connected to consumer at port %v\n", cRegMessage.port)
	consumer := &ConsumerConn{
		status: true,
		conn:   conn,
	}
	cgroup.lock.Lock()
	cgroup.consumers = append(cgroup.consumers, consumer)
	if len(cgroup.partitions) < len(cgroup.consumers) {
		partition := &Partition{}
		partition.init(topic.topicID, cgroup.groupID, uint16(len(cgroup.partitions)+1))
		cgroup.partitions = append(cgroup.partitions, partition)
		cgroup.store()
	}
	fmt.Printf("Pushed to the list of consumer, port %v, partition %d\n", cRegMessage.port, len(cgroup.partitions)-1)
	partitionIdx := len(cgroup.partitions) - 1
	cgroup.lock.Unlock()
	go b.readConsumerReadyAndSend(cgroup, consumer, partitionIdx)
	var resp byte = 0
	return &resp, nil
}

func (b *Broker) readConsumerReadyAndSend(cgroup *CGroup, consumerConn *ConsumerConn, partitionIdx int) {
	streamRW := bufio.NewReadWriter(bufio.NewReader(consumerConn.conn), bufio.NewWriter(consumerConn.conn))

	for {
		// Read ack
		if !consumerConn.status {
			parsedMessage, err := readMessageFromStream(streamRW) // Wait forever!!
			if parsedMessage == nil || err != nil {
				panic(err)
			}
			if parsedMessage.rProducerConsumeMessage != nil {
				consumerConn.status = true
			} else {
				fmt.Printf("Parsed message not rProducerConsumeMessage: %v", parsedMessage)
				panic("Why not rProducerConsumeMessage???")
			}
		}

		// Take message from the assigned partition
		partition := cgroup.partitions[partitionIdx]

		partition.lock.Lock()
		pcm := partition.queue.pop()
		partition.lock.Unlock()

		if pcm == nil {
			continue
		}

		// Write PCM message to ready consumer
		consumerConn.status = false
		err := writeMessageToStream(streamRW, Message{
			ProducerConsumeMessage: pcm,
		})
		if err != nil {
			panic(err)
		}
	}
}
