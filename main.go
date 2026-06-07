package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func main() {
	// startServer()
	fmt.Println(os.Args)
	switch os.Args[1] {
	case "server":
		var broker = Broker{}
		err := broker.startBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	case "producer":
		fmt.Println("Trying to start producer processes")
		port, err := strconv.ParseInt(os.Args[2], 10, 16)
		if err != nil {
			panic(err)
		}
		topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
		if err != nil {
			panic(err)
		}
		producer := Producer{
			port:    uint16(port),
			topicID: uint16(topicID),
		}
		producer.startProducerServer()
	case "consumer":
		fmt.Println("Trying to start consumer processes")
		port, err := strconv.ParseInt(os.Args[2], 10, 16)
		if err != nil {
			panic(err)
		}
		topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
		if err != nil {
			panic(err)
		}
		groupID, err := strconv.ParseInt(os.Args[4], 10, 16)
		if err != nil {
			panic(err)
		}
		consumer := Consumer{
			port:    uint16(port),
			topicID: uint16(topicID),
			groupID: uint16(groupID),
		}
		consumer.startConsumerServer()
	default:
		clientConnectTCPAndEcho(10000)
	}
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
		}
	}
	message := Message{ECHO: &line}
	err = writeMessageToStream(streamRw, message)
	if err != nil {
		panic(err)
	}

	// Try to read back from the stream
	resp, err := readMessageFromStream(streamRw)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Receive message from server: %s\n", *resp.rECHO)
}
