<a id="readme-top"></a>
<h1 align="center">KafkaGo</h1>
<p align="center">
    <img src="docs/logo-no-background.svg" alt="Logo" height="128" width="128"/>
</p>
<p align="center">
    Build Kafka with Golang from zero to hero!
</p>

<a name="table-of-contents"></a>

## Table of contents

-   [Table of contents](#table-of-contents)
-   [Architecture](#architecture)
-   [Description](#description)
-   [Core Concepts](#core-concepts)
    -   [Network Programming](#network-programming)
    -   [TCP protocol](#tcp-protocol)
    -   [TCP Socket Flow](#tcp-socket-flow)
    -   [Buffered I/O](#buffered-io)
    -   [Multiple Connections](#multiple-connections)
-   [Installation](#installation)
-   [Usage](#usage)
-   [Config](#config)
-   [TODO](#todo)
-   [License](#license)
-   [Acknowledgments](#acknowledgments)

<!-- Description -->
<a name="description"></a>

## Description

### 🚀 KafkaGo: A High-Performance Distributed Streaming Platform in Go

KafkaGo is a research-focused implementation of a Distributed Message Queue, inspired by the architecture of Apache Kafka and built entirely from scratch using Golang.

While many developers use Kafka as a black box, this project aims to "peel back the curtain" by re-engineering its core components. By building the storage engine, partitioning logic, and network protocols manually, this project explores the intricate challenges of distributed systems and high-throughput data streaming.

### 🎯 Why Does This Project Exist?
"What I cannot create, I do not understand." – **Richard Feynman**.

Born from a fascination with Distributed Systems, Gokafka was created to demystify the "magic" behind Kafka. The primary objectives are:

**Deep Dive:** To master the mechanics of Append-only Log storage.

**Concurrency:** To leverage the full power of Goroutines and Channels for handling thousands of concurrent connections.

**Distributed Logic:** To manually design distributed mechanisms, from Broker management to Partition logic.

<!-- Architecture -->
<a name="architecture"></a>

## Architecture

<img src="docs/architeture.png" alt="Architecture" />

-   **Producer**: the entity that sends messages to the queue
-   **Consumer**: the entity that receives and processes messages from the queue
-   **Message**: the data unit sent between the producer and consumer
-   **Topic**: categories used to organize messages. Messages are sent to and read from specific topics. In other words, producers write data to topics, and consumers read data from topics.
-   **Subscription**: a configuration defining which consumers receive messages from a specific topic.
- **Consumer Group**: a group of consumers sharing a common subscription to a topic.
- **Commit**: acknowledgment of a message by a consumer. Signal for dequeue.

<!-- Core Concepts -->
<a name="core-concepts"></a>

## Core Concepts

<a name="network-programming"></a>

### Network Programming

-   In a computer network, the devices can use vastly different softward and hardware. However, they need to communicate and send data to each other.

-   A protocol defines the format and the order of messages exchanged between two or more communicating entities, as well as the actions taken on the transmission and/or receipt of a message or other event.

<a name="tcp-protocol"></a>

### TCP protocol

<img src="docs/tcp.png" alt="TCP protocol" />

-   The first step is to send and receive message from another machine with the TCP protocol.

-   On machine A, create a TCP server with an address ("IP1") on a machine, which accepts a connection.

-    On machine B, connect to an address ("IP1") using TCP protocol.

-   Send and receive messages via the established stream.

<a name="tcp-socket-flow"></a>

### TCP Socket Flow

<img src="docs/tpc_socket_flow.png" alt="TCP Socket flow" />

-   **socket()**: Create a socket

-   **connect()**: Connect to a server and interact via **read()** and **write()**

-   **bind()**: Specify the local address and the port to user for a server

-   **listen()**: Set the length of the connection queue

-   **accept()**: Wait for the next connection request to arrive (get from the queue)

-   **close()**: closes the socket connection


<a name="buffered-io"></a>

### Buffered I/O

-   **Buffered I/O** batches data in user-space (or library) buffers and performs fewer, larger system calls. Great for throughput.

-   **Unbuffered I/O**: sends data directly (or more directly) to the kernel/device, offering lower-latency and more predictable timing — at a throughput cost for many small ops.

-   Use buffered I/O for normal file processing and high-throughput tasks. Use unbuffered or synchronous I/O when you need immediate visibility, determinism, or strict durability.

<a name="multiple-connections"></a>

### Multiple Connections

-   Handling multiple connection effectively with the help of OS is the main source of network programming efficiency gain!.

<!-- Installation -->
<a name="installation"></a>

## Installation

<!-- Usage -->
<a name="usage"></a>

## Usage

<!-- Config -->
<a name="config"></a>

## Config

<!-- Todo -->
<a name="todo"></a>

## TODO

<!-- License -->
<a name="licence"></a>

## License

[MIT](https://choosealicense.com/licenses/mit/)

<!-- ACKNOWLEDGMENTS -->
<a name="acknowledgments"></a>

## Acknowledgments
* [Why is TCP Called a Connection Oriented Protocol?](https://www.geeksforgeeks.org/computer-networks/why-is-tcp-called-a-connection-oriented-protocol/)
* [Buffered vs Unbuffered I/O on Unix](https://viniciusrocha.com/posts/buffered-vs-unbuffered-i/o-on-unix/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>
