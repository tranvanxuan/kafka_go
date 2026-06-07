package main

import "sync"

type Partition struct {
	queue Queue
	lock  sync.Mutex
}

func (p *Partition) init(topicID, cgroupID, partitionID uint16) {
	p.queue = Queue{}
	p.queue.init(topicID, cgroupID, partitionID)
}
