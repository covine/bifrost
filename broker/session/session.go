package session

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"

	"github.com/covine/bifrost/broker/ipq"
)

type Session struct {
	mu sync.RWMutex
	// client id
	id     string
	connId string
	// subscribe topics and qoss
	topics map[string]byte
	// for qos 0
	ZQueue    *list.List
	ZQueueLen int
	// for qos 1, 2
	OTQueue    *list.List
	OTQueueLen int
	// for qos 1, 2
	inflight    ipq.IPQ
	inflightLen int

	delCallback func(*packets.PublishPacket)
}

func newSession(id string, connId string, zeroQueueLen, OneTwoQueueLen, inflight int,
	f func(*packets.PublishPacket), df func(interface{})) (*Session, error) {
	return &Session{
		id:          id,
		connId:      connId,
		topics:      make(map[string]byte, 1),
		ZQueue:      list.New(),
		ZQueueLen:   zeroQueueLen,
		OTQueue:     list.New(),
		OTQueueLen:  OneTwoQueueLen,
		inflight:    ipq.NewMemoryIPQ(inflight, df),
		inflightLen: inflight,
		delCallback: f,
	}, nil
}

func (s *Session) ConnID() string {
	return s.connId
}

func (s *Session) UpdateConnID(connID string) {
	s.connId = connID
}

func (s *Session) Free() {
	s.topics = nil
	s.ZQueue.Init()
	s.OTQueue.Init()
	s.inflight.Free()
}

func (s *Session) AddTopic(topic string, qos byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.topics[topic] = qos
	return nil
}

func (s *Session) RemoveTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.topics, topic)
	return nil
}

func (s *Session) Topics() ([]string, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var topics []string
	var qoss []byte

	for k, v := range s.topics {
		topics = append(topics, k)
		qoss = append(qoss, v)
	}

	return topics, qoss, nil
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) PushBackZ(packet *packets.PublishPacket) {
	s.ZQueue.PushBack(packet)
	for s.ZQueue.Len() > s.ZQueueLen {
		f := s.ZQueue.Front()
		s.ZQueue.Remove(f)
		s.delCallback(f.Value.(*packets.PublishPacket))
		fmt.Printf("remove packet from qos 0 queue\n")
	}
}

func (s *Session) PushBackOT(packet *packets.PublishPacket) {
	s.OTQueue.PushBack(packet)
	for s.OTQueue.Len() > s.OTQueueLen {
		f := s.OTQueue.Front()
		s.OTQueue.Remove(f)
		s.delCallback(f.Value.(*packets.PublishPacket))
		fmt.Printf("remove packet from qos 1,2 queue\n")
	}
}

func (s *Session) Inflight() ipq.IPQ {
	return s.inflight
}
