package session

import (
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type IStore interface {
	Count() int

	GetOrCreate(id string, connID string, peer bool) (*Session, bool, error)

	RemoveAndCreate(id string, connID string, peer bool) (*Session, error)

	Remove(id string, connID string) error
}

type Config struct {
	ZQueueLen      int
	OTQueueLen     int
	Inflight       int
	PeerZQueueLen  int
	PeerOTQueueLen int
	PeerInflight   int
}

type memoryStore struct {
	mu            sync.RWMutex
	config        *Config
	table         map[string]*Session
	delCallback   func(*packets.PublishPacket)
	delIFCallback func(interface{})
}

func NewMemoryStore(config *Config, f func(*packets.PublishPacket), df func(interface{})) *memoryStore {
	return &memoryStore{
		config:        config,
		table:         make(map[string]*Session),
		delCallback:   f,
		delIFCallback: df,
	}
}

func (m *memoryStore) Count() int {
	return len(m.table)
}

// GetOrCreate
// (*Session, bool, error) bool: exists
func (m *memoryStore) GetOrCreate(id string, connId string, peer bool) (*Session, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.table[id]; ok {
		s.UpdateConnID(connId)
		return s, true, nil
	}

	qosZeroQueue := m.config.ZQueueLen
	qoSOneTwoQueue := m.config.OTQueueLen
	inflight := m.config.Inflight
	if peer {
		qosZeroQueue = m.config.PeerZQueueLen
		qoSOneTwoQueue = m.config.PeerOTQueueLen
		inflight = m.config.PeerInflight
	}
	s, err := newSession(id, connId, qosZeroQueue, qoSOneTwoQueue, inflight, m.delCallback, m.delIFCallback)
	if err != nil {
		return nil, false, err
	}
	m.table[id] = s
	return s, false, nil
}

func (m *memoryStore) RemoveAndCreate(id string, connId string, peer bool) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.table, id)

	qosZeroQueue := m.config.ZQueueLen
	qoSOneTwoQueue := m.config.OTQueueLen
	inflight := m.config.Inflight
	if peer {
		qosZeroQueue = m.config.PeerZQueueLen
		qoSOneTwoQueue = m.config.PeerOTQueueLen
		inflight = m.config.PeerInflight
	}
	s, err := newSession(id, connId, qosZeroQueue, qoSOneTwoQueue, inflight, m.delCallback, m.delIFCallback)
	if err != nil {
		return nil, err
	}
	m.table[id] = s
	return s, nil
}

func (m *memoryStore) Remove(id string, connId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.table[id]; ok {
		if s.ConnID() == connId {
			delete(m.table, id)
		}
	}

	return nil
}
