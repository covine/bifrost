package broker

import (
	"sync"
)

type midStore struct {
	sync.RWMutex
	index map[uint16]struct{}
}

const (
	minMid uint16 = 1
	maxMid uint16 = 65535
)

func newMidStore() *midStore {
	return &midStore{
		index: make(map[uint16]struct{}),
	}
}

func (m *midStore) reset() {
	m.Lock()
	defer m.Unlock()
	m.index = make(map[uint16]struct{})
}

func (m *midStore) close() {
	m.Lock()
	defer m.Unlock()
	m.index = nil
}

func (m *midStore) free(id uint16) {
	m.Lock()
	defer m.Unlock()
	delete(m.index, id)
}

// alloc 0 returned means no message id left
func (m *midStore) alloc() uint16 {
	m.Lock()
	defer m.Unlock()

	for i := minMid; i < maxMid; i++ {
		if _, ok := m.index[i]; !ok {
			m.index[i] = struct{}{}
			return i
		}
	}

	return 0
}
