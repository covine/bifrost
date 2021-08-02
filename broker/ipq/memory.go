package ipq

import (
	"container/list"
	"sync"
)

var defaultMaxLen = 1024

type Item struct {
	Key   interface{}
	Value interface{}
}

type MemoryIPQ struct {
	sync.RWMutex
	maxLen      int
	list        *list.List
	table       map[interface{}]*list.Element
	delCallback func(interface{})
}

func NewMemoryIPQ(maxLen int, f func(interface{})) *MemoryIPQ {
	l := maxLen
	if l < 1 {
		l = defaultMaxLen
	}
	return &MemoryIPQ{
		maxLen:      l,
		list:        list.New(),
		table:       make(map[interface{}]*list.Element),
		delCallback: f,
	}
}

func (m *MemoryIPQ) SpaceLeft() bool {
	return m.list.Len() < m.maxLen
}

func (m *MemoryIPQ) Len() int {
	return m.list.Len()
}

func (m *MemoryIPQ) PushBack(k, v interface{}, move, update bool) {
	if k == nil || v == nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	if ele, ok := m.table[k]; ok {
		if move {
			m.list.MoveToBack(ele)
		}
		if update {
			ele.Value.(*Item).Value = v
		}
		return
	}

	e := m.list.PushBack(&Item{
		Key:   k,
		Value: v,
	})
	m.table[k] = e

	for m.list.Len() > m.maxLen {
		f := m.list.Front()
		delete(m.table, f.Value.(*Item).Key)
		m.list.Remove(f)
	}
}

func (m *MemoryIPQ) Get(k interface{}) interface{} {
	if k == nil {
		return nil
	}

	m.RLock()
	defer m.RUnlock()

	if ele, ok := m.table[k]; ok {
		return ele.Value.(*Item).Value
	}

	return nil
}

func (m *MemoryIPQ) List() *list.List {
	return m.list
}

func (m *MemoryIPQ) Del(k interface{}) {
	if k == nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	if ele, ok := m.table[k]; ok {
		delete(m.table, k)
		m.list.Remove(ele)
		m.delCallback(ele.Value)
	}
}

func (m *MemoryIPQ) Range(f func(*Item) bool) {
	m.RLock()
	defer m.RUnlock()

	var next *list.Element
	for e := m.list.Front(); e != nil; e = next {
		next = e.Next()
		r := f(e.Value.(*Item))
		if r {
			continue
		} else {
			break
		}
	}
}

func (m *MemoryIPQ) DelInRange(f func(*Item) (bool, bool)) {
	m.Lock()
	defer m.Unlock()

	var next *list.Element
	for e := m.list.Front(); e != nil; e = next {
		next = e.Next()
		con, del := f(e.Value.(*Item))
		if del {
			delete(m.table, e.Value.(*Item).Key)
			m.list.Remove(e)
			m.delCallback(e.Value)
		}
		if con {
			continue
		} else {
			break
		}
	}
}

func (m *MemoryIPQ) Reset() {
	m.Lock()
	defer m.Unlock()

	m.table = make(map[interface{}]*list.Element)
	m.list.Init()
}

func (m *MemoryIPQ) Free() {
	m.Lock()
	defer m.Unlock()

	m.table = nil
	m.list.Init()
}
