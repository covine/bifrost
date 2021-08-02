package broker

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type iTopicsCore interface {
	// if old subscription exists in topic, replace it with the new subscription and qos,
	// otherwise, append the new one.
	Swap(topic []byte, old, new *subscription, qos byte) error

	// append new subscription to topic if not exists, update qos if exists
	Subscribe(topic []byte, sub *subscription, qos byte) error

	// remove sub from sub tree if exists
	Unsubscribe(topic []byte, sub *subscription) error

	Subscribers(topic []byte, subs *[]*subscription, qoss *[]byte) (uint64, error)

	Retain(msg *packets.PublishPacket) error

	Retained(topic []byte, msgs *[]*packets.PublishPacket) error

	Version() uint64

	Close() error
}

type memoryCore struct {
	// Sub/unsub mutex
	smu sync.RWMutex
	// Subscription tree
	subRoot *snode
	// Retained message mutex
	rmu sync.RWMutex
	// Retained messages topic tree
	retainRoot *retainNode

	version uint64
}

func newMemoryCore() *memoryCore {
	return &memoryCore{
		subRoot:    newSubNode(),
		retainRoot: newRetainNode(),
		version:    0,
	}
}

func (m *memoryCore) Version() uint64 {
	return atomic.LoadUint64(&m.version)
}

func (m *memoryCore) Swap(topic []byte, old, new *subscription, qos byte) error {
	m.smu.Lock()
	defer m.smu.Unlock()

	return m.subRoot.swap(topic, old, new, qos, &m.version)
}

func (m *memoryCore) Subscribe(topic []byte, sub *subscription, qos byte) error {
	m.smu.Lock()
	defer m.smu.Unlock()

	return m.subRoot.sinsert(topic, qos, sub, &m.version)
}

func (m *memoryCore) Unsubscribe(topic []byte, sub *subscription) error {
	m.smu.Lock()
	defer m.smu.Unlock()

	return m.subRoot.sremove(topic, sub, &m.version)
}

func (m *memoryCore) Subscribers(topic []byte, subs *[]*subscription, qoss *[]byte) (uint64, error) {
	m.smu.RLock()
	defer m.smu.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	return m.version, m.subRoot.smatch(topic, subs, qoss)
}

func (m *memoryCore) Retain(msg *packets.PublishPacket) error {
	m.rmu.Lock()
	defer m.rmu.Unlock()

	if len(msg.Payload) == 0 {
		return m.retainRoot.remove([]byte(msg.TopicName))
	}

	return m.retainRoot.insertOrUpdate([]byte(msg.TopicName), msg)
}

func (m *memoryCore) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	m.rmu.RLock()
	defer m.rmu.RUnlock()

	return m.retainRoot.match(topic, msgs)
}

func (m *memoryCore) Close() error {
	m.subRoot = nil
	m.retainRoot = nil
	return nil
}

const (
	MWC = "#"

	SWC = "+"

	// SEP = "/"

	// SYS = "$"
)

const (
	stateCHR byte = iota // regular character
	stateMWC             // #
	stateSWC             // +
	// stateSEP             // /
	// stateSYS             // $
)

func next(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			// can not #/
			// # must at the last level
			if s == stateMWC {
				return nil, nil, errors.New("'#' must at the last level")
			}
			// /*******
			if i == 0 {
				return []byte(SWC), topic[i+1:], nil
			}
			return topic[:i], topic[i+1:], nil
		case '#':
			if i != 0 {
				return nil, nil, errors.New("'#' must occupy entire topic level")
			}
			s = stateMWC
		case '+':
			if i != 0 {
				return nil, nil, errors.New("'+' must occupy entire topic level")
			}
			s = stateSWC
		default:
			// only /+/ or /#/
			if s == stateMWC || s == stateSWC {
				return nil, nil, errors.New("'#' and '+' must occupy entire topic level")
			}
			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
}

type snode struct {
	subs []*subscription
	qos  []byte
	// next topic level
	snodes map[string]*snode
}

func newSubNode() *snode {
	return &snode{
		snodes: make(map[string]*snode),
	}
}

func (s *snode) smatch(topic []byte, subs *[]*subscription, qoss *[]byte) error {
	if len(topic) == 0 {
		for i, sub := range s.subs {
			*subs = append(*subs, sub)
			*qoss = append(*qoss, s.qos[i])
		}

		if mwc, _ := s.snodes[MWC]; mwc != nil {
			for i, sub := range mwc.subs {
				*subs = append(*subs, sub)
				*qoss = append(*qoss, mwc.qos[i])
			}
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := next(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	if n, ok := s.snodes[MWC]; ok {
		for i, sub := range n.subs {
			*subs = append(*subs, sub)
			*qoss = append(*qoss, n.qos[i])
		}
	}

	if n, ok := s.snodes[SWC]; ok {
		if err := n.smatch(rem, subs, qoss); err != nil {
			return err
		}
	}

	if n, ok := s.snodes[level]; ok {
		if err := n.smatch(rem, subs, qoss); err != nil {
			return err
		}
	}

	return nil
}

func (s *snode) sinsert(topic []byte, qos byte, sub *subscription, version *uint64) error {
	if len(topic) == 0 {
		for i := range s.subs {
			if s.subs[i] == sub {
				s.qos[i] = qos
				return nil
			}
		}

		s.subs = append(s.subs, sub)
		s.qos = append(s.qos, qos)

		atomic.AddUint64(version, 1)
		return nil
	}

	lev, remain, err := next(topic)
	if err != nil {
		return err
	}

	level := string(lev)
	n, ok := s.snodes[level]
	if !ok {
		n = newSubNode()
		s.snodes[level] = n
	}

	return n.sinsert(remain, qos, sub, version)
}

func (s *snode) swap(topic []byte, old, new *subscription, qos byte, version *uint64) error {
	if len(topic) == 0 {
		for i := range s.subs {
			if s.subs[i] == old {
				s.subs[i] = new
				s.qos[i] = qos
				return nil
			}
		}

		s.subs = append(s.subs, new)
		s.qos = append(s.qos, qos)

		atomic.AddUint64(version, 1)
		return nil
	}

	ntl, remain, err := next(topic)
	if err != nil {
		return err
	}

	level := string(ntl)
	n, ok := s.snodes[level]
	if !ok {
		n = newSubNode()
		s.snodes[level] = n
	}

	return n.swap(remain, old, new, qos, version)
}

func (s *snode) sremove(topic []byte, sub interface{}, version *uint64) error {
	if len(topic) == 0 {
		if sub == nil {
			s.subs = s.subs[0:0]
			s.qos = s.qos[0:0]
			atomic.AddUint64(version, 1)
			return nil
		}

		for i := range s.subs {
			if s.subs[i] == sub {
				s.subs = append(s.subs[:i], s.subs[i+1:]...)
				s.qos = append(s.qos[:i], s.qos[i+1:]...)
				atomic.AddUint64(version, 1)
				return nil
			}
		}

		return nil
	}

	ntl, rem, err := next(topic)
	if err != nil {
		return err
	}

	level := string(ntl)
	n, ok := s.snodes[level]
	if !ok {
		return nil
	}

	if err := n.sremove(rem, sub, version); err != nil {
		return err
	}

	if len(n.subs) == 0 && len(n.snodes) == 0 {
		delete(s.snodes, level)
	}

	return nil
}

// retained message nodes
type retainNode struct {
	// If this is the end of the topic string, then add retained messages here
	msg *packets.PublishPacket
	// Otherwise add the next topic level here
	retainNodes map[string]*retainNode
}

func newRetainNode() *retainNode {
	return &retainNode{
		retainNodes: make(map[string]*retainNode),
	}
}

func (r *retainNode) allRetained(msgs *[]*packets.PublishPacket) {
	if r.msg != nil {
		*msgs = append(*msgs, r.msg)
	}

	for _, n := range r.retainNodes {
		n.allRetained(msgs)
	}
}

func (r *retainNode) match(topic []byte, msgs *[]*packets.PublishPacket) error {
	// If the topic is empty, it means we are at the final matching retainNode. If so,
	// add the retained msg to the list.
	if len(topic) == 0 {
		if r.msg != nil {
			*msgs = append(*msgs, r.msg)
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := next(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	if level == MWC {
		// If '#', add all retained messages starting this node
		r.allRetained(msgs)
	} else if level == SWC {
		// If '+', check all nodes at this level. Next levels must be matched.
		for _, n := range r.retainNodes {
			if err := n.match(rem, msgs); err != nil {
				return err
			}
		}
	} else {
		// Otherwise, find the matching node, go to the next level
		if n, ok := r.retainNodes[level]; ok {
			if err := n.match(rem, msgs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *retainNode) insertOrUpdate(topic []byte, msg *packets.PublishPacket) error {
	// If there's no more topic levels, that means we are at the matching retainNode.
	if len(topic) == 0 {
		// Reuse the message if possible
		r.msg = msg

		return nil
	}

	// Not the last level, so let's find or create the next level snode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := next(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add snode if it doesn't already exist
	n, ok := r.retainNodes[level]
	if !ok {
		n = newRetainNode()
		r.retainNodes[level] = n
	}

	return n.insertOrUpdate(rem, msg)
}

// Remove the retained message for the topic
func (r *retainNode) remove(topic []byte) error {
	// If the topic is empty, it means we are at the final matching retainNode.
	// remove the buffer and message.
	if len(topic) == 0 {
		r.msg = nil
		return nil
	}

	// Not the last level
	// find the next level retainNode, and recursively call it's remove()
	// ntl = next topic level
	ntl, rem, err := next(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the retainNode that matches the topic level
	n, ok := r.retainNodes[level]
	if !ok {
		return nil
	}

	// Remove the subscriber from the next level retainNode
	if err := n.remove(rem); err != nil {
		return err
	}

	// If there are no more retainNodes to the next level we just visited let's remove it
	if len(n.retainNodes) == 0 {
		delete(r.retainNodes, level)
	}

	return nil
}
