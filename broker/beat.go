package broker

import (
	"container/list"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"

	"github.com/covine/bifrost/broker/ipq"
)

func (s *server) beat() {
	defer s.workers.Done()

	ticker := time.NewTicker(time.Duration(s.broker.config.Client.Retry) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.retry()
		}
	}
}

func (s *server) retry() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status != ConnStatusConnected {
		return
	}

	s.pubMu.Lock()
	defer s.pubMu.Unlock()

	s.session.Inflight().Range(func(item *ipq.Item) bool {
		if len(s.pubChan) < s.pubChanLen {
			fp := item.Value.(*flyPacket)
			if time.Now().Sub(fp.Time) > time.Duration(s.broker.config.Client.Retry)*time.Second {
				pub := fp.Packet.(*packets.PublishPacket)
				pub.Dup = true
				select {
				case <-s.ctx.Done():
					return false
				case s.pubChan <- pub:
					fp.Time = time.Now()
					// fmt.Printf("retry pub message id: %d\n", pub.MessageID)
				}
			}
			return true
		} else {
			return false
		}
	})

	// <Publish> QoS 2,1 queue
	var otNext *list.Element
	for e := s.session.OTQueue.Front(); e != nil; e = otNext {
		otNext = e.Next()
		if len(s.pubChan) < s.pubChanLen && s.session.Inflight().SpaceLeft() {
			pub := e.Value.(*packets.PublishPacket)
			pub.MessageID = s.midStore.alloc()
			pub.Dup = false
			select {
			case <-s.ctx.Done():
				return
			case s.pubChan <- pub:
				fp := &flyPacket{
					Packet: pub,
					Time:   time.Now(),
				}
				s.session.Inflight().PushBack(pub.MessageID, fp, false, false)
				s.session.OTQueue.Remove(e)
				// fmt.Printf("send packet 1, 2 from queue, message id: %d, len: %d\n", pub.MessageID, s.session.OTQueue.Len())
			}
		} else {
			break
		}
	}

	// <Publish> Qos 0 queue
	var zNext *list.Element
	for e := s.session.ZQueue.Front(); e != nil; e = zNext {
		zNext = e.Next()
		if len(s.pubChan) < s.pubChanLen {
			select {
			case <-s.ctx.Done():
				return
			case s.pubChan <- e.Value.(*packets.PublishPacket):
				s.session.ZQueue.Remove(e)
				// fmt.Printf("send packet 0 and remove from zero queue, len: %d\n", s.session.ZQueue.Len())
			}
		} else {
			break
		}
	}
}

func (m *mePeer) beat() {
	defer m.workers.Done()

	ticker := time.NewTicker(m.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.retry()
		}
	}
}

func (m *mePeer) retry() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status != ConnStatusConnected {
		return
	}

	m.subUnsubMu.Lock()
	defer m.subUnsubMu.Unlock()

	m.inflight.Range(func(item *ipq.Item) bool {
		if len(m.subUnsubChan) < m.subUnsubChanLen {
			fp := item.Value.(*flyPacket)
			if time.Now().Sub(fp.Time) > m.retryInterval {
				select {
				case <-m.ctx.Done():
					return false
				case m.subUnsubChan <- fp.Packet:
					fp.Time = time.Now()
					// fmt.Printf("retry sub unsub message id: %d\n", fp.Packet.Details().MessageID)
				}
			}
			return true
		} else {
			return false
		}
	})

	m.subUnsubQueue.DelInRange(func(item *ipq.Item) (bool, bool) {
		if len(m.subUnsubChan) < m.subUnsubChanLen && m.subUnsubQueue.SpaceLeft() {
			topic := item.Key.(string)
			topicType := item.Value.(*proxySubUnsub).topicType
			qos := item.Value.(*proxySubUnsub).qos

			var packet packets.ControlPacket
			if topicType == SUBType {
				p := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
				p.Topics = append(p.Topics, topic)
				p.Qoss = append(p.Qoss, qos)
				p.MessageID = m.midStore.alloc()
				packet = p
			} else {
				p := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
				p.Topics = append(p.Topics, topic)
				p.MessageID = m.midStore.alloc()
				packet = p
			}

			select {
			case <-m.ctx.Done():
				return false, false
			case m.subUnsubChan <- packet:
				fp := &flyPacket{
					Packet: packet,
					Time:   time.Now(),
				}
				m.inflight.PushBack(packet.Details().MessageID, fp, false, false)
				return true, true
			}
		} else {
			return false, false
		}
	})
}
