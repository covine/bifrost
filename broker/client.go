package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/net/websocket"

	"github.com/covine/bifrost/broker/session"
	"github.com/covine/bifrost/plugins/bridge"
)

func (b *Broker) serve(conn net.Conn, connType ConnType) {
	/// review newServer() free() store() drop() online() offline() goroutines
	/// store drop -> list map
	if connType == ConnTypClient && atomic.LoadUint64(&b.info.ClientCount) >= b.config.Client.Connection.Max {
		_ = conn.Close()
		return
	}

	if connType == ConnTypClient {
		atomic.AddUint64(&b.info.ClientCount, 1)
		defer atomic.AddUint64(&b.info.ClientCount, ^uint64(0))
	} else {
		atomic.AddUint64(&b.info.ProxyCount, 1)
		defer atomic.AddUint64(&b.info.ProxyCount, ^uint64(0))
	}

	c := newServer(b, conn, connType)
	defer c.free()
	defer c.close()

	ctx, cancel := context.WithTimeout(context.TODO(), c.broker.connectWaitTime)
	go c.waitConnect(ctx)
	packet, err := packets.ReadPacket(c.connection)
	if err != nil {
		cancel()
		return
	}
	connect, ok := packet.(*packets.ConnectPacket)
	if !ok {
		cancel()
		return
	}
	if err := c.handleConnect(connect); err != nil {
		cancel()
		return
	}
	cancel()

	c.store()
	defer c.drop()

	c.online()
	defer c.offline()

	c.workers.Add(4)
	go c.onSend()
	go c.beat()
	go c.onReceivePubSub()
	go c.receive()
	c.workers.Wait()
}

type server struct {
	// broker
	broker *Broker
	// tcp connection
	connection net.Conn
	// server property
	id         string
	localIP    string
	localPort  string
	remoteIP   string
	remotePort string
	peer       bool
	status     ConnStatus
	mu         sync.RWMutex
	// worker
	workers sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	// receive
	keepalive   uint16
	recvChan    chan packets.ControlPacket
	recvChanLen int
	// send
	ackChan chan packets.ControlPacket
	// send.publish
	pubChan    chan *packets.PublishPacket
	pubChanLen int
	pubMu      sync.RWMutex
	// mid
	midStore *midStore
	// connect
	connectReceived bool
	connected       bool
	withClientID    bool
	peerID          string
	peerConnID      string
	clientID        string
	cleanSession    bool
	will            *packets.PublishPacket
	username        string
	password        []byte
	// session
	session *session.Session
	// sub/pub map
	subMap map[string]*subscription
	pubMap map[string]*publish
}

func (s *server) free() {
	fmt.Printf("free client: %s\n", s.clientID)

	s.midStore.close()
	s.midStore = nil

	s.subMap = nil
	s.pubMap = nil
}

func newServer(b *Broker, connection net.Conn, connType ConnType) *server {
	/// review
	recvChanLen := b.config.Receiver.Client.RecvChan
	if connType != ConnTypClient {
		recvChanLen = b.config.Receiver.Peer.RecvChan
	}

	ackChanLen := b.config.Sender.Client.AckChan
	pubChanLen := b.config.Sender.Client.PubChan
	if connType != ConnTypClient {
		ackChanLen = b.config.Sender.Peer.AckChan
		pubChanLen = b.config.Sender.Peer.PubChan
	}

	s := server{
		broker:     b,
		connection: connection,
		id:         uuid.NewV4().String(),
		status:     ConnStatusConnected,
		// recv
		recvChanLen: recvChanLen,
		recvChan:    make(chan packets.ControlPacket, recvChanLen),
		// send
		ackChan: make(chan packets.ControlPacket, ackChanLen),
		// send.publish
		pubChanLen: pubChanLen,
		pubChan:    make(chan *packets.PublishPacket, pubChanLen),
		// message id store
		midStore: newMidStore(),
		// connect
		cleanSession: true,
		withClientID: false,
		clientID:     "",
		connected:    false,
		subMap:       make(map[string]*subscription),
		pubMap:       make(map[string]*publish),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.localIP, s.localPort, _ = net.SplitHostPort(s.connection.LocalAddr().String())

	remoteAddr := s.connection.RemoteAddr()
	network := remoteAddr.Network()
	if network != "websocket" {
		s.remoteIP, s.remotePort, _ = net.SplitHostPort(remoteAddr.String())
	} else {
		ws := s.connection.(*websocket.Conn)
		s.remoteIP, s.remotePort, _ = net.SplitHostPort(ws.Request().RemoteAddr)
	}

	if connType != ConnTypClient {
		s.peer = true
	}

	return &s
}

func (s *server) waitConnect(ctx context.Context) {
	select {
	case <-ctx.Done():
		if !s.connectReceived {
			fmt.Printf("no <connect> packet after %d secs, close server\n", s.broker.config.Client.WaitConnect)
			s.close()
		}
	case <-s.ctx.Done():
	}
}

func (s *server) store() {
	/// 实现有序map
	if !s.peer {
		s.broker.clientsMu.Lock()
		pre, ok := s.broker.clients[s.clientID]
		s.broker.clients[s.clientID] = s
		s.broker.clientsMu.Unlock()
		if ok {
			fmt.Printf("close previous client id: %s\n", pre.clientID)
			pre.close()
		}
	} else {
		s.broker.proxyMesMu.Lock()
		pre, ok := s.broker.proxyMes[s.clientID]
		s.broker.proxyMes[s.clientID] = s
		s.broker.proxyMesMu.Unlock()
		if ok {
			fmt.Printf("close previous peer me id: %s\n", pre.clientID)
			pre.close()
		}
	}
}

func (s *server) drop() {
	if !s.peer {
		s.broker.clientsMu.Lock()
		if cli, ok := s.broker.clients[s.clientID]; ok {
			if cli == s {
				delete(s.broker.clients, s.clientID)
			}
		}
		s.broker.clientsMu.Unlock()
	} else {
		s.broker.proxyMesMu.Lock()
		if cli, ok := s.broker.proxyMes[s.clientID]; ok {
			if cli == s {
				delete(s.broker.proxyMes, s.clientID)
			}
		}
		s.broker.proxyMesMu.Unlock()
	}
}

func (s *server) online() {
	if !s.peer {
		s.broker.publishOnOffLine(s.clientID, s.remoteIP, s.username, true)
		if s.broker.bridgeProvider != nil {
			err := s.broker.bridgeProvider.Publish(&bridge.Elements{
				ClientID:  s.clientID,
				Username:  s.username,
				Action:    bridge.Connect,
				Timestamp: time.Now().Unix(),
			})
			if err != nil {
				fmt.Printf("bridge mq error: %v\n", err)
			}
		}
	}
}

func (s *server) offline() {
	for topic, sub := range s.subMap {
		err := s.broker.topicCore.Unsubscribe([]byte(sub.topic), sub)
		if err != nil {
			fmt.Printf("unsubscribe error: %v server id: %s\n ", err, s.clientID)
		}

		if !s.peer {
			s.broker.proxyUnSub(s.id, topic)

			if s.broker.bridgeProvider != nil {
				err := s.broker.bridgeProvider.Publish(&bridge.Elements{
					ClientID:  s.clientID,
					Username:  s.username,
					Action:    bridge.Unsubscribe,
					Timestamp: time.Now().Unix(),
					Topic:     topic,
				})
				if err != nil {
					fmt.Printf("bridge mq error: %v\n", err)
				}
			}
		}
	}

	if !s.peer {
		if s.cleanSession {
			_ = s.broker.sessionStore.Remove(s.clientID, s.id)
			s.session.Free()
		}
		if s.will != nil {
			s.broker.publish(s.will)
		}

		s.broker.publishOnOffLine(s.clientID, s.remoteIP, s.username, false)

		if s.broker.bridgeProvider != nil {
			err := s.broker.bridgeProvider.Publish(&bridge.Elements{
				ClientID:  s.clientID,
				Username:  s.username,
				Action:    bridge.Disconnect,
				Timestamp: time.Now().Unix(),
			})
			if err != nil {
				fmt.Printf("bridge mq error: %v\n", err)
			}
		}
	} else {
		_ = s.broker.sessionStore.Remove(s.clientID, s.id)
		s.session.Free()
	}
}

func (s *server) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status = ConnStatusClosed
	s.cancel()
	_ = s.connection.Close()
}

func (s *server) sendPub(p *packets.PublishPacket) {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()

	switch p.Qos {
	case 1:
		// give space for retry
		if len(s.pubChan) < (s.pubChanLen/7*6) && s.session.Inflight().SpaceLeft() {
			p.MessageID = s.midStore.alloc()
			select {
			case <-s.ctx.Done():
			case s.pubChan <- p:
				fp := &flyPacket{
					Packet: p,
					Time:   time.Now(),
				}
				s.session.Inflight().PushBack(p.MessageID, fp, false, false)
			}
		} else {
			s.session.PushBackOT(p)
		}
	case 0:
		if len(s.pubChan) < (s.pubChanLen / 7 * 1) {
			select {
			case <-s.ctx.Done():
			case s.pubChan <- p:
			}
		} else {
			// if congestion occurs
			s.session.PushBackZ(p)
		}
	}
}

func (s *server) pubRetain(p *packets.PublishPacket, qos byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status != ConnStatusConnected {
		return
	}

	pub := s.broker.newPublish()
	pub.TopicName = p.TopicName
	pub.Payload = p.Payload
	pub.Qos = qos
	pub.Retain = true

	s.sendPub(pub)
}

func (s *server) pub(p *packets.PublishPacket, qos byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status != ConnStatusConnected {
		return
	}

	pub := s.broker.newPublish()
	pub.TopicName = p.TopicName
	pub.Payload = p.Payload
	pub.Qos = qos

	s.sendPub(pub)
}

func (s *server) sharePub(p *packets.PublishPacket, qos byte, group string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status != ConnStatusConnected {
		return
	}

	pub := s.broker.newPublish()
	pub.TopicName = p.TopicName
	pub.Payload = p.Payload
	pub.Qos = qos
	if s.peer {
		if p.Retain {
			pub.Retain = true
		}
		pub.TopicName = "$share/" + group + "/" + pub.TopicName
	}

	s.sendPub(pub)
}

func (s *server) ackPub(packet *packets.PublishPacket) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status != ConnStatusConnected {
		return
	}

	switch packet.Qos {
	case 2:
	case 1:
		pa := s.broker.newPuback()
		pa.MessageID = packet.MessageID
		select {
		case <-s.ctx.Done():
		case s.ackChan <- pa:
		}
	case 0:
	}
}

func (s *server) ack(p packets.ControlPacket) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status != ConnStatusConnected {
		return
	}

	select {
	case <-s.ctx.Done():
	case s.ackChan <- p:
	}
}
