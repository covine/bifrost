package broker

import (
	"fmt"
	"io"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func (s *server) receive() {
	defer s.workers.Done()

	keepAlive := time.Second * time.Duration(s.keepalive)
	timeOut := keepAlive + (keepAlive / 2)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			if keepAlive > 0 {
				if err := s.connection.SetReadDeadline(time.Now().Add(timeOut)); err != nil {
					fmt.Printf("set read dead line error: %v, client id: %s\n", err, s.clientID)
					s.close()
					return
				}
			}

			packet, err := s.broker.ReadPacket(s.connection)
			if err != nil {
				if err == io.EOF {
					fmt.Printf("client disconnected: %s:%s\n", s.remoteIP, s.remotePort)
				} else {
					fmt.Printf("error from: %s:%s, error: %s\n", s.remoteIP, s.remotePort, err)
				}
				s.close()
				return
			}

			switch m := packet.(type) {
			case *packets.PublishPacket:
				switch m.Qos {
				case 2:
					// QoS 2 not supported at present
				case 1:
					fallthrough
				case 0:
					if len(s.recvChan) < s.recvChanLen {
						select {
						case s.recvChan <- m:
						case <-s.ctx.Done():
						}
					}
				default:
					fmt.Printf("invalid publish qos\n")
					s.close()
					return
				}
			case *packets.PubackPacket:
				s.handlePuback(m)
			case *packets.SubscribePacket:
				if len(s.recvChan) < s.recvChanLen {
					select {
					case s.recvChan <- m:
					case <-s.ctx.Done():
					}
				}
			case *packets.UnsubscribePacket:
				if len(s.recvChan) < s.recvChanLen {
					select {
					case s.recvChan <- m:
					case <-s.ctx.Done():
					}
				}
			case *packets.PingreqPacket:
				s.handlePing(m)
			case *packets.DisconnectPacket:
				s.handleDisconnect(m)
			case *packets.ConnectPacket:
				// the second <Connect> packet received
				// should close the connection
				_ = s.handleConnect(m)
			case *packets.SubackPacket:
				// <Suback> can't be sent from client
			case *packets.ConnackPacket:
				// only server -> client
			case *packets.PubrecPacket:
				// QoS 2 not supported at present
			case *packets.PubrelPacket:
				// QoS 2 not supported at present
			case *packets.PubcompPacket:
				// QoS 2 not supported at present
			case *packets.UnsubackPacket:
				// <Unsuback> can't be sent from client
			case *packets.PingrespPacket:
				// only server -> client
			default:
				fmt.Printf("invalid packet\n")
				s.close()
				return
			}
		}
	}
}

func (s *server) onReceivePubSub() {
	defer s.workers.Done()

	reset := time.NewTicker(time.Duration(s.broker.config.Client.Reset) * time.Second)
	defer reset.Stop()
	for {
		select {
		case m := <-s.recvChan:
			switch msg := m.(type) {
			case *packets.PublishPacket:
				s.handlePublish(msg)
			case *packets.SubscribePacket:
				s.handleSubscribe(msg)
			case *packets.UnsubscribePacket:
				s.handleUnsubscribe(msg)
			}
		case <-reset.C:
			s.pubMap = make(map[string]*publish)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *server) onSend() {
	defer s.workers.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.ackChan:
			switch m := msg.(type) {
			case *packets.PingrespPacket:
				if err := m.Write(s.connection); err != nil {
					fmt.Printf("error: %v, client id: %s\n", err, s.clientID)
					s.close()
					return
				}
			case *packets.PubackPacket:
				if err := m.Write(s.connection); err != nil {
					fmt.Printf("error: %v, client id: %s\n", err, s.clientID)
					s.broker.putPuback(m)
					s.close()
					return
				}
				s.broker.putPuback(m)
			case *packets.SubackPacket, *packets.UnsubackPacket:
				if err := m.Write(s.connection); err != nil {
					fmt.Printf("error: %v, client id: %s\n", err, s.clientID)
					s.close()
					return
				}
			case *packets.ConnectPacket:
				// only client -> server
			case *packets.ConnackPacket:
				// already send in handleConnect
			case *packets.PublishPacket:
				// ackChan has no <Publish> packet
			case *packets.PubrecPacket:
				// QoS 2 not supported at present
			case *packets.PubrelPacket:
				// QoS 2 not supported at present
			case *packets.PubcompPacket:
				// QoS 2 not supported at present
			case *packets.SubscribePacket:
				// only client -> server
			case *packets.UnsubscribePacket:
				// only client -> server
			case *packets.PingreqPacket:
				// only client -> server
			case *packets.DisconnectPacket:
				// only client -> server
			}
		case pub := <-s.pubChan:
			if err := pub.Write(s.connection); err != nil {
				fmt.Printf("error: %v, client id: %s\n", err, s.clientID)
				s.close()
				return
			}

			if pub.Qos > 0 {
				p := s.session.Inflight().Get(pub.MessageID)
				if fp, ok := p.(*flyPacket); ok {
					fp.Time = time.Now()
				}
			} else {
				s.broker.putPublish(pub)
			}
		}
	}
}

func (m *mePeer) read() ConnStatus {
	reset := time.NewTicker(time.Duration(m.broker.config.Peer.Reset) * time.Second)
	defer reset.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return ConnStatusClosed
		case <-reset.C:
			m.pubMap = make(map[string]*publish)
		default:
			packet, err := packets.ReadPacket(m.connection)
			if err != nil {
				if err == io.EOF {
					fmt.Printf("me->peer disconnected: %s\n", m.peer)
				} else {
					fmt.Printf("read packet from peer->me: %s, error: %v\n", m.peer, err)
				}
				return ConnStatusBroken
			}

			switch msg := packet.(type) {
			case *packets.PublishPacket:
				switch msg.Qos {
				case 2:
					// QoS 2 not supported at present
				case 1:
					fallthrough
				case 0:
					m.onPublish(msg)
				default:
					// should close?
				}
			case *packets.SubackPacket:
				m.onSuback(msg)
			case *packets.UnsubackPacket:
				m.onUnsuback(msg)
			case *packets.ConnectPacket:
				// no direction
			case *packets.ConnackPacket:
				// handle after connect
			case *packets.PubackPacket:
				// no direction
			case *packets.PubrecPacket:
				// QoS 2 not supported at present
			case *packets.PubrelPacket:
				// QoS 2 not supported at present
			case *packets.PubcompPacket:
				// QoS 2 not supported at present
			case *packets.SubscribePacket:
				// no direction
			case *packets.UnsubscribePacket:
				// no direction
			case *packets.PingreqPacket:
				// no direction
			case *packets.PingrespPacket:
				// without
			case *packets.DisconnectPacket:
				// no direction
			default:
				// should close?
			}
		}
	}
}

func (m *mePeer) onSend() {
	defer m.workers.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case message := <-m.subUnsubChan:
			switch message.(type) {
			case *packets.SubscribePacket:
				m.write(message)
			case *packets.UnsubscribePacket:
				m.write(message)
			}
		case message := <-m.ackChan:
			switch msg := message.(type) {
			case *packets.PubackPacket:
				m.write(message)
				m.broker.putPuback(msg)
			case *packets.ConnectPacket:
			case *packets.ConnackPacket:
			case *packets.PublishPacket:
			case *packets.SubackPacket:
			case *packets.UnsubackPacket:
			case *packets.PubrecPacket:
			case *packets.PubrelPacket:
			case *packets.PubcompPacket:
			case *packets.PingreqPacket:
			case *packets.PingrespPacket:
			case *packets.DisconnectPacket:
			}
		}
	}
}
