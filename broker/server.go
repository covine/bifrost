package broker

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"

	"github.com/covine/bifrost/plugins/bridge"
)

func (s *server) handleConnect(packet *packets.ConnectPacket) error {
	/// handleConnect
	if s.connectReceived {
		s.close()
		return errors.New("<Connect> already received, connection closed")
	}
	s.connectReceived = true

	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.ReturnCode = packet.Validate()
	if connack.ReturnCode != packets.Accepted {
		return errors.New("invalid <Connect> packet")
	}

	// TODO auth
	if !s.broker.authenticate(packet.Username, string(packet.Password), packet.ClientIdentifier) {
		connack.ReturnCode = packets.ErrRefusedNotAuthorised
		_ = connack.Write(s.connection)
		return errors.New("authenticate failed")
	}

	if s.peer {
		res := strings.Split(packet.ClientIdentifier, ":")
		if len(res) != 2 {
			connack.ReturnCode = packets.ErrRefusedIDRejected
			_ = connack.Write(s.connection)
			return errors.New("error proxy id")
		}
		if len(res[0]) <= 0 || len(res[1]) <= 0 {
			connack.ReturnCode = packets.ErrRefusedIDRejected
			_ = connack.Write(s.connection)
			return errors.New("error proxy id")
		}
		s.peerID = res[0]
		s.peerConnID = res[1]
	}

	s.clientID = packet.ClientIdentifier
	s.cleanSession = packet.CleanSession
	if s.clientID != "" {
		s.withClientID = true
	} else {
		s.clientID = s.id
		s.withClientID = false
	}

	// session
	if s.cleanSession {
		session, err := s.broker.sessionStore.RemoveAndCreate(s.clientID, s.id, s.peer)
		if err != nil {
			connack.ReturnCode = packets.ErrRefusedServerUnavailable
			_ = connack.Write(s.connection)
			return err
		}
		s.session = session
		connack.ReturnCode = packets.Accepted
	} else {
		if !s.withClientID {
			connack.ReturnCode = packets.ErrRefusedIDRejected
			_ = connack.Write(s.connection)
			return errors.New("must clean session if without client id")
		}
		session, exists, err := s.broker.sessionStore.GetOrCreate(s.clientID, s.id, s.peer)
		if err != nil {
			connack.ReturnCode = packets.ErrRefusedServerUnavailable
			_ = connack.Write(s.connection)
			return err
		} else {
			s.session = session
			connack.SessionPresent = exists
			connack.ReturnCode = packets.Accepted
		}
	}

	if packet.WillFlag {
		s.will = s.broker.newPublish()
		s.will.Qos = packet.WillQos
		s.will.TopicName = packet.WillTopic
		s.will.Retain = packet.WillRetain
		s.will.Payload = packet.WillMessage
	} else {
		s.will = nil
	}
	s.username = packet.Username
	s.password = packet.Password
	s.keepalive = packet.Keepalive
	s.connected = true

	return connack.Write(s.connection)
}

func (s *server) handlePublish(packet *packets.PublishPacket) {
	// peer must never send <publish> packet
	if s.peer {
		return
	}

	pub, ok := s.pubMap[packet.TopicName]
	if ok {
		if pub.version != s.broker.topicCore.Version() {
			if err := syncPubMap(s.broker.topicCore, []byte(packet.TopicName), pub, false); err != nil {
				fmt.Printf("sync topic pub error, client id: %s\n", s.clientID)
				return
			}
		}
	} else {
		if !validTopic(packet.TopicName, s.peer, true) {
			fmt.Printf("invalid topic\n")
			s.close()
			return
		}

		// error or false returned is different, so finish it later
		if !s.broker.checkTopicAuth(PUB, s.clientID, s.username, s.remoteIP, packet.TopicName) {
			s.ackPub(packet)
			return
		}

		pub = &publish{
			version: 0,
		}
		if err := syncPubMap(s.broker.topicCore, []byte(packet.TopicName), pub, false); err != nil {
			fmt.Printf("sync topic pub error, client id: %s\n", s.clientID)
			return
		}
		s.pubMap[packet.TopicName] = pub
	}

	if packet.Retain {
		if err := s.broker.topicCore.Retain(packet); err != nil {
			fmt.Printf("error retaining message, error: %v, server id: %s\n", err, s.clientID)
			return
		}
	}

	s.ackPub(packet)

	for _, s := range pub.clients {
		s.sub.router.pub(packet, minQos(packet.Qos, s.qos))
	}

	for _, s := range pub.peers {
		if packet.Retain {
			s.sub.router.pubRetain(packet, minQos(packet.Qos, s.qos))
		} else {
			s.sub.router.pub(packet, minQos(packet.Qos, s.qos))
		}
	}

	for group, s := range pub.groups {
		if len(s.subs) > 0 {
			idx := r.Intn(len(s.subs))
			s.subs[idx].router.sharePub(packet, minQos(packet.Qos, s.qoss[idx]), group)
		}
	}

	if s.broker.bridgeProvider != nil {
		err := s.broker.bridgeProvider.Publish(&bridge.Elements{
			ClientID:  s.clientID,
			Username:  s.username,
			Action:    bridge.Publish,
			Timestamp: time.Now().Unix(),
			Payload:   string(packet.Payload),
			Size:      int32(len(packet.Payload)),
			Topic:     packet.TopicName,
		})
		if err != nil {
			fmt.Printf("bridge mq error: %v\n", err)
		}
	}
}

func (s *server) handlePuback(packet *packets.PubackPacket) {
	s.session.Inflight().Del(packet.MessageID)
	s.midStore.free(packet.MessageID)
}

func (s *server) handleSubscribe(packet *packets.SubscribePacket) {
	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID

	subRetains := make(map[*subscription][]*packets.PublishPacket)
	for i, origin := range packet.Topics {
		topic := origin
		qos := packet.Qoss[i]
		if !validTopic(topic, s.peer, false) {
			fmt.Printf("[sub] invalid topic: %s, client id: %s\n", topic, s.clientID)
			suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
			continue
		}
		if !validQoS(qos) {
			fmt.Printf("[sub] invalid qos %d, client id: %s\n", qos, s.clientID)
			suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
			continue
		}
		if qos > QosMax {
			qos = QosMax
		}

		if !s.peer {
			// error or false returned is different, so finish it later
			if !s.broker.checkTopicAuth(SUB, s.clientID, s.username, s.remoteIP, topic) {
				fmt.Printf("[sub] topic auth failed, topic: %s, client id: %s\n", topic, s.clientID)
				suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
				continue
			}
		}

		// share subscription
		group := ""
		share := false
		if strings.HasPrefix(topic, "$share/") {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
				continue
			}
			share = true
			group = substr[1]
			topic = substr[2]
		}

		// subscription tree
		sub := &subscription{
			router:   s,
			peer:     s.peer,
			peerID:   s.peerID,
			clientID: s.clientID,
			topic:    topic,
			qos:      qos,
			share:    share,
			group:    group,
		}
		if preSub, ok := s.subMap[origin]; ok {
			err := s.broker.topicCore.Swap([]byte(topic), preSub, sub, qos)
			if err != nil {
				fmt.Printf("[sub] swap error: %v, client id: %s\n", err, s.clientID)
				suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
				continue
			}
		} else {
			err := s.broker.topicCore.Subscribe([]byte(topic), sub, qos)
			if err != nil {
				fmt.Printf("[sub] error: %v, client id: %s\n", err, s.clientID)
				suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
				continue
			}
		}
		s.subMap[origin] = sub

		// session
		err := s.session.AddTopic(origin, qos)
		if err != nil {
			fmt.Printf("[sub] session error: %v, client id: %s\n", err, s.clientID)
			suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
			continue
		}

		if !s.peer && !share {
			var retains []*packets.PublishPacket
			err = s.broker.topicCore.Retained([]byte(topic), &retains)
			if err != nil {
				fmt.Printf("[sub] get retained error: %v, client id: %s\n", err, s.clientID)
				suback.ReturnCodes = append(suback.ReturnCodes, QosFailure)
				continue
			}
			subRetains[sub] = retains
		}

		suback.ReturnCodes = append(suback.ReturnCodes, qos)

		if !s.peer {
			s.broker.proxySub(s.id, origin, qos)

			if s.broker.bridgeProvider != nil {
				err := s.broker.bridgeProvider.Publish(&bridge.Elements{
					ClientID:  s.clientID,
					Username:  s.username,
					Action:    bridge.Subscribe,
					Timestamp: time.Now().Unix(),
					Topic:     origin,
				})
				if err != nil {
					fmt.Printf("bridge subscription error: %v\n", err)
				}
			}
		}
	}

	s.ack(suback)

	for sub, retains := range subRetains {
		for _, rt := range retains {
			s.pubRetain(rt, minQos(rt.Qos, sub.qos))
		}
	}
}

func (s *server) handleUnsubscribe(packet *packets.UnsubscribePacket) {
	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	for _, topic := range packet.Topics {
		if topic == "" {
			continue
		}

		if sub, ok := s.subMap[topic]; ok {
			err := s.broker.topicCore.Unsubscribe([]byte(sub.topic), sub)
			if err != nil {
				fmt.Printf("unsubscribe error: %v, client id: %s\n", err, s.clientID)
			} else {
				delete(s.subMap, topic)
			}
		}

		err := s.session.RemoveTopic(topic)
		if err != nil {
			fmt.Printf("remove session topic error: %v, client id: %s\n", err, s.clientID)
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

	s.ack(unsuback)
}

func (s *server) handlePing(_ *packets.PingreqPacket) {
	s.ack(s.broker.pingResp)
}

func (s *server) handleDisconnect(_ *packets.DisconnectPacket) {
	s.will = nil
	s.close()
}
