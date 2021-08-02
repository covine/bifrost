package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	uuid "github.com/satori/go.uuid"

	"github.com/covine/bifrost/broker/ipq"
)

type InflightPacket struct {
	Packet packets.ControlPacket
	Time   time.Time
}

type TopicType int32

const (
	SUBType   TopicType = 0
	UNSUBType TopicType = 1
)

type proxySubUnsub struct {
	topicType TopicType
	qos       byte
}

type mePeer struct {
	pid    string
	id     string
	broker *Broker

	ctx        context.Context
	mu         sync.RWMutex
	connection net.Conn
	status     ConnStatus

	localHost string
	localPort string
	peer      string

	midStore *midStore
	subMap   map[string]byte
	subMapMu sync.RWMutex
	pubMap   map[string]*publish

	workers sync.WaitGroup

	retryInterval time.Duration
	sendTimeout   time.Duration

	ackChan chan packets.ControlPacket

	subUnsubChan    chan packets.ControlPacket
	subUnsubChanLen int
	subUnsubMu      sync.RWMutex

	inflight      ipq.IPQ
	subUnsubQueue ipq.IPQ
}

func newMePeer(ctx context.Context, b *Broker, pid string, bi *brokerInfo) *mePeer {
	mp := &mePeer{
		pid:    pid,
		id:     uuid.NewV4().String(),
		broker: b,

		ctx:        ctx,
		connection: nil,
		status:     ConnStatusInit,

		peer: bi.ClusterHost + ":" + strconv.Itoa(bi.ClusterPort),

		midStore: newMidStore(),
		subMap:   make(map[string]byte),
		pubMap:   make(map[string]*publish),

		retryInterval: time.Duration(b.config.Cluster.Proxy.Retry) * time.Second,
		sendTimeout:   time.Duration(b.config.Cluster.Proxy.SendTimeout) * time.Second,

		ackChan: make(chan packets.ControlPacket, b.config.Cluster.Proxy.AckChan),

		subUnsubChanLen: b.config.Cluster.Proxy.SendSubUnsubChan,
		subUnsubChan:    make(chan packets.ControlPacket, b.config.Cluster.Proxy.SendSubUnsubChan),

		inflight:      ipq.NewMemoryIPQ(b.config.Cluster.Proxy.Inflight, b.onDelPublish),
		subUnsubQueue: ipq.NewMemoryIPQ(b.config.Cluster.Proxy.SubUnsubQueue, b.onDelPublish),
	}

	return mp
}

func (m *mePeer) start() {
	m.workers.Add(3)
	go m.onSend()
	go m.beat()
	go m.connect()
	m.workers.Wait()

	m.midStore.close()
	m.midStore = nil

	m.inflight.Free()
	m.subUnsubQueue.Free()

	m.subMap = nil
	m.pubMap = nil
}

func (m *mePeer) connecting() (ConnStatus, net.Conn) {
	delay := 0
	retry := 0
	for {
		select {
		case <-m.ctx.Done():
			return ConnStatusClosed, nil
		default:
			connection, err := net.Dial("tcp", m.peer)
			if err != nil {
				fmt.Printf("try %d connect me -> peer: %s error: %v\n", retry+1, m.peer, err)

				if delay > m.broker.config.Cluster.PeerConnMaxInterval {
					delay = m.broker.config.Cluster.PeerConnMaxInterval
				} else if delay == 0 {
					delay = 3
				} else {
					delay *= 2
				}

				ticker := time.NewTicker(time.Duration(delay) * time.Second)
				select {
				case <-m.ctx.Done():
					ticker.Stop()
					return ConnStatusClosed, nil
				case <-ticker.C:
					ticker.Stop()
					retry++
					continue
				}
			} else {
				fmt.Printf("try %d connect me -> peer success: %s\n", retry+1, m.peer)
				return ConnStatusConnected, connection
			}
		}
	}
}

func (m *mePeer) connect() {
	defer m.workers.Done()

	for {
		m.mu.Lock()
		if m.status != ConnStatusInit && m.status != ConnStatusBroken {
			m.mu.Unlock()
			break
		}
		m.status = ConnStatusConnecting
		m.mu.Unlock()

		status, connection := m.connecting()

		m.mu.Lock()
		if m.status == ConnStatusConnecting && status == ConnStatusConnected && connection != nil {
			m.connection = connection
			m.localHost, m.localPort, _ = net.SplitHostPort(m.connection.LocalAddr().String())
			if !m.sendConnect() {
				m.status = ConnStatusBroken
				_ = m.connection.Close()
				m.mu.Unlock()
				fmt.Printf("send <connect> to peer error\n")
				time.Sleep(3 * time.Second)
				continue
			}
			m.status = ConnStatusConnected
		} else {
			if connection != nil {
				_ = connection.Close()
			}
			m.status = ConnStatusClosed
			m.mu.Unlock()
			break
		}
		m.mu.Unlock()

		m.afterConnect()

		status = m.read()

		m.mu.Lock()
		if status != ConnStatusBroken {
			m.status = ConnStatusClosed
			_ = m.connection.Close()
			m.mu.Unlock()
			break
		} else {
			m.status = ConnStatusBroken
			_ = m.connection.Close()
			m.mu.Unlock()
			m.afterBroken()
			continue
		}
	}
}

func (m *mePeer) sendConnect() bool {
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName = "MQIsdp"
	p.ProtocolVersion = 3
	p.CleanSession = true
	p.ClientIdentifier = m.pid + ":" + m.id

	if err := p.Write(m.connection); err != nil {
		return false
	}

	ca, err := packets.ReadPacket(m.connection)
	if err != nil {
		return false
	}
	if ca == nil {
		return false
	}

	msg, ok := ca.(*packets.ConnackPacket)
	if !ok {
		return false
	}

	if msg.ReturnCode != packets.Accepted {
		return false
	}

	return true
}

func (m *mePeer) sub(topic string, qos byte) {
	m.subMapMu.Lock()
	m.subMap[topic] = qos
	m.subMapMu.Unlock()

	m.sendSub(topic, qos)
}

func (m *mePeer) unSub(topic string) {
	m.subMapMu.Lock()
	delete(m.subMap, topic)
	m.subMapMu.Unlock()

	m.sendUnsub(topic)
}

func (m *mePeer) afterConnect() {
	m.proxySubs()
}

func (m *mePeer) afterBroken() {
	m.inflight.Reset()
	m.subUnsubQueue.Reset()
	m.midStore.reset()
	m.pubMap = make(map[string]*publish)
}

func (m *mePeer) proxySubs() {
	m.subMapMu.RLock()
	defer m.subMapMu.RUnlock()

	for topic, qos := range m.subMap {
		m.sendSub(topic, qos)
	}
}

func (m *mePeer) onPublish(packet *packets.PublishPacket) {
	group := ""
	share := false
	if isOnOfflineTopic(packet.TopicName) {
		// problem: kick off each other too often
		m.processOnOffLine(packet)
	} else if strings.HasPrefix(packet.TopicName, "$share/") {
		substr := groupCompile.FindStringSubmatch(packet.TopicName)
		if len(substr) != 3 {
			return
		}
		share = true
		group = substr[1]
		packet.TopicName = substr[2]
	}

	pub, ok := m.pubMap[packet.TopicName]
	if ok {
		if pub.version != m.broker.topicCore.Version() {
			if err := syncPubMap(m.broker.topicCore, []byte(packet.TopicName), pub, true); err != nil {
				fmt.Printf("sync topic pub error, me->peer id: %s\n", m.id)
				return
			}
		}
	} else {
		pub = &publish{
			version: 0,
		}
		if err := syncPubMap(m.broker.topicCore, []byte(packet.TopicName), pub, true); err != nil {
			fmt.Printf("error sync topic pub, me->peer id: %s\n", m.id)
			return
		}
		m.pubMap[packet.TopicName] = pub
	}

	if packet.Retain {
		if err := m.broker.topicCore.Retain(packet); err != nil {
			fmt.Printf("error retaining message, error: %v, me->peer id: %s\n", err, m.id)
			return
		}
	}

	m.ackPub(packet)

	if share {
		if s, ok := pub.groups[group]; ok {
			if len(s.subs) > 0 {
				idx := r.Intn(len(s.subs))
				s.subs[idx].router.sharePub(packet, minQos(packet.Qos, s.qoss[idx]), group)
			}
		}
	} else {
		for _, s := range pub.clients {
			s.sub.router.pub(packet, minQos(packet.Qos, s.qos))
		}
	}
}

func (m *mePeer) onSuback(packet *packets.SubackPacket) {
	if !validQoS(packet.Qos) {
		p := m.inflight.Get(packet.MessageID)
		if fp, ok := p.(*flyPacket); ok {
			if s, o := fp.Packet.(*packets.SubscribePacket); o {
				for _, t := range s.Topics {
					fmt.Printf("me->peer subscribe failed: %s\n", t)
					m.subMapMu.RLock()
					if qos, exists := m.subMap[t]; exists {
						m.sendSub(t, qos)
					}
					m.subMapMu.RUnlock()
				}
			}
		}
	}
	m.inflight.Del(packet.MessageID)
	m.midStore.free(packet.MessageID)
}

func (m *mePeer) onUnsuback(packet *packets.UnsubackPacket) {
	// fmt.Printf("free msg id on me->peer unsuback: %d\n", packet.MessageID)
	m.inflight.Del(packet.MessageID)
	m.midStore.free(packet.MessageID)
}

func (m *mePeer) processOnOffLine(packet *packets.PublishPacket) {
	onOff := &onOffLine{}
	if err := json.Unmarshal(packet.Payload, onOff); err != nil {
		return
	}
	// fmt.Printf("kick out client id: %s\n", clientID)
	m.broker.closeClient(onOff.ClientID)
}

func (m *mePeer) sendSub(topic string, qos byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status != ConnStatusConnected {
		return
	}

	m.subUnsubMu.Lock()
	defer m.subUnsubMu.Unlock()

	if len(m.subUnsubChan) < (m.subUnsubChanLen/7*6) && m.inflight.SpaceLeft() {
		p := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
		p.Topics = append(p.Topics, topic)
		p.Qoss = append(p.Qoss, qos)
		p.MessageID = m.midStore.alloc()
		select {
		case <-m.ctx.Done():
		case m.subUnsubChan <- p:
			fp := &flyPacket{
				Packet: p,
				Time:   time.Now(),
			}
			m.inflight.PushBack(p.MessageID, fp, false, false)
		}
	} else {
		m.subUnsubQueue.PushBack(topic, &proxySubUnsub{
			topicType: SUBType,
			qos:       qos,
		}, false, true)
		if !m.subUnsubQueue.SpaceLeft() {
			_ = m.connection.Close()
		}
	}
}

func (m *mePeer) sendUnsub(topic string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status != ConnStatusConnected {
		return
	}

	m.subUnsubMu.Lock()
	defer m.subUnsubMu.Unlock()

	if len(m.subUnsubChan) < (m.subUnsubChanLen/7*6) && m.inflight.SpaceLeft() {
		p := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
		p.Topics = append(p.Topics, topic)
		p.MessageID = m.midStore.alloc()
		select {
		case <-m.ctx.Done():
		case m.subUnsubChan <- p:
			fp := &flyPacket{
				Packet: p,
				Time:   time.Now(),
			}
			m.inflight.PushBack(p.MessageID, fp, false, false)
		}
	} else {
		m.subUnsubQueue.PushBack(topic, &proxySubUnsub{
			topicType: UNSUBType,
		}, false, true)
		if !m.subUnsubQueue.SpaceLeft() {
			_ = m.connection.Close()
		}
	}
}

func (m *mePeer) write(packet packets.ControlPacket) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status != ConnStatusConnected {
		return
	}

	if m.sendTimeout > 0 {
		if err := m.connection.SetWriteDeadline(time.Now().Add(m.sendTimeout)); err != nil {
			_ = m.connection.Close()
			return
		}
	}

	if err := packet.Write(m.connection); err != nil {
		fmt.Printf("me->peer send packet error: %v\n", err)
		_ = m.connection.Close()
		return
	}

	switch msg := packet.(type) {
	case *packets.SubscribePacket:
		p := m.inflight.Get(msg.MessageID)
		if fp, ok := p.(*flyPacket); ok {
			fp.Time = time.Now()
		}
	case *packets.UnsubscribePacket:
		p := m.inflight.Get(msg.MessageID)
		if fp, ok := p.(*flyPacket); ok {
			fp.Time = time.Now()
		}
	}

	if m.sendTimeout > 0 {
		if err := m.connection.SetWriteDeadline(time.Time{}); err != nil {
			_ = m.connection.Close()
			return
		}
	}
}

func (m *mePeer) ackPub(packet *packets.PublishPacket) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status != ConnStatusConnected {
		return
	}

	switch packet.Qos {
	case 2:
	case 1:
		pa := m.broker.newPuback()
		pa.MessageID = packet.MessageID
		select {
		case <-m.ctx.Done():
		case m.ackChan <- pa:
		}
	case 0:
	}
}

func (m *mePeer) ack(p packets.ControlPacket) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status != ConnStatusConnected {
		return
	}

	select {
	case <-m.ctx.Done():
	case m.ackChan <- p:
	}
}

/*
type grpcClient struct {
	broker *Broker
	//peerInfo *peerInfo
	status ConnStatus

	conn   *grpc.ClientConn
	client proto.bifrostGrpcClient

	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.RWMutex
}

func newGrpcClient(broker *Broker, pi *peerInfo) *grpcClient {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &grpcClient{
		broker: broker,
		//peerInfo:   pi,
		status:     ConnStatusInit,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

func (g *grpcClient) connecting() (ConnStatus, *grpc.ClientConn) {
	addr := fmt.Sprintf("%s:%d", g.peerInfo.ClusterHost, g.peerInfo.RpcPort)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())

	delay := 0
	retry := 0
	for {
		select {
		case <-g.ctx.Done():
			return ConnStatusClosed, nil
		default:
			ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
			connection, err := grpc.DialContext(ctx, addr, opts...)
			cancel()
			if err != nil {
				fmt.Printf("try %d connect grpc me -> peer: %s error: %v\n", retry+1, addr, err)

				if delay > 30 {
					delay = 30
				} else if delay == 0 {
					delay = 1
				} else {
					delay *= 2
				}
				time.Sleep(time.Duration(delay) * time.Second)
				retry++
			} else {
				fmt.Printf("try %d connect grpc me -> peer success: %s\n", retry+1, addr)
				return ConnStatusConnected, connection
			}
		}
	}
}

func (g *grpcClient) connect() {
	g.mu.Lock()
	if g.status != ConnStatusInit {
		g.mu.Unlock()
		return
	}
	g.status = ConnStatusConnecting
	g.mu.Unlock()

	status, connection := g.connecting()

	g.mu.Lock()
	defer g.mu.Unlock()
	if status != ConnStatusConnected || g.status != ConnStatusConnecting || connection == nil {
		if connection != nil {
			_ = connection.Close()
		}
		g.status = ConnStatusClosed
		return
	} else {
		g.status = ConnStatusConnected
		g.conn = connection
		g.client = proto.NewbifrostGrpcClient(connection)
	}
}

func (g *grpcClient) updatePeerID(id string) {
	g.peerInfo.ID = id
}

func (g *grpcClient) close() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.cancelFunc()

	if g.conn != nil {
		_ = g.conn.Close()
	}
}

// ------------- TODO review
func (g *grpcClient) Ping(ctx context.Context, request *proto.PingRequest) (*proto.PingResponse, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.status == ConnStatusConnected && g.client != nil && g.conn != nil {
		return g.client.Ping(ctx, request)
	} else {
		return nil, errors.New("invalid grpc server")
	}
}

func (g *grpcClient) KickConn(ctx context.Context, request *proto.KickConnRequest) (*proto.KickConnResponse, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.status == ConnStatusConnected && g.client != nil && g.conn != nil {
		return g.client.KickConn(ctx, request)
	} else {
		return nil, errors.New("invalid grpc server")
	}
}

*/
