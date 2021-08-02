package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/satori/go.uuid"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/covine/bifrost/broker/ipq"
	"github.com/covine/bifrost/broker/session"
	"github.com/covine/bifrost/plugins/auth"
	"github.com/covine/bifrost/plugins/bridge"
)

type Broker struct {
	info   *brokerInfo
	config *Config

	clients    map[string]*server
	clientsMu  sync.Mutex
	proxyMes   map[string]*server
	proxyMesMu sync.Mutex

	proxies   map[string]*proxy
	proxiesMu sync.RWMutex

	sessionStore   session.IStore
	topicCore      iTopicsCore
	auth           auth.Auth
	bridgeProvider bridge.Bridge

	etcd *clientv3.Client

	subMap   map[string]*qosConns
	subMapMu sync.RWMutex

	pingResp packets.ControlPacket

	connectPool     *sync.Pool
	connackPool     *sync.Pool
	publishPool     *sync.Pool
	pubackPool      *sync.Pool
	subscribePool   *sync.Pool
	subackPool      *sync.Pool
	unsubscribePool *sync.Pool
	unsubackPool    *sync.Pool
	pingreqPool     *sync.Pool
	pingrespPool    *sync.Pool
	disconnectPool  *sync.Pool

	// settings
	connectWaitTime time.Duration
}

func NewBroker(config *Config) (*Broker, error) {
	b := &Broker{
		info:      &brokerInfo{},
		config:    config,
		clients:   make(map[string]*server),
		proxyMes:  make(map[string]*server),
		proxies:   make(map[string]*proxy),
		topicCore: newMemoryCore(),
		subMap:    make(map[string]*qosConns),

		connectPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Connect)
			},
		},
		connackPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Connack)
			},
		},
		publishPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Publish)
			},
		},
		pubackPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Puback)
			},
		},
		subscribePool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Subscribe)
			},
		},
		subackPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Suback)
			},
		},
		unsubscribePool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Unsubscribe)
			},
		},
		unsubackPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Unsuback)
			},
		},
		pingreqPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Pingreq)
			},
		},
		pingrespPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Pingresp)
			},
		},
		disconnectPool: &sync.Pool{
			New: func() interface{} {
				return packets.NewControlPacket(packets.Disconnect)
			},
		},
		pingResp: packets.NewControlPacket(packets.Pingresp),
		// settings
		connectWaitTime: time.Duration(config.Client.WaitConnect) * time.Second,
	}

	b.sessionStore = session.NewMemoryStore(&session.Config{
		ZQueueLen:      config.Session.Client.ZQueue,
		OTQueueLen:     config.Session.Client.OTQueue,
		Inflight:       config.Session.Client.Inflight,
		PeerZQueueLen:  config.Session.Peer.ZQueue,
		PeerOTQueueLen: config.Session.Peer.OTQueue,
		PeerInflight:   config.Session.Peer.Inflight,
	}, b.putPublish, b.onDelPublish)

	var err error

	if len(b.config.Bridge.Name) > 0 {
		switch b.config.Bridge.Name {
		case "kafka":
			deliverMap := make(map[string]string)
			for _, v := range b.config.Bridge.Kafka.DeliverMap {
				deliverMap[v.Key] = v.Value
			}
			b.bridgeProvider, err = bridge.NewKafka(&bridge.KafkaConfig{
				Addr:             b.config.Bridge.Kafka.Endpoints,
				ConnectTopic:     b.config.Bridge.Kafka.OnConnect,
				SubscribeTopic:   b.config.Bridge.Kafka.OnSubscribe,
				PublishTopic:     b.config.Bridge.Kafka.OnPublish,
				UnsubscribeTopic: b.config.Bridge.Kafka.OnUnsubscribe,
				DisconnectTopic:  b.config.Bridge.Kafka.OnDisconnect,
				DeliverMap:       deliverMap,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if b.config.Cluster.Enable {
		if len(b.config.Cluster.External) == 0 {
			return nil, errors.New("require cluster external host")
		} else {
			b.info.External = b.config.Cluster.External
		}
		b.info.TcpPort = b.config.Serve.TCP.Port
		b.info.HttpPort = b.config.Serve.HTTP.Port
		b.info.TlsPort = b.config.Serve.TLS.Port
		b.info.WsPort = b.config.Serve.WS.Port
		b.info.WsPath = b.config.Serve.WS.Path
		b.info.WssPort = b.config.Serve.WSS.Port
		b.info.WssPath = b.config.Serve.WSS.Path

		if b.config.Cluster.TcpPort == 0 {
			return nil, errors.New("require cluster port")
		} else {
			b.info.ClusterPort = b.config.Cluster.TcpPort
		}

		if b.config.Cluster.Host == "" || b.config.Cluster.Host == "0.0.0.0" {
			addresses, err := net.InterfaceAddrs()
			if err != nil {
				return nil, err
			}
			for _, address := range addresses {
				if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
					if ipNet.IP.To4() != nil {
						b.info.ClusterHost = ipNet.IP.String()
						break
					}
				}
			}
		} else {
			b.info.ClusterHost = b.config.Cluster.Host
		}
		if b.info.ClusterHost == "0.0.0.0" || b.info.ClusterHost == "" {
			return nil, errors.New("can not get local ip")
		}
		b.info.Internal = b.info.ClusterHost
		b.info.ID = uuid.NewV4().String()

		// etcd3 for cluster
		b.etcd, err = clientv3.New(clientv3.Config{
			Endpoints:   b.config.Cluster.Etcd.Endpoints,
			DialTimeout: 3 * time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithBlock(),
			},
		})
		if err != nil {
			fmt.Printf("connect to etcd error: %v\n", err)
			return nil, err
		}
		fmt.Printf("connect to <etcd> success\n")
	}

	return b, nil
}

func (b *Broker) Start() error {
	go func() {
		err := b.listenGRPC()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
			return
		}
	}()

	go func() {
		err := b.listenHTTP()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
			return
		}
	}()

	if b.config.Serve.TCP.Enable {
		go func() {
			err := b.listenTCP(false)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
				return
			}
		}()
	}
	if b.config.Serve.TLS.Enable {
		go func() {
			err := b.listenTCP(true)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
				return
			}
		}()
	}
	if b.config.Serve.WS.Enable {
		go func() {
			err := b.listenWebsocket(false)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}()
	}
	if b.config.Serve.WSS.Enable {
		go func() {
			err := b.listenWebsocket(true)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}()
	}

	// cluster
	if b.config.Cluster.Enable {
		go func() {
			err := b.listenCluster()
			if err != nil {
				log.Fatal(err.Error())
			}
		}()

		// keep node alive in etcd
		err := b.keep()
		if err != nil {
			return err
		}
		// brokers change notification from etcd
		go b.watchBrokers()
		// keep brokers connections
		go b.keepBrokers()
	}

	if b.config.Mode.Debug {
		go b.printf()
	}
	return nil
}

func (b *Broker) publishOnOffLine(id string, ip string, username string, online bool) {
	p := b.newPublish()
	p.TopicName = onOfflineTopic(b.info.ID, id, online)
	p.Qos = 0
	payload := onOffLine{
		BrokerID:  b.info.ID,
		ClientID:  id,
		IP:        ip,
		UserName:  username,
		Online:    online,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return
	}
	p.Payload = bytes
	b.publish(p)
}

func (b *Broker) publish(packet *packets.PublishPacket) {
	if packet.Retain {
		if err := b.topicCore.Retain(packet); err != nil {
			fmt.Printf("error retaining broker message, error: %v\n", err)
			return
		}
	}

	pub := &publish{
		version: 0,
	}
	if err := syncPubMap(b.topicCore, []byte(packet.TopicName), pub, false); err != nil {
		fmt.Printf("sync topic pub error, broker id: %s, topic: %s\n", b.info.ID, packet.TopicName)
		return
	}

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

	if !packet.Retain {
		b.putPublish(packet)
	}
}

func (b *Broker) closeClient(clientID string) {
	b.clientsMu.Lock()
	c, ok := b.clients[clientID]
	if ok {
		delete(b.clients, clientID)
		b.clientsMu.Unlock()
		c.close()
		return
	}
	b.clientsMu.Unlock()
}

func (b *Broker) syncSubs(p *proxy) {
	b.subMapMu.RLock()
	defer b.subMapMu.RUnlock()

	p.sub(onlineMSub(), 0)

	for topic, qc := range b.subMap {
		p.sub(topic, qc.qos)
	}
}

func (b *Broker) proxySub(id string, topic string, qos byte) {
	b.subMapMu.Lock()
	defer b.subMapMu.Unlock()

	if qc, ok := b.subMap[topic]; ok {
		if qc.qos >= qos && len(qc.conns) > 0 {
			qc.conns[id] = qos
			return
		}
		qc.qos = qos
		if qc.conns == nil {
			qc.conns = make(map[string]byte)
		}
		qc.conns[id] = qos
	} else {
		b.subMap[topic] = &qosConns{
			qos:   qos,
			conns: make(map[string]byte),
		}
		b.subMap[topic].conns[id] = qos
	}

	b.proxiesMu.RLock()
	defer b.proxiesMu.RUnlock()
	for _, p := range b.proxies {
		p.sub(topic, qos)
	}
}

func (b *Broker) proxyUnSub(id string, topic string) {
	b.subMapMu.Lock()
	defer b.subMapMu.Unlock()

	if qc, ok := b.subMap[topic]; ok {
		if connQos, o := qc.conns[id]; o {
			delete(qc.conns, id)

			if len(qc.conns) <= 0 {
				delete(b.subMap, topic)

				b.proxiesMu.RLock()
				for _, p := range b.proxies {
					p.unSub(topic)
				}
				b.proxiesMu.RUnlock()
				return
			} else {
				if connQos < qc.qos {
					return
				}

				var maxQos byte
				for _, qos := range qc.conns {
					if maxQos < qos {
						maxQos = qos
					}
				}
				if maxQos < qc.qos {
					qc.qos = maxQos
					b.proxiesMu.RLock()
					for _, p := range b.proxies {
						p.sub(topic, qc.qos)
					}
					b.proxiesMu.RUnlock()
					return
				}
			}
		} else {
			return
		}
	} else {
		return
	}
}

func (b *Broker) printf() {
	for {
		fmt.Printf("client conns: %d, proxy conns: %d, proxies: %d, goroutines: %d\n",
			atomic.LoadUint64(&b.info.ClientCount), atomic.LoadUint64(&b.info.ProxyCount), len(b.proxies), runtime.NumGoroutine())
		time.Sleep(3 * time.Second)
	}
}

func (b *Broker) onDelPublish(i interface{}) {
	_ = i.(*ipq.Item).Value.(*flyPacket)
	// FIXME
	// b.putPublish(fp.Packet.(*packets.PublishPacket))
}

func (b *Broker) Close() {
}
