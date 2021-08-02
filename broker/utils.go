package broker

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type ConnStatus int32

const (
	ConnStatusInit       ConnStatus = 0
	ConnStatusConnecting ConnStatus = 1
	ConnStatusConnected  ConnStatus = 2
	ConnStatusBroken     ConnStatus = 3
	ConnStatusClosed     ConnStatus = 4
)

type ConnType int32

const (
	ConnTypClient ConnType = 0
	ConnTypPeer   ConnType = 1
)

type brokerInfo struct {
	ID          string `json:"id"`
	ClusterHost string `json:"clusterHost"`
	ClusterPort int    `json:"clusterPort"`

	ClientCount uint64 `json:"clientCount"`
	ProxyCount  uint64 `json:"proxyCount"`
	Proxies     int    `json:"proxies"`

	Internal string `json:"internal"`
	External string `json:"external"`

	HttpPort int    `json:"httpPort"`
	TcpPort  int    `json:"tcpPort"`
	TlsPort  int    `json:"tlsPort"`
	WsPort   int    `json:"wsPort"`
	WsPath   string `json:"wsPath"`
	WssPort  int    `json:"wssPort"`
	WssPath  string `json:"wssPath"`
}

type iRouter interface {
	pub(p *packets.PublishPacket, qos byte)
	pubRetain(p *packets.PublishPacket, qos byte)
	sharePub(p *packets.PublishPacket, qos byte, group string)
}

type subscription struct {
	router   iRouter
	peer     bool
	peerID   string
	clientID string
	topic    string
	qos      byte
	share    bool
	group    string
}

const (
	_GroupTopicRegexp = `^\$share/([0-9a-zA-Z_-]+)/(.*)$`
)

var (
	r            = rand.New(rand.NewSource(time.Now().UnixNano()))
	groupCompile = regexp.MustCompile(_GroupTopicRegexp)
)

const (
	QosAtMostOnce  = 0x00
	QosAtLeastOnce = 0x01
	QosExactlyOnce = 0x02
	QosFailure     = 0x80
	QosMax         = 0x01
)

type onOffLine struct {
	BrokerID  string `json:"brokerID"`
	ClientID  string `json:"clientID"`
	IP        string `json:"ip"`
	UserName  string `json:"username"`
	Online    bool   `json:"online"`
	Timestamp string `json:"timestamp"`
}

type subList struct {
	subs []*subscription
	qoss []byte
}

type subOne struct {
	sub *subscription
	qos byte
}

type publish struct {
	version uint64
	clients map[string]*subOne
	peers   map[string]*subOne
	groups  map[string]*subList
}

type qosConns struct {
	qos   byte
	conns map[string]byte
}

type flyPacket struct {
	Packet packets.ControlPacket
	Time   time.Time
}

func preNum(data byte) int {
	var mask byte = 0x80
	var num = 0
	for i := 0; i < 8; i++ {
		if (data & mask) == mask {
			num++
			mask = mask >> 1
		} else {
			break
		}
	}
	return num
}

func isUTF8(data []byte) bool {
	i := 0
	for i < len(data) {
		if data[i] == 0x00 {
			// should not contain null
			return false
		} else if (data[i] & 0x80) == 0x00 {
			// 0XXX_XXXX
			i++
			continue
		} else if num := preNum(data[i]); num > 2 {
			// 110X_XXXX 10XX_XXXX
			// 1110_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_0XXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_10XX 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_110X 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			i++
			for j := 0; j < num-1; j++ {
				if (data[i] & 0xc0) != 0x80 {
					return false
				}
				i++
			}
		} else {
			return false
		}
	}
	return true
}

func syncPubMap(c iTopicsCore, topic []byte, pub *publish, isMePeer bool) error {
	var subs []*subscription
	var qoss []byte
	version, err := c.Subscribers(topic, &subs, &qoss)
	if err != nil {
		return err
	}

	pub.groups = make(map[string]*subList)
	pub.peers = make(map[string]*subOne)
	pub.clients = make(map[string]*subOne)

	if isMePeer {
		// for <mePeer> object
		for i, sub := range subs {
			if sub.peer {
				continue
			} else if sub.share {
				if s, ok := pub.groups[sub.group]; ok {
					s.subs = append(s.subs, sub)
					s.qoss = append(s.qoss, qoss[i])
				} else {
					s := &subList{
						subs: make([]*subscription, 0),
						qoss: make([]byte, 0),
					}
					s.subs = append(s.subs, sub)
					s.qoss = append(s.qoss, qoss[i])
					pub.groups[sub.group] = s
				}
			} else {
				if s, ok := pub.clients[sub.clientID]; ok {
					if qoss[i] > s.qos {
						s.sub = sub
						s.qos = qoss[i]
					}
				} else {
					s := &subOne{
						sub: sub,
						qos: qoss[i],
					}
					pub.clients[sub.clientID] = s
				}
			}
		}
	} else {
		// for <server> object
		for i, sub := range subs {
			if sub.share {
				if s, ok := pub.groups[sub.group]; ok {
					s.subs = append(s.subs, sub)
					s.qoss = append(s.qoss, qoss[i])
				} else {
					s := &subList{
						subs: make([]*subscription, 0),
						qoss: make([]byte, 0),
					}
					s.subs = append(s.subs, sub)
					s.qoss = append(s.qoss, qoss[i])
					pub.groups[sub.group] = s
				}
			} else if sub.peer {
				if s, ok := pub.peers[sub.peerID]; ok {
					if qoss[i] > s.qos {
						s.sub = sub
						s.qos = qoss[i]
					}
				} else {
					s := &subOne{
						sub: sub,
						qos: qoss[i],
					}
					pub.peers[sub.peerID] = s
				}
			} else {
				if s, ok := pub.clients[sub.clientID]; ok {
					if qoss[i] > s.qos {
						s.sub = sub
						s.qos = qoss[i]
					}
				} else {
					s := &subOne{
						sub: sub,
						qos: qoss[i],
					}
					pub.clients[sub.clientID] = s
				}
			}
		}
	}

	pub.version = version
	return nil
}

func minQos(a byte, b byte) byte {
	m := a
	if b < m {
		m = b
	}
	return m
}

func validTopic(topic string, isPeer bool, isPub bool) bool {
	if isPeer {
		return true
	} else {
		if isPub {
			return len(topic) > 0 &&
				len(topic) <= 65535 &&
				isUTF8([]byte(topic)) &&
				!strings.HasPrefix(topic, "$") &&
				!strings.Contains(topic, "#") &&
				!strings.Contains(topic, "+")
		} else {
			return len(topic) > 0 &&
				len(topic) <= 65535 &&
				isUTF8([]byte(topic))
		}
	}
}

func validQoS(qos byte) bool {
	return qos == QosAtMostOnce || qos == QosAtLeastOnce || qos == QosExactlyOnce
}

func onlineMSub() string {
	return "$SYS/brokers/+/clients/+/connected"
}

func onOfflineTopic(brokerID string, clientID string, online bool) string {
	if online {
		return fmt.Sprintf("$SYS/brokers/%s/clients/%s/connected", brokerID, clientID)
	} else {
		return fmt.Sprintf("$SYS/brokers/%s/clients/%s/disconnected", brokerID, clientID)
	}
}

func isOnOfflineTopic(topic string) bool {
	re := regexp.MustCompile(`^\$SYS/brokers/[0-9a-zA-Z_-]+/clients/[0-9a-zA-Z_-]+/(connected|disconnected)$`)
	return re.MatchString(topic)
}
