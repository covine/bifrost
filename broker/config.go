package broker

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/pelletier/go-toml"
)

type Owner struct {
	Name    string
	Created time.Time
	Updated time.Time
}

type GRPC struct {
	Host string
	Port int
}

type HTTP struct {
	Host string
	Port int
}

type TCP struct {
	Enable bool
	Host   string
	Port   int
}

type Connection struct {
	Max uint64
}

type TLS struct {
	Enable  bool
	Host    string
	Port    int
	Verify  bool
	CaPem   string
	CertPem string
	KeyPem  string
}

type WS struct {
	Enable bool
	Host   string
	Port   int
	Path   string
}

type WSS struct {
	Enable  bool
	Host    string
	Port    int
	Path    string
	CaPem   string
	CertPem string
	KeyPem  string
}

type Serve struct {
	GRPC GRPC
	HTTP HTTP
	TCP  TCP
	TLS  TLS
	WS   WS
	WSS  WSS
}

type Client struct {
	Retry       int
	Reset       int
	WaitConnect int
	Connection  Connection
}

type Peer struct {
	Reset int
}

type Etcd struct {
	Endpoints []string
	Timeout   int64
}

type DeliverMap struct {
	Key   string
	Value string
}

type Kafka struct {
	Endpoints     []string
	OnConnect     string
	OnPublish     string
	OnSubscribe   string
	OnDisconnect  string
	OnUnsubscribe string
	DeliverMap    []DeliverMap
}

type Bridge struct {
	Name  string
	Kafka Kafka
}

type Proxy struct {
	PoolSize         int
	Retry            int
	SendTimeout      int
	AckChan          int
	SendSubUnsubChan int
	Inflight         int
	SubUnsubQueue    int
}

type Cluster struct {
	Enable              bool
	External            string
	Host                string
	TcpPort             int
	LeaseKeyPrefix      string
	Lease               int64
	RenewLeaseInterval  int64
	SyncBrokersInterval int64
	PeerConnMaxInterval int
	Etcd                Etcd
	Proxy               Proxy
}

type Mode struct {
	Debug bool
}

type SessionConfig struct {
	ZQueue   int
	OTQueue  int
	Inflight int
}

type Session struct {
	Client SessionConfig
	Peer   SessionConfig
}

type SenderConfig struct {
	AckChan int
	PubChan int
}

type Sender struct {
	Retry   int
	Timeout int
	Client  SenderConfig
	Peer    SenderConfig
}

type BConfig struct {
	Reset int
}

type ReceiverConfig struct {
	RecvChan int
}

type Receiver struct {
	Client ReceiverConfig
	Peer   ReceiverConfig
}

type Config struct {
	Title    string
	Owner    Owner
	Mode     Mode
	Broker   BConfig
	Serve    Serve
	Client   Client
	Peer     Peer
	Cluster  Cluster
	Session  Session
	Receiver Receiver
	Sender   Sender
	Bridge   Bridge
}

func usage() {
	fmt.Printf("%s\n", `
Usage: bifrost [options]

Broker Options:
	-c,  --config <file>              Configuration file path

Common Options:
	-h, --help                        Show this message
`)
	os.Exit(0)
}

func Configure(args []string) (*Config, error) {
	config := &Config{}
	var cf string

	fs := flag.NewFlagSet("bifrost", flag.ExitOnError)
	fs.StringVar(&cf, "config", "", "config file path.")
	fs.StringVar(&cf, "c", "", "config file path.")
	fs.Usage = usage
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if len(cf) <= 0 {
		return nil, errors.New("require config file")
	}
	content, err := ioutil.ReadFile(cf)
	if err != nil {
		return nil, err
	}
	err = toml.Unmarshal(content, config)
	if err != nil {
		return nil, err
	}

	if err := config.check(); err != nil {
		return nil, err
	}

	return config, nil
}

func (config *Config) check() error {
	if config.Serve.TCP.Enable {
		if config.Serve.TCP.Host == "" {
			config.Serve.TCP.Host = "0.0.0.0"
		}
		if config.Serve.TCP.Port == 0 {
			return errors.New("require tcp port")
		}
	}

	if config.Serve.TLS.Enable {
		if config.Serve.TLS.Host == "" {
			config.Serve.TLS.Host = "0.0.0.0"
		}
		if config.Serve.TLS.Port == 0 {
			return errors.New("require tcp security port")
		}
	}

	if config.Serve.WS.Enable {
		if config.Serve.WS.Host == "" {
			config.Serve.WS.Host = "0.0.0.0"
		}
		if config.Serve.WS.Port == 0 {
			return errors.New("require websocket port")
		}
	}

	if config.Serve.WSS.Enable {
		if config.Serve.WSS.Host == "" {
			config.Serve.WSS.Host = "0.0.0.0"
		}
		if config.Serve.WSS.Port == 0 {
			return errors.New("require websocket security port")
		}
	}

	return nil
}
