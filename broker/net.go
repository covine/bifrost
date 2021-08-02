package broker

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/net/websocket"
	"google.golang.org/grpc"

	"github.com/covine/bifrost/pb"
)

func (b *Broker) genTLSConfig(tlsInfo TLS) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(tlsInfo.CertPem, tlsInfo.KeyPem)
	if err != nil {
		return nil, err
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}

	config := tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if tlsInfo.Verify {
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if tlsInfo.CaPem != "" {
		rootPEM, err := ioutil.ReadFile(tlsInfo.CaPem)
		if err != nil || rootPEM == nil {
			return nil, err
		}
		pl := x509.NewCertPool()
		ok := pl.AppendCertsFromPEM(rootPEM)
		if !ok {
			return nil, errors.New("failed to parse root ca certificate")
		}
		config.ClientCAs = pl
	}

	return &config, nil
}

func (b *Broker) listenTCP(security bool) error {
	var err error
	var l net.Listener

	if security {
		tlsConfig, err := b.genTLSConfig(b.config.Serve.TLS)
		if err != nil {
			return err
		}
		address := b.config.Serve.TLS.Host + ":" + strconv.Itoa(b.config.Serve.TLS.Port)
		fmt.Printf("Start [  TLS  ] Listening on: %s\n", address)
		l, err = tls.Listen("tcp", address, tlsConfig)
	} else {
		address := b.config.Serve.TCP.Host + ":" + strconv.Itoa(b.config.Serve.TCP.Port)
		fmt.Printf("Start [  TCP  ] Listening on: %s\n", address)
		l, err = net.Listen("tcp", address)
	}
	if err != nil {
		return err
	}

	var delay time.Duration
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
				}
				if max := 1 * time.Second; delay > max {
					delay = max
				}
				fmt.Printf("http: Accept error: %v; retrying in %v\n", err, delay)
				time.Sleep(delay)
				continue
			}
			return err
		}
		delay = 0

		go b.serve(conn, ConnTypClient)
	}
}

func (b *Broker) listenCluster() error {
	address := b.config.Cluster.Host + ":" + strconv.Itoa(b.config.Cluster.TcpPort)
	fmt.Printf("Start [Cluster] Listening on: %s\n", address)

	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	var delay time.Duration
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
				}
				if max := 1 * time.Second; delay > max {
					delay = max
				}
				fmt.Printf("http: Accept error: %v; retrying in %v\n", err, delay)
				time.Sleep(delay)
				continue
			}
			return err
		}
		delay = 0

		go b.serve(conn, ConnTypPeer)
	}
}

func (b *Broker) wsHandler(ws *websocket.Conn) {
	ws.PayloadType = websocket.BinaryFrame
	b.serve(ws, ConnTypClient)
}

func (b *Broker) listenWebsocket(security bool) error {
	address := ""
	path := ""
	if security {
		address = b.config.Serve.WSS.Host + ":" + strconv.Itoa(b.config.Serve.WSS.Port)
		path = b.config.Serve.WSS.Path
		fmt.Printf("Start [  WSS  ] Listening on: %s%s\n", address, path)
	} else {
		address = b.config.Serve.WS.Host + ":" + strconv.Itoa(b.config.Serve.WS.Port)
		path = b.config.Serve.WS.Path
		fmt.Printf("Start [  WS   ] Listening on: %s%s\n", address, path)
	}

	mux := http.NewServeMux()
	mux.Handle(path, &websocket.Server{Handler: b.wsHandler})
	if security {
		return http.ListenAndServeTLS(address, b.config.Serve.WSS.CertPem, b.config.Serve.WSS.KeyPem, mux)
	} else {
		return http.ListenAndServe(address, mux)
	}
}

func (b *Broker) ping(w http.ResponseWriter, _ *http.Request) {
	_, _ = fmt.Fprintf(w, "pong\n")
}

func (b *Broker) listenHTTP() error {
	http.HandleFunc("/ping", b.ping)
	address := b.config.Serve.HTTP.Host + ":" + strconv.Itoa(b.config.Serve.HTTP.Port)
	fmt.Printf("Start [  HTTP ] Listening on: %s\n", address)
	return http.ListenAndServe(address, nil)
}

func (b *Broker) listenGRPC() error {
	address := b.config.Serve.GRPC.Host + ":" + strconv.Itoa(b.config.Serve.GRPC.Port)
	fmt.Printf("Start [  GRPC ] Listening on: %s\n", address)

	g, err := newGrpcServer(b)
	if err != nil {
		return err
	}

	s := grpc.NewServer(grpc.MaxConcurrentStreams(math.MaxUint32))
	proto.RegisterBifrostGrpcServer(s, g)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	// block here
	err = s.Serve(lis)
	if err != nil {
		return err
	}

	return nil
}
