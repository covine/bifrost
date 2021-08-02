module github.com/covine/bifrost

go 1.15

replace (
	go.etcd.io/etcd/api/v3 v3.5.0-pre => go.etcd.io/etcd/api/v3 v3.0.0-20201125202132-28d1af294e43
	go.etcd.io/etcd/client/v3 v3.5.0-pre => go.etcd.io/etcd/client/v3 v3.0.0-20201125202132-28d1af294e43
	go.etcd.io/etcd/pkg/v3 v3.5.0-pre => go.etcd.io/etcd/pkg/v3 v3.0.0-20201125202132-28d1af294e43
	google.golang.org/grpc v1.33.2 => google.golang.org/grpc v1.29.1
)

require (
	github.com/Shopify/sarama v1.27.2
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/golang/protobuf v1.3.5
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pelletier/go-toml v1.8.1
	github.com/satori/go.uuid v1.2.0
	go.etcd.io/etcd/api/v3 v3.5.0-pre
	go.etcd.io/etcd/client/v3 v3.5.0-pre
	go.opentelemetry.io/otel v0.15.1-0.20210107194535-5ed96e92446d
	go.opentelemetry.io/otel/exporters/stdout v0.15.1-0.20210107194535-5ed96e92446d
	go.opentelemetry.io/otel/sdk v0.15.1-0.20210107194535-5ed96e92446d
	golang.org/x/net v0.0.0-20201110031124-69a78807bb2b
	google.golang.org/grpc v1.33.2
)
