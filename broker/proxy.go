package broker

import (
	"context"
	"hash/fnv"
	"sync/atomic"

	uuid "github.com/satori/go.uuid"
)

const (
	chance = 3
)

type proxy struct {
	ID     string
	chance uint32

	broker *Broker

	poolSize int
	pool     []*mePeer

	ctx    context.Context
	cancel context.CancelFunc

	peerInfo *brokerInfo
}

func newProxy(b *Broker, bi *brokerInfo) *proxy {
	ctx, cancel := context.WithCancel(context.Background())
	p := &proxy{
		ID:       uuid.NewV4().String(),
		chance:   chance,
		broker:   b,
		poolSize: b.config.Cluster.Proxy.PoolSize,
		pool:     make([]*mePeer, 0, b.config.Cluster.Proxy.PoolSize),
		ctx:      ctx,
		cancel:   cancel,
		peerInfo: bi,
	}

	for i := 0; i < p.poolSize; i++ {
		mp := newMePeer(p.ctx, p.broker, p.ID, bi)
		p.pool = append(p.pool, mp)
		go mp.start()
	}

	return p
}

func (p *proxy) updateBrokerID(id string) {
	p.peerInfo.ID = id
}

func (p *proxy) resetChance() {
	atomic.SwapUint32(&p.chance, chance)
}

func (p *proxy) reduceChance() uint32 {
	return atomic.AddUint32(&p.chance, ^uint32(0))
}

func (p *proxy) close() {
	p.cancel()
}

func (p *proxy) sub(topic string, qos byte) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(topic))
	idx := h.Sum64() % uint64(p.poolSize)

	p.pool[idx].sub(topic, qos)
}

func (p *proxy) unSub(topic string) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(topic))
	idx := h.Sum64() % uint64(p.poolSize)

	p.pool[idx].unSub(topic)
}
