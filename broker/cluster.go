package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

func (b *Broker) brokerKey(bi *brokerInfo) string {
	return fmt.Sprintf("%s:%d", bi.ClusterHost, bi.ClusterPort)
}

func (b *Broker) leaseKey(bi *brokerInfo) string {
	// /cluster/node/lease/ClusterHost:clusterPort
	return fmt.Sprintf("%s/%s:%d",
		b.config.Cluster.LeaseKeyPrefix,
		bi.ClusterHost,
		bi.ClusterPort,
	)
}

func (b *Broker) lease() error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(b.config.Cluster.Etcd.Timeout)*time.Second)
	lease, err := b.etcd.Grant(ctx, b.config.Cluster.Lease)
	if err != nil {
		cancel()
		return err
	}
	cancel()

	nodeInfo, err := json.Marshal(b.info)
	if err != nil {
		return err
	}

	ctx, cancel = context.WithTimeout(context.TODO(), time.Duration(b.config.Cluster.Etcd.Timeout)*time.Second)
	_, err = b.etcd.Put(ctx, b.leaseKey(b.info), string(nodeInfo), clientv3.WithLease(lease.ID))
	if err != nil {
		cancel()
		return err
	}
	cancel()

	return nil
}

func (b *Broker) keepAlive() error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(b.config.Cluster.Etcd.Timeout)*time.Second)
	resp, err := b.etcd.Get(ctx, b.leaseKey(b.info))
	if err != nil {
		cancel()
		return err
	}
	cancel()

	if len(resp.Kvs) > 0 {
		pi := brokerInfo{}
		err := json.Unmarshal(resp.Kvs[0].Value, &pi)
		if err != nil {
			return err
		}
		if pi.ID != b.info.ID {
			fmt.Printf("me %s:%d rebooted (%s)\n",
				b.info.ClusterHost,
				b.info.ClusterPort,
				b.info.External,
			)
		}
	}

	return b.lease()
}

func (b *Broker) keep() error {
	err := b.keepAlive()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Duration(b.config.Cluster.RenewLeaseInterval) * time.Second)
	go func(broker *Broker) {
		for {
			select {
			case <-ticker.C:
				// if network changed and then changed back, has some problem
				err := broker.keepAlive()
				if err != nil {
					fmt.Printf("keep node alive error: %v\n", err)
				}
			}
		}
	}(b)

	return nil
}

func (b *Broker) watchBrokers() {
	ch := b.etcd.Watch(context.Background(), b.config.Cluster.LeaseKeyPrefix, clientv3.WithPrefix())
	for c := range ch {
		for _, ev := range c.Events {
			leaseKey := string(ev.Kv.Key)
			if leaseKey == b.leaseKey(b.info) {
				// pass me
				continue
			}

			switch ev.Type {
			case mvccpb.PUT:
				brokerInfo := brokerInfo{}
				err := json.Unmarshal(ev.Kv.Value, &brokerInfo)
				if err != nil {
					fmt.Printf("unmarshal broker node event value error: %v\n", err)
					continue
				}

				b.proxiesMu.Lock()
				b.addProxy(&brokerInfo)
				b.proxiesMu.Unlock()
			case mvccpb.DELETE:
				k := strings.TrimPrefix(leaseKey, b.config.Cluster.LeaseKeyPrefix+"/")

				b.proxiesMu.Lock()
				b.removeProxy(k)
				b.proxiesMu.Unlock()
			default:
				break
			}
		}
	}
}

func (b *Broker) keepBrokers() {
	for {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(b.config.Cluster.Etcd.Timeout)*time.Second)
		res, err := b.etcd.Get(ctx, b.config.Cluster.LeaseKeyPrefix, clientv3.WithPrefix())
		if err != nil {
			cancel()
			fmt.Printf("pull brokers info error: %v\n", err)
			time.Sleep(7 * time.Second)
			continue
		}
		cancel()

		// connect to broker if not connected
		for _, kv := range res.Kvs {
			// pass me
			leaseKey := string(kv.Key)
			if leaseKey == b.leaseKey(b.info) {
				continue
			}

			brokerInfo := brokerInfo{}
			err := json.Unmarshal(kv.Value, &brokerInfo)
			if err != nil {
				fmt.Printf("unmarshal broker node info error: %v\n", err)
				continue
			}

			b.proxiesMu.Lock()
			b.addProxy(&brokerInfo)
			b.proxiesMu.Unlock()
		}

		b.proxiesMu.Lock()
		for k := range b.proxies {
			find := false
			for _, kv := range res.Kvs {
				// pass me
				leaseKey := string(kv.Key)
				if leaseKey == b.leaseKey(b.info) {
					continue
				}

				bi := brokerInfo{}
				err := json.Unmarshal(kv.Value, &bi)
				if err != nil {
					fmt.Printf("unmarshal broker node info error: %v\n", err)
					continue
				}
				if b.brokerKey(&bi) == k {
					find = true
					break
				}
			}
			if !find {
				b.removeProxy(k)
			}
		}
		b.proxiesMu.Unlock()

		time.Sleep(time.Duration(b.config.Cluster.SyncBrokersInterval) * time.Second)
	}
}

func (b *Broker) addProxy(bi *brokerInfo) {
	key := b.brokerKey(bi)

	if proxy, ok := b.proxies[key]; ok {
		if proxy.peerInfo.ID != bi.ID {
			fmt.Printf("node: %s rebooted\n", key)
			proxy.updateBrokerID(bi.ID)
		}
		proxy.resetChance()
	} else {
		p := newProxy(b, bi)
		b.proxies[key] = p
		b.info.Proxies = len(b.proxies)
		go b.syncSubs(p)
	}
}

func (b *Broker) removeProxy(key string) {
	if proxy, ok := b.proxies[key]; ok {
		if proxy.reduceChance() <= 0 {
			fmt.Printf("disconnecting me -> broker %s\n", key)
			delete(b.proxies, key)
			b.info.Proxies = len(b.proxies)
			proxy.close()
		}
	}
}
