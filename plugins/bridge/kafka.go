package bridge

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type KafkaConfig struct {
	Addr             []string          `json:"addr"`
	ConnectTopic     string            `json:"onConnect"`
	SubscribeTopic   string            `json:"onSubscribe"`
	PublishTopic     string            `json:"onPublish"`
	UnsubscribeTopic string            `json:"onUnsubscribe"`
	DisconnectTopic  string            `json:"onDisconnect"`
	DeliverMap       map[string]string `json:"deliverMap"`
}

type kafka struct {
	config *KafkaConfig
	client sarama.AsyncProducer
}

func NewKafka(config *KafkaConfig) (*kafka, error) {
	k := &kafka{config: config}
	if err := k.connect(); err != nil {
		return nil, err
	}
	return k, nil
}

func (k *kafka) Publish(e *Elements) error {
	return k.pub(e)
}

func (k *kafka) connect() error {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_1_0
	cli, err := sarama.NewAsyncProducer(k.config.Addr, conf)
	if err != nil {
		return err
	}

	go func() {
		for err := range cli.Errors() {
			println(err.Error())
		}
	}()

	k.client = cli
	return nil
}

func (k *kafka) pub(e *Elements) error {
	topics := make(map[string]bool)
	switch e.Action {
	case Connect:
		if k.config.ConnectTopic != "" {
			topics[k.config.ConnectTopic] = true
		}
	case Publish:
		if k.config.PublishTopic != "" {
			topics[k.config.PublishTopic] = true
		}
		// foreach regexp map config
		for reg, topic := range k.config.DeliverMap {
			match := matchTopic(reg, e.Topic)
			if match {
				topics[topic] = true
			}
		}
	case Subscribe:
		if k.config.SubscribeTopic != "" {
			topics[k.config.SubscribeTopic] = true
		}
	case Unsubscribe:
		if k.config.UnsubscribeTopic != "" {
			topics[k.config.UnsubscribeTopic] = true
		}
	case Disconnect:
		if k.config.DisconnectTopic != "" {
			topics[k.config.DisconnectTopic] = true
		}
	default:
		return errors.New("error action: " + e.Action)
	}

	return k.publish(topics, e.ClientID, e)
}

func (k *kafka) publish(topics map[string]bool, key string, msg *Elements) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	for topic := range topics {
		select {
		case k.client.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(key),
			Value: sarama.ByteEncoder(payload),
		}:
			continue
		case <-time.After(5 * time.Second):
			return errors.New("write kafka timeout")
		}

	}

	return nil
}

func match(subTopic []string, topic []string) bool {
	if len(subTopic) == 0 {
		if len(topic) == 0 {
			return true
		}
		return false
	}

	if len(topic) == 0 {
		if subTopic[0] == "#" {
			return true
		}
		return false
	}

	if subTopic[0] == "#" {
		return true
	}

	if (subTopic[0] == "+") || (subTopic[0] == topic[0]) {
		return match(subTopic[1:], topic[1:])
	}
	return false
}

func matchTopic(subTopic string, topic string) bool {
	return match(strings.Split(subTopic, "/"), strings.Split(topic, "/"))
}
