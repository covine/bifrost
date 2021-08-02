package broker

import (
	"strings"
)

const (
	SUB = "1"
	PUB = "2"
)

func (b *Broker) checkTopicAuth(action, clientID, username, ip, topic string) bool {
	// cache support
	if b.auth != nil {
		if isOnOfflineTopic(topic) {
			return true
		}

		if strings.HasPrefix(topic, "$share/") && action == SUB {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				return false
			}
			topic = substr[2]
		}

		return b.auth.CheckACL(action, clientID, username, ip, topic)
	}

	return true
}

func (b *Broker) authenticate(username, password, clientID string) bool {
	if b.auth != nil {
		// TODO real
		return b.auth.CheckConnect(clientID, username, password)
	}

	return true
}
