package broker

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
)

var cleanFixedHeader = packets.FixedHeader{
	MessageType:     0,
	Dup:             false,
	Qos:             0,
	Retain:          false,
	RemainingLength: 0,
}

func (b *Broker) newConnect() *packets.ConnectPacket {
	return b.connectPool.Get().(*packets.ConnectPacket)
}

func (b *Broker) putConnect(p *packets.ConnectPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Connect
	p.ProtocolName = ""
	p.ProtocolVersion = 0
	p.CleanSession = false
	p.WillFlag = false
	p.WillQos = 0
	p.WillRetain = false
	p.UsernameFlag = false
	p.PasswordFlag = false
	p.ReservedBit = 0
	p.Keepalive = 0
	p.ClientIdentifier = ""
	p.WillTopic = ""
	p.WillMessage = nil
	p.Username = ""
	p.Password = nil
	b.connectPool.Put(p)
}

func (b *Broker) newConnack() *packets.ConnackPacket {
	return b.connackPool.Get().(*packets.ConnackPacket)
}

func (b *Broker) putConnack(p *packets.ConnackPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Connack
	p.SessionPresent = false
	p.ReturnCode = 0
	b.connackPool.Put(p)
}

func (b *Broker) newPublish() *packets.PublishPacket {
	return b.publishPool.Get().(*packets.PublishPacket)
}

func (b *Broker) putPublish(p *packets.PublishPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Publish
	p.TopicName = ""
	p.MessageID = 0
	p.Payload = nil
	b.publishPool.Put(p)
}

func (b *Broker) newPuback() *packets.PubackPacket {
	return b.pubackPool.Get().(*packets.PubackPacket)
}

func (b *Broker) putPuback(p *packets.PubackPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Puback
	p.MessageID = 0
	b.pubackPool.Put(p)
}

func (b *Broker) newSubscribe() *packets.SubscribePacket {
	return b.subscribePool.Get().(*packets.SubscribePacket)
}

func (b *Broker) putSubscribe(p *packets.SubscribePacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Subscribe
	p.Topics = nil
	p.Qoss = nil
	p.MessageID = 0
	b.subscribePool.Put(p)
}

func (b *Broker) newSuback() *packets.SubackPacket {
	return b.subackPool.Get().(*packets.SubackPacket)
}

func (b *Broker) putSuback(p *packets.SubackPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Suback
	p.ReturnCodes = nil
	p.MessageID = 0
	b.subackPool.Put(p)
}

func (b *Broker) newUnsubscribe() *packets.UnsubscribePacket {
	return b.unsubscribePool.Get().(*packets.UnsubscribePacket)
}

func (b *Broker) putUnsubscribe(p *packets.UnsubscribePacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Unsubscribe
	p.Topics = nil
	p.MessageID = 0
	b.unsubscribePool.Put(p)
}

func (b *Broker) newUnsuback() *packets.UnsubackPacket {
	return b.unsubackPool.Get().(*packets.UnsubackPacket)
}

func (b *Broker) putUnsuback(p *packets.UnsubackPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Unsuback
	p.MessageID = 0
	b.unsubackPool.Put(p)
}

func (b *Broker) newPingreq() *packets.PingreqPacket {
	return b.pingreqPool.Get().(*packets.PingreqPacket)
}

func (b *Broker) putPingreq(p *packets.PingreqPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Pingreq
	b.pingreqPool.Put(p)
}

func (b *Broker) newPingresp() *packets.PingrespPacket {
	return b.pingrespPool.Get().(*packets.PingrespPacket)
}

func (b *Broker) putPingresp(p *packets.PingrespPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Pingresp
	b.pingrespPool.Put(p)
}

func (b *Broker) newDisconnect() *packets.DisconnectPacket {
	return b.disconnectPool.Get().(*packets.DisconnectPacket)
}

func (b *Broker) putDisconnect(p *packets.DisconnectPacket) {
	p.FixedHeader = cleanFixedHeader
	p.MessageType = packets.Disconnect
	b.disconnectPool.Put(p)
}
