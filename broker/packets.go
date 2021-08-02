package broker

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type fixedHeader struct {
	MessageType     byte
	Dup             bool
	Qos             byte
	Retain          bool
	RemainingLength int
}

func decodeLength(r io.Reader) (int, error) {
	var rLength uint32
	var multiplier uint32
	b := make([]byte, 1)
	for multiplier < 27 { //fix: Infinite '(digit & 128) == 1' will cause the dead loop
		_, err := io.ReadFull(r, b)
		if err != nil {
			return 0, err
		}

		digit := b[0]
		rLength |= uint32(digit&127) << multiplier
		if (digit & 128) == 0 {
			break
		}
		multiplier += 7
	}
	return int(rLength), nil
}

func (f *fixedHeader) unpack(typeAndFlags byte, r io.Reader) error {
	f.MessageType = typeAndFlags >> 4
	f.Dup = (typeAndFlags>>3)&0x01 > 0
	f.Qos = (typeAndFlags >> 1) & 0x03
	f.Retain = typeAndFlags&0x01 > 0

	var err error
	f.RemainingLength, err = decodeLength(r)
	return err
}

func (b *Broker) packetWithHeader(f fixedHeader) (packets.ControlPacket, error) {
	switch f.MessageType {
	case packets.Connect:
		p := b.newConnect()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Connack:
		p := b.newConnack()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Disconnect:
		p := b.newDisconnect()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Publish:
		p := b.newPublish()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Puback:
		p := b.newPuback()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Pubrec:
		return &packets.PubrecPacket{FixedHeader: packets.FixedHeader{
			MessageType:     f.MessageType,
			Dup:             f.Dup,
			Qos:             f.Qos,
			Retain:          f.Retain,
			RemainingLength: f.RemainingLength,
		}}, nil
	case packets.Pubrel:
		return &packets.PubrelPacket{FixedHeader: packets.FixedHeader{
			MessageType:     f.MessageType,
			Dup:             f.Dup,
			Qos:             f.Qos,
			Retain:          f.Retain,
			RemainingLength: f.RemainingLength,
		}}, nil
	case packets.Pubcomp:
		return &packets.PubcompPacket{FixedHeader: packets.FixedHeader{
			MessageType:     f.MessageType,
			Dup:             f.Dup,
			Qos:             f.Qos,
			Retain:          f.Retain,
			RemainingLength: f.RemainingLength,
		}}, nil
	case packets.Subscribe:
		p := b.newSubscribe()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Suback:
		p := b.newSuback()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Unsubscribe:
		p := b.newUnsubscribe()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Unsuback:
		p := b.newUnsuback()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Pingreq:
		p := b.newPingreq()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	case packets.Pingresp:
		p := b.newPingresp()
		p.MessageType = f.MessageType
		p.Dup = f.Dup
		p.Qos = f.Qos
		p.Retain = f.Retain
		p.RemainingLength = f.RemainingLength
		return p, nil
	}
	return nil, fmt.Errorf("unsupported packet type 0x%x", f.MessageType)
}

func (b *Broker) ReadPacket(r io.Reader) (packets.ControlPacket, error) {
	var f fixedHeader
	bs := make([]byte, 1)

	_, err := io.ReadFull(r, bs)
	if err != nil {
		return nil, err
	}

	err = f.unpack(bs[0], r)
	if err != nil {
		return nil, err
	}

	cp, err := b.packetWithHeader(f)
	if err != nil {
		return nil, err
	}

	packetBytes := make([]byte, f.RemainingLength)
	n, err := io.ReadFull(r, packetBytes)
	if err != nil {
		return nil, err
	}
	if n != f.RemainingLength {
		return nil, errors.New("failed to read expected data")
	}

	err = cp.Unpack(bytes.NewBuffer(packetBytes))
	return cp, err
}
