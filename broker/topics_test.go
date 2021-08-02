package broker

import (
	"bytes"
	"errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"testing"
)

type res struct {
	topic  []byte
	remain []byte
	err    error
}

type sinsertParam struct {
	topic []byte
	qos   byte
	sub   *subscription
}

func TestNext(t *testing.T) {

	var nextTests = []struct {
		in       []byte
		expected res
	}{
		{[]byte("sensors/temperature"), res{
			topic:  []byte("sensors"),
			remain: []byte("temperature"),
			err:    nil,
		}},
		{[]byte("sensors/"), res{
			topic:  []byte("sensors"),
			remain: nil,
			err:    nil,
		}},
		{[]byte("sensors"), res{
			topic:  []byte("sensors"),
			remain: nil,
			err:    nil,
		}},
		{[]byte("sensors/#"), res{
			topic:  []byte("sensors"),
			remain: []byte("#"),
			err:    nil,
		}},
		{[]byte("sensors#/"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'#' must occupy entire topic level"),
		}},
		{[]byte("sensors+/"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'+' must occupy entire topic level"),
		}},
		{[]byte("//"), res{
			topic:  []byte("+"),
			remain: []byte("/"),
			err:    nil,
		}},
		{[]byte("/+"), res{
			topic:  []byte("+"),
			remain: []byte("+"),
			err:    nil,
		}},
		{[]byte("/sensors/#"), res{
			topic:  []byte("+"),
			remain: []byte("sensors/#"),
			err:    nil,
		}},
		{[]byte("/#"), res{
			topic:  []byte("+"),
			remain: []byte("#"),
			err:    nil,
		}},
		{[]byte("+/"), res{
			topic:  []byte("+"),
			remain: []byte(""),
			err:    nil,
		}},
		{[]byte("+asf/"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'#' and '+' must occupy entire topic level"),
		}},
		{[]byte("+#s/"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'#' must occupy entire topic level"),
		}},
		{[]byte("++sf/"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'+' must occupy entire topic level"),
		}},
		{[]byte("#/"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'#' must at the last level"),
		}},
		{[]byte("#"), res{
			topic:  []byte("#"),
			remain: nil,
			err:    nil,
		}},
		{[]byte("#+"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'+' must occupy entire topic level"),
		}},
		{[]byte("#dd"), res{
			topic:  nil,
			remain: nil,
			err:    errors.New("'#' and '+' must occupy entire topic level"),
		}},
	}

	for _, tt := range nextTests {
		topic, r, e := next(tt.in)
		if !bytes.Equal(topic, tt.expected.topic) || !bytes.Equal(r, tt.expected.remain) {
			t.Errorf("next(%s) = %s, %s;\nexpected: %s, %s",
				string(tt.in), string(topic), string(r),
				string(tt.expected.topic), string(tt.expected.remain))
		}
		if e != nil {
			if tt.expected.err == nil || e.Error() != tt.expected.err.Error() {
				t.Errorf("next() = %v; expected: %v", e, tt.expected.err)
			}
		} else {
			// e == nil
			if tt.expected.err != nil {
				t.Errorf("next() = %v; expected: %v", e, tt.expected.err)
			}
		}
	}
}

func TestSinsert(t *testing.T) {
	root := newSubNode()

	var sinsertTests = []sinsertParam{
		{[]byte("sensors"), 0, &subscription{topic: "roy"}},
		{[]byte("sensors/temperature"), 0, &subscription{topic: "tony"}},
		{[]byte("sensors/temperature/room1"), 0, &subscription{topic: "juicy"}},
		{[]byte("sensors/temperature/room2"), 0, &subscription{topic: "lily"}},
		{[]byte("sensors/temperature/room2"), 1, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature/room3"), 0, &subscription{topic: "tom"}},
		{[]byte("sensors/humidity"), 0, &subscription{topic: "jack"}},
		{[]byte("sensors/humidity/room1"), 0, &subscription{topic: "tracy"}},
		{[]byte("sensors/humidity/room2"), 0, &subscription{topic: "mike"}},
		{[]byte("sensors/humidity/room3"), 0, &subscription{topic: "bob"}},
		{[]byte("sensors/humidity/room3"), 2, &subscription{topic: "bush"}},
		{[]byte("sensors/#"), 0, &subscription{topic: "root"}},
	}
	version := uint64(1)

	for _, tt := range sinsertTests {
		err := root.sinsert(tt.topic, tt.qos, tt.sub, &version)
		if err != nil {
			t.Errorf("err is %v", err)
		}
	}

	if version != 13 {
		t.Errorf("version is %d; expected 13", version)
	}

	level1 := root.snodes["sensors"]

	if level1.qos[0] != 0 || level1.subs[0].topic != "roy" {
		t.Errorf("qos is %d, sub is %s", level1.qos[0], level1.subs[0].topic)
	}

	level2 := level1.snodes["temperature"]
	level3 := level2.snodes["room2"]

	if level3.subs[0].topic != "lily" {
		t.Errorf("sub is %s", level3.subs[0].topic)
	}

	if level3.subs[1].topic != "lucy" || level3.qos[1] != 1 {
		t.Errorf("qos is %d, sub is %s", level3.qos[1], level3.subs[1].topic)
	}

	level2 = level1.snodes["humidity"]
	level3 = level2.snodes["room3"]

	if level2.qos[0] != 0 || level2.subs[0].topic != "jack" {
		t.Errorf("qos is %d, sub is %s", level2.qos[0], level2.subs[0].topic)
	}

	if level3.qos[0] != 0 || level3.subs[0].topic != "bob" {
		t.Errorf("qos is %d, sub is %s", level3.qos[0], level3.subs[0].topic)
	}

	if level3.qos[1] != 2 || level3.subs[1].topic != "bush" {
		t.Errorf("qos is %d, sub is %s", level3.qos[1], level3.subs[1].topic)
	}
}

func TestSremove(t *testing.T) {
	root := newSubNode()

	var sinsertTests = []sinsertParam{
		{[]byte("sensors"), 0, &subscription{topic: "roy"}},
		{[]byte("sensors/temperature"), 0, &subscription{topic: "tony"}},
		{[]byte("sensors/temperature/room1"), 0, &subscription{topic: "juicy"}},
		{[]byte("sensors/temperature/room2"), 0, &subscription{topic: "lily"}},
		{[]byte("sensors/temperature/room2"), 1, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature/room2"), 2, &subscription{topic: "kate"}},
		{[]byte("sensors/temperature/room3"), 0, &subscription{topic: "tom"}},
		{[]byte("sensors/humidity"), 0, &subscription{topic: "jack"}},
		{[]byte("sensors/humidity/room1"), 0, &subscription{topic: "tracy"}},
		{[]byte("sensors/humidity/room2"), 0, &subscription{topic: "mike"}},
		{[]byte("sensors/humidity/room3"), 0, &subscription{topic: "bob"}},
		{[]byte("sensors/humidity/room3"), 2, &subscription{topic: "bush"}},
		{[]byte("sensors/#"), 0, &subscription{topic: "root"}},
	}

	version := uint64(1)

	for _, tt := range sinsertTests {
		err := root.sinsert(tt.topic, tt.qos, tt.sub, &version)
		if err != nil {
			t.Errorf("err is %v", err)
		}
	}

	err := root.sremove([]byte("sensors/temperature/room1"), "ju", &version)
	if err != nil {
		t.Errorf("error is %v", err)
	}

	err = root.sremove([]byte("sensors/temperature/room1"), "juicy", &version)
	if err != nil {
		t.Errorf("error is %v; expected <nil>", err)
	}

	level1 := root.snodes["sensors"]
	level2 := level1.snodes["temperature"]
	v, ok := level2.snodes["room1"]
	if ok {
		t.Errorf("topic room1 in node map should be deleted, the value is %v", v)
	}

	err = root.sremove([]byte("sensors/temperature/room1"), "juicy", &version)
	if err != nil {
		t.Errorf("error is %v", err)
	}

	err = root.sremove([]byte("sensors/temperature/room2"), "juicy", &version)
	if err != nil {
		t.Errorf("error is %v", err)
	}

	err = root.sremove([]byte("sensors/temperature/room2"), "lily", &version)
	if err != nil {
		t.Errorf("error is %v; expected <nil>", err)
	}

	v, ok = level2.snodes["room2"]
	if !ok {
		t.Errorf("Topic of room2 shouldn't be deleted right now")
	}
	if len(v.subs) != 2 || len(v.qos) != 2 || v.subs[0].topic != "lucy" || v.qos[0] != 1 {
		t.Errorf("lenght of subs: %d, length of qos: %d, v.sub[0]: %s, v.qos[0]: %d; expected: 2, 2, lucy, 1",
			len(v.subs), len(v.qos), v.subs[0].topic, v.qos[0])
	}

	err = root.sremove([]byte("sensors/temperature/room2"), "lucy", &version)
	if err != nil {
		t.Errorf("error is %v; expected <nil>", err)
	}

	v, ok = level2.snodes["room2"]
	if !ok {
		t.Errorf("Topic of room2 shouldn't be deleted right now")
	}
	if len(v.subs) != 1 || len(v.qos) != 1 || v.subs[0].topic != "kate" || v.qos[0] != 2 {
		t.Errorf("lenght of subs: %d, length of qos: %d, v.sub[0]: %s, v.qos[0]: %d; expected: 1, 1, kate, 2",
			len(v.subs), len(v.qos), v.subs[0].topic, v.qos[0])
	}

	err = root.sremove([]byte("sensors/temperature/room2"), "kate", &version)
	if err != nil {
		t.Errorf("error is %v; expected <nil>", err)
	}

	v, ok = level2.snodes["room2"]
	if ok {
		t.Errorf("Topic of room2 should be deleted right now")
	}

	if len(level2.snodes) != 1 {
		t.Errorf("length is %d; expected is 1", len(level2.snodes))
	}

	err = root.sremove([]byte("sensors/temperature/room3"), "tom", &version)
	if err != nil {
		t.Errorf("error is %v; expected <nil>", err)
	}

	v, ok = level2.snodes["room3"]
	if ok {
		t.Errorf("Topic of room3 should be deleted right now")
	}

	if len(level2.snodes) != 0 {
		t.Errorf("length is %d; expected is 0", len(level2.snodes))
	}

	err = root.sremove([]byte("sensors/temperature/"), "tony", &version)
	if err != nil {
		t.Errorf("error is %v; expected <nil>", err)
	}

	level2, ok = level1.snodes["temperature"]
	if ok {
		t.Errorf("topic temperature should be deleted right now")
	}

	err = root.sremove([]byte("sensors"), "roy", &version)
	if err != nil {
		t.Errorf("error is %v; expected <nil>", err)
	}

	level1, ok = root.snodes["sensors"]
	if !ok {
		t.Errorf("topic sensors shouldn't be deleted right now")
	}

	if len(level1.qos) != 0 || len(level1.subs) != 0 {
		t.Errorf("length of subs is %d, length of qos is %d; expected 0, 0", len(level1.subs), len(level1.qos))
	}

	if len(level1.snodes) != 2 {
		t.Errorf("length of snodes is %d; expected 2", len(level1.snodes))
	}

	level2 = level1.snodes["humidity"]
	if len(level2.snodes) != 3 || len(level2.subs) != 1 {
		t.Errorf("length of snodes is %d, length of subs is %d; expected 3, 1",
			len(level2.snodes), len(level2.subs))
	}

	level3 := level2.snodes["room3"]
	if len(level3.subs) != 2 || len(level3.qos) != 2 {
		t.Errorf("length of subs is %d, length of qos is %d; expected 2, 2",
			len(level3.subs), len(level3.qos))
	}
}

func TestSmatch(t *testing.T) {
	root := newSubNode()

	var sinsertTests = []sinsertParam{
		{[]byte("sensors"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature/room1"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature/room2"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature/room2"), 1, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature/room2"), 2, &subscription{topic: "lucy"}},
		{[]byte("sensors/temperature/room3"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/humidity"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/humidity/room1"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/humidity/room2"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/humidity/room3"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/humidity/room3"), 2, &subscription{topic: "lucy"}},
		{[]byte("sensors/#"), 0, &subscription{topic: "lucy"}},
		{[]byte("sensors/+/room2"), 1, &subscription{topic: "lucy"}},
		{[]byte("sensors/humidity/#"), 0, &subscription{topic: "lucy"}},
	}

	version := uint64(1)

	for _, tt := range sinsertTests {
		err := root.sinsert(tt.topic, tt.qos, tt.sub, &version)
		if err != nil {
			t.Errorf("err is %v", err)
		}
	}

	var subs []*subscription
	var qoss []byte

	err := root.smatch([]byte("sensors"), &subs, &qoss)
	if err != nil {
		t.Errorf("error: %v should be <nil>", err)
	}
	if len(subs) != 2 || len(qoss) != 2 {
		t.Errorf("length of subs is %d, length of qoss is %d; expected: 2, 2", len(subs), len(qoss))
	}
	for _, v := range subs {
		if v.topic != "roy" && v.topic != "root" {
			t.Errorf("subsriber info went wrong, sub is %s", v.topic)
		}
	}
	for _, v := range qoss {
		if v != 0 {
			t.Errorf("value of qos should be 0")
		}
	}

	subs = subs[0:0]
	qoss = qoss[0:0]
	err = root.smatch([]byte("sensors/temperature/room2"), &subs, &qoss)
	if err != nil {
		t.Errorf("error: %v should be <nil>", err)
	}
	if len(subs) != 5 || len(qoss) != 5 {
		t.Errorf("length of subs is %d, length of qoss is %d; expected: 5, 5", len(subs), len(qoss))
	}

	subs = subs[0:0]
	qoss = qoss[0:0]
	err = root.smatch([]byte("sensors/humidity/room2"), &subs, &qoss)
	if err != nil {
		t.Errorf("error: %v should be <nil>", err)
	}
	if len(subs) != 4 || len(qoss) != 4 {
		t.Errorf("length of subs is %d, length of qoss is %d; expected: 4, 4", len(subs), len(qoss))
	}
}

func TestInsertOrUpdate(t *testing.T) {
	root := newRetainNode()
	cases := []*packets.PublishPacket{
		{TopicName: "device/door-lock", MessageID: 1, Payload: []byte("test1")},
		{TopicName: "device/door-lock/capacity", MessageID: 2, Payload: []byte("test2")},
		{TopicName: "device/air-conditioner", MessageID: 3, Payload: []byte("test3")},
		{TopicName: "device/air-conditioner/temperature", MessageID: 4, Payload: []byte("test4")},
		{TopicName: "device/air-conditioner/humidity", MessageID: 5, Payload: []byte("test5")},
		{TopicName: "device/fridge/temperature", MessageID: 6, Payload: []byte("test6")},
		{TopicName: "device/fridge/eco", MessageID: 7, Payload: []byte("test7")},
		{TopicName: "device/fridge", MessageID: 8, Payload: []byte("test8")},
		{TopicName: "device/air-conditioner/eco", MessageID: 9, Payload: []byte("test9")},
		{TopicName: "device/tv/sound", MessageID: 10, Payload: []byte("test10")},
		{TopicName: "device/tv", MessageID: 11, Payload: []byte("test11")},
		{TopicName: "device/tv/smart", MessageID: 12, Payload: []byte("test12")},
		{TopicName: "device", MessageID: 13, Payload: []byte("test13")},
	}

	for _, v := range cases {
		err := root.insertOrUpdate([]byte(v.TopicName), v)
		if err != nil {
			t.Errorf("error occured when insert, error: %v\n", err)
		}
	}

	level1 := root.retainNodes["device"]
	msgID := level1.msg.MessageID
	if msgID != 13 {
		t.Errorf("msg id is %d; expected: [13]\n", msgID)
	}

	level2 := level1.retainNodes["tv"]
	msgID = level2.msg.MessageID
	if msgID != 11 {
		t.Errorf("msg id is %d; expected: [11]\n", msgID)
	}

	level2 = level1.retainNodes["fridge"]
	msgID = level2.msg.MessageID
	if msgID != 8 {
		t.Errorf("msg id is %d; expected: [8]\n", msgID)
	}

	level3 := level2.retainNodes["temperature"]
	msgID = level3.msg.MessageID
	if msgID != 6 {
		t.Errorf("msg id is %d; expected: [6]\n", msgID)
	}

	case1 := &packets.PublishPacket{
		TopicName: "device/fridge/temperature",
		MessageID: 15,
		Payload:   nil,
	}
	err := root.insertOrUpdate([]byte("device/fridge/temperature"), case1)
	if err != nil {
		t.Errorf("error occured %v\n", err)
	}
	level3 = level2.retainNodes["temperature"]
	msgID = level3.msg.MessageID
	if msgID != 15 {
		t.Errorf("msg id is %d; expected: [15]\n", msgID)
	}
}

func TestMatch(t *testing.T) {
	root := newRetainNode()
	cases := []*packets.PublishPacket{
		{TopicName: "device/door-lock", MessageID: 1, Payload: []byte("test1")},
		{TopicName: "device/door-lock/capacity", MessageID: 2, Payload: []byte("test2")},
		{TopicName: "device/air-conditioner", MessageID: 3, Payload: []byte("test3")},
		{TopicName: "device/air-conditioner/temperature", MessageID: 4, Payload: []byte("test4")},
		{TopicName: "device/air-conditioner/humidity", MessageID: 5, Payload: []byte("test5")},
		{TopicName: "device/fridge/temperature", MessageID: 6, Payload: []byte("test6")},
		{TopicName: "device/fridge/eco", MessageID: 7, Payload: []byte("test7")},
		{TopicName: "device/fridge", MessageID: 8, Payload: []byte("test8")},
		{TopicName: "device/air-conditioner/eco", MessageID: 9, Payload: []byte("test9")},
		{TopicName: "device/tv/sound", MessageID: 10, Payload: []byte("test10")},
		{TopicName: "device/tv", MessageID: 11, Payload: []byte("test11")},
		{TopicName: "device/tv/smart", MessageID: 12, Payload: []byte("test12")},
	}

	for _, v := range cases {
		err := root.insertOrUpdate([]byte(v.TopicName), v)
		if err != nil {
			t.Errorf("error occured when insert, error: %v\n", err)
		}
	}

	var msgs []*packets.PublishPacket

	err := root.match([]byte("device/fridge"), &msgs)
	if err != nil {
		t.Errorf("error occured when match topic [device/fridge], error: %v\n", err)
	}
	if len(msgs) != 1 {
		t.Errorf("message legnth is %d; expected: 1\n", len(msgs))
	}
	msg := msgs[0]
	if msg.TopicName != "device/fridge" || msg.MessageID != 8 {
		t.Errorf("topicname is %s, id is %d; expected [device/fridge], [8]", msg.TopicName, msg.MessageID)
	}

	msgs = msgs[0:0]
	err = root.match([]byte("device/#"), &msgs)
	if err != nil {
		t.Errorf("error occured when match topic [device/#], error: %v\n", err)
	}
	if len(msgs) != 12 {
		t.Errorf("message legnth is %d; expected: 12\n", len(msgs))
	}

	msgs = msgs[0:0]
	err = root.match([]byte("device/+/temperature"), &msgs)
	if err != nil {
		t.Errorf("error occured when match topic [device/+/temperature], error: %v\n", err)
	}
	if len(msgs) != 2 {
		t.Errorf("message legnth is %d; expected: 2\n", len(msgs))
	}

	msgs = msgs[0:0]
	err = root.match([]byte("device/door-lock/#"), &msgs)
	if err != nil {
		t.Errorf("error occured when match topic [device/door-lock/#], error: %v\n", err)
	}
	if len(msgs) != 2 {
		t.Errorf("message legnth is %d; expected: 2\n", len(msgs))
	}

	msgs = msgs[0:0]
	err = root.match([]byte("device/tv/#"), &msgs)
	if err != nil {
		t.Errorf("error occured when match topic [device/tv/#], error: %v\n", err)
	}
	if len(msgs) != 3 {
		t.Errorf("message legnth is %d; expected: 3\n", len(msgs))
	}

	msgs = msgs[0:0]
	err = root.match([]byte("device/air-conditioner/humidity"), &msgs)
	if err != nil {
		t.Errorf("error occured when match topic [device/air-conditioner/humidity], error: %v\n", err)
	}
	if len(msgs) != 1 {
		t.Errorf("message legnth is %d; expected: 1\n", len(msgs))
	}
	msg = msgs[0]
	if msg.TopicName != "device/air-conditioner/humidity" || msg.MessageID != 5 {
		t.Errorf("topicname is %s, id is %d; expected [device/air-conditioner/humidity], [5]",
			msg.TopicName, msg.MessageID)
	}

	msgs = msgs[0:0]
	err = root.match([]byte("device/airconditioner/humidity"), &msgs)
	if err != nil {
		t.Errorf("error shoud be nil when match topic [device/airconditioner/humidity], error: %v\n", err)
	}
	if len(msgs) != 0 {
		t.Errorf("length of msgs is %d; expected [0]\n", len(msgs))
	}
}

func TestRemove(t *testing.T) {
	root := newRetainNode()
	cases := []*packets.PublishPacket{
		{TopicName: "device/door-lock", MessageID: 1, Payload: []byte("test1")},
		{TopicName: "device/door-lock/capacity", MessageID: 2, Payload: []byte("test2")},
		{TopicName: "device/air-conditioner", MessageID: 3, Payload: []byte("test3")},
		{TopicName: "device/air-conditioner/temperature", MessageID: 4, Payload: []byte("test4")},
		{TopicName: "device/air-conditioner/humidity", MessageID: 5, Payload: []byte("test5")},
		{TopicName: "device/fridge/temperature", MessageID: 6, Payload: []byte("test6")},
		{TopicName: "device/fridge/eco", MessageID: 7, Payload: []byte("test7")},
		{TopicName: "device/fridge", MessageID: 8, Payload: []byte("test8")},
		{TopicName: "device/air-conditioner/eco", MessageID: 9, Payload: []byte("test9")},
		{TopicName: "device/tv/sound", MessageID: 10, Payload: []byte("test10")},
		{TopicName: "device/tv", MessageID: 11, Payload: []byte("test11")},
		{TopicName: "device/tv/smart", MessageID: 12, Payload: []byte("test12")},
		{TopicName: "device", MessageID: 13, Payload: []byte("test13")},
	}

	for _, v := range cases {
		err := root.insertOrUpdate([]byte(v.TopicName), v)
		if err != nil {
			t.Errorf("error occured when insert, error: %v\n", err)
		}
	}

	err := root.remove([]byte("device/tv"))
	if err != nil {
		t.Errorf("error should be nil %v\n", err)
	}

	level1 := root.retainNodes["device"]
	level2 := level1.retainNodes["tv"]
	if level2.msg != nil {
		t.Errorf("msg should be nil %v\n", level2.msg)
	}
}
