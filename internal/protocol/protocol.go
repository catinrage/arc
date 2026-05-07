package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	TypeOpen byte = iota + 1
	TypeData
	TypeClose
	TypeError
	TypeOpenOK
	TypeReady
)

const (
	headerSize        = 9
	MaxPayloadSize    = 16 << 20
	MaxHostNameLength = 255
	UDPAssociateHost  = "__arc_udp_associate__"
)

type Message struct {
	Type     byte
	StreamID uint32
	Payload  []byte
}

type OpenRequest struct {
	Host string
	Port uint16
}

func EncodeMessage(msg Message) ([]byte, error) {
	if len(msg.Payload) > MaxPayloadSize {
		return nil, fmt.Errorf("payload too large: %d", len(msg.Payload))
	}

	out := make([]byte, headerSize+len(msg.Payload))
	out[0] = msg.Type
	binary.BigEndian.PutUint32(out[1:5], msg.StreamID)
	binary.BigEndian.PutUint32(out[5:9], uint32(len(msg.Payload)))
	copy(out[headerSize:], msg.Payload)
	return out, nil
}

func DecodeMessage(data []byte) (Message, error) {
	if len(data) < headerSize {
		return Message{}, errors.New("message too short")
	}

	payloadLen := binary.BigEndian.Uint32(data[5:9])
	if payloadLen > MaxPayloadSize {
		return Message{}, fmt.Errorf("payload too large: %d", payloadLen)
	}
	if len(data)-headerSize != int(payloadLen) {
		return Message{}, fmt.Errorf("bad payload length: got %d want %d", len(data)-headerSize, payloadLen)
	}

	return Message{
		Type:     data[0],
		StreamID: binary.BigEndian.Uint32(data[1:5]),
		Payload:  data[headerSize:],
	}, nil
}

func EncodeOpen(req OpenRequest) ([]byte, error) {
	if len(req.Host) == 0 {
		return nil, errors.New("host is empty")
	}
	if len(req.Host) > MaxHostNameLength {
		return nil, fmt.Errorf("host too long: %d", len(req.Host))
	}

	payload := make([]byte, 1+len(req.Host)+2)
	payload[0] = byte(len(req.Host))
	copy(payload[1:], req.Host)
	binary.BigEndian.PutUint16(payload[1+len(req.Host):], req.Port)
	return payload, nil
}

func DecodeOpen(payload []byte) (OpenRequest, error) {
	if len(payload) < 3 {
		return OpenRequest{}, errors.New("open payload too short")
	}

	hostLen := int(payload[0])
	if hostLen == 0 {
		return OpenRequest{}, errors.New("host is empty")
	}
	if len(payload) != 1+hostLen+2 {
		return OpenRequest{}, errors.New("bad open payload length")
	}

	return OpenRequest{
		Host: string(payload[1 : 1+hostLen]),
		Port: binary.BigEndian.Uint16(payload[1+hostLen:]),
	}, nil
}
