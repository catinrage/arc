package udprelay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	MaxHostLength = 255
	MaxPayload    = 65535
)

type Packet struct {
	Host    string
	Port    uint16
	Payload []byte
}

func WritePacket(w io.Writer, pkt Packet) error {
	if len(pkt.Host) == 0 {
		return errors.New("udp packet host is empty")
	}
	if len(pkt.Host) > MaxHostLength {
		return fmt.Errorf("udp packet host too long: %d", len(pkt.Host))
	}
	if len(pkt.Payload) > MaxPayload {
		return fmt.Errorf("udp payload too large: %d", len(pkt.Payload))
	}

	frameLen := 1 + len(pkt.Host) + 2 + 2 + len(pkt.Payload)
	frame := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(frame[:4], uint32(frameLen))
	frame[4] = byte(len(pkt.Host))
	copy(frame[5:], pkt.Host)
	off := 5 + len(pkt.Host)
	binary.BigEndian.PutUint16(frame[off:off+2], pkt.Port)
	off += 2
	binary.BigEndian.PutUint16(frame[off:off+2], uint16(len(pkt.Payload)))
	off += 2
	copy(frame[off:], pkt.Payload)

	_, err := w.Write(frame)
	return err
}

func ReadPacket(r io.Reader) (Packet, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Packet{}, err
	}
	frameLen := binary.BigEndian.Uint32(lenBuf[:])
	if err := validateFrameLength(frameLen); err != nil {
		return Packet{}, err
	}

	frame := make([]byte, int(frameLen))
	if _, err := io.ReadFull(r, frame); err != nil {
		return Packet{}, err
	}
	return DecodeFrame(frame)
}

func DecodeBuffered(buf *[]byte, emit func(Packet) error) error {
	for {
		if len(*buf) < 4 {
			return nil
		}
		frameLen := binary.BigEndian.Uint32((*buf)[:4])
		if err := validateFrameLength(frameLen); err != nil {
			return err
		}
		total := 4 + int(frameLen)
		if len(*buf) < total {
			return nil
		}
		pkt, err := DecodeFrame((*buf)[4:total])
		if err != nil {
			return err
		}
		if err := emit(pkt); err != nil {
			return err
		}
		*buf = (*buf)[total:]
	}
}

func DecodeFrame(frame []byte) (Packet, error) {
	if len(frame) < 5 {
		return Packet{}, fmt.Errorf("bad udp frame length: %d", len(frame))
	}

	hostLen := int(frame[0])
	if hostLen == 0 {
		return Packet{}, errors.New("udp packet host is empty")
	}
	if len(frame) < 1+hostLen+4 {
		return Packet{}, errors.New("short udp packet frame")
	}

	off := 1
	host := string(frame[off : off+hostLen])
	off += hostLen
	port := binary.BigEndian.Uint16(frame[off : off+2])
	off += 2
	payloadLen := int(binary.BigEndian.Uint16(frame[off : off+2]))
	off += 2
	if len(frame)-off != payloadLen {
		return Packet{}, fmt.Errorf("bad udp payload length: got %d want %d", len(frame)-off, payloadLen)
	}

	payload := make([]byte, payloadLen)
	copy(payload, frame[off:])
	return Packet{Host: host, Port: port, Payload: payload}, nil
}

func HostPortFromAddr(addr net.Addr) (string, uint16, error) {
	if udp, ok := addr.(*net.UDPAddr); ok {
		if udp.Port < 0 || udp.Port > 65535 {
			return "", 0, fmt.Errorf("udp port out of range: %d", udp.Port)
		}
		return udp.IP.String(), uint16(udp.Port), nil
	}
	host, portText, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", 0, err
	}
	port, err := net.LookupPort("udp", portText)
	if err != nil {
		return "", 0, err
	}
	if port < 0 || port > 65535 {
		return "", 0, fmt.Errorf("udp port out of range: %d", port)
	}
	return host, uint16(port), nil
}

func validateFrameLength(frameLen uint32) error {
	if frameLen < 5 || frameLen > MaxHostLength+5+MaxPayload {
		return fmt.Errorf("bad udp frame length: %d", frameLen)
	}
	return nil
}
