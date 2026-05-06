package socks

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

type Request struct {
	Host string
	Port uint16
}

var (
	ErrUnsupportedVersion = errors.New("unsupported socks version")
	ErrUnsupportedCommand = errors.New("unsupported socks command")
	ErrUnsupportedAddress = errors.New("unsupported socks address type")
)

func ReadRequest(rw io.ReadWriter) (Request, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(rw, hdr[:]); err != nil {
		return Request{}, err
	}
	if hdr[0] != 5 {
		return Request{}, ErrUnsupportedVersion
	}

	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(rw, methods); err != nil {
		return Request{}, err
	}
	if _, err := rw.Write([]byte{0x05, 0x00}); err != nil {
		return Request{}, err
	}

	var reqHdr [4]byte
	if _, err := io.ReadFull(rw, reqHdr[:]); err != nil {
		return Request{}, err
	}
	if reqHdr[0] != 5 {
		return Request{}, ErrUnsupportedVersion
	}
	if reqHdr[1] != 1 {
		return Request{}, ErrUnsupportedCommand
	}

	var host string
	switch reqHdr[3] {
	case 1:
		var ip [4]byte
		if _, err := io.ReadFull(rw, ip[:]); err != nil {
			return Request{}, err
		}
		host = net.IP(ip[:]).String()
	case 3:
		var lenBuf [1]byte
		if _, err := io.ReadFull(rw, lenBuf[:]); err != nil {
			return Request{}, err
		}
		name := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(rw, name); err != nil {
			return Request{}, err
		}
		host = string(name)
	case 4:
		var ip [16]byte
		if _, err := io.ReadFull(rw, ip[:]); err != nil {
			return Request{}, err
		}
		host = net.IP(ip[:]).String()
	default:
		return Request{}, ErrUnsupportedAddress
	}

	var portBuf [2]byte
	if _, err := io.ReadFull(rw, portBuf[:]); err != nil {
		return Request{}, err
	}
	port := binary.BigEndian.Uint16(portBuf[:])
	if host == "" || port == 0 {
		return Request{}, fmt.Errorf("bad destination %q:%d", host, port)
	}

	return Request{Host: host, Port: port}, nil
}

func WriteSuccess(w io.Writer) error {
	_, err := w.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}

func WriteFailure(w io.Writer, code byte) error {
	_, err := w.Write([]byte{0x05, code, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}
