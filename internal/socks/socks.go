package socks

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	CmdConnect      byte = 1
	CmdBind         byte = 2
	CmdUDPAssociate byte = 3
)

type Request struct {
	Command byte
	Host    string
	Port    uint16
}

type UDPDatagram struct {
	Host    string
	Port    uint16
	Payload []byte
}

type CommandError struct {
	Command byte
}

func (e CommandError) Error() string {
	return fmt.Sprintf("unsupported socks command %s(%d)", CommandName(e.Command), e.Command)
}

var (
	ErrUnsupportedVersion = errors.New("unsupported socks version")
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
	if reqHdr[1] != CmdConnect && reqHdr[1] != CmdUDPAssociate {
		return Request{}, CommandError{Command: reqHdr[1]}
	}

	host, port, _, err := readAddress(rw, reqHdr[3])
	if err != nil {
		return Request{}, err
	}
	if host == "" || (reqHdr[1] == CmdConnect && port == 0) {
		return Request{}, fmt.Errorf("bad destination %q:%d", host, port)
	}

	return Request{Command: reqHdr[1], Host: host, Port: port}, nil
}

func CommandName(cmd byte) string {
	switch cmd {
	case CmdConnect:
		return "connect"
	case CmdBind:
		return "bind"
	case CmdUDPAssociate:
		return "udp_associate"
	default:
		return "unknown"
	}
}

func ReplyCodeForError(err error) byte {
	var cmdErr CommandError
	if errors.As(err, &cmdErr) {
		return 0x07
	}
	if errors.Is(err, ErrUnsupportedAddress) {
		return 0x08
	}
	return 0x05
}

func WriteSuccess(w io.Writer) error {
	_, err := w.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}

func WriteUDPAssociateSuccess(w io.Writer, addr *net.UDPAddr) error {
	ip := addr.IP
	if ip == nil || ip.IsUnspecified() {
		ip = net.ParseIP("127.0.0.1")
	}
	if ip4 := ip.To4(); ip4 != nil {
		reply := []byte{0x05, 0x00, 0x00, 0x01, ip4[0], ip4[1], ip4[2], ip4[3], 0, 0}
		binary.BigEndian.PutUint16(reply[8:], uint16(addr.Port))
		_, err := w.Write(reply)
		return err
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return fmt.Errorf("bad udp bind ip: %s", ip)
	}
	reply := make([]byte, 4+16+2)
	reply[0], reply[1], reply[2], reply[3] = 0x05, 0x00, 0x00, 0x04
	copy(reply[4:], ip16)
	binary.BigEndian.PutUint16(reply[20:], uint16(addr.Port))
	_, err := w.Write(reply)
	return err
}

func WriteFailure(w io.Writer, code byte) error {
	_, err := w.Write([]byte{0x05, code, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}

func ParseUDPDatagram(data []byte) (UDPDatagram, error) {
	if len(data) < 4 {
		return UDPDatagram{}, errors.New("short socks udp datagram")
	}
	if data[0] != 0 || data[1] != 0 {
		return UDPDatagram{}, errors.New("bad socks udp reserved bytes")
	}
	if data[2] != 0 {
		return UDPDatagram{}, errors.New("fragmented socks udp datagrams are unsupported")
	}

	host, port, off, err := parseAddress(data, 3)
	if err != nil {
		return UDPDatagram{}, err
	}
	payload := make([]byte, len(data)-off)
	copy(payload, data[off:])
	return UDPDatagram{Host: host, Port: port, Payload: payload}, nil
}

func EncodeUDPDatagram(pkt UDPDatagram) ([]byte, error) {
	addr, err := encodeAddress(pkt.Host, pkt.Port)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 3+len(addr)+len(pkt.Payload))
	copy(out[:3], []byte{0, 0, 0})
	copy(out[3:], addr)
	copy(out[3+len(addr):], pkt.Payload)
	return out, nil
}

func readAddress(r io.Reader, atyp byte) (string, uint16, int, error) {
	switch atyp {
	case 1:
		var buf [6]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return "", 0, 0, err
		}
		return net.IP(buf[:4]).String(), binary.BigEndian.Uint16(buf[4:]), 1 + 4 + 2, nil
	case 3:
		var lenBuf [1]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return "", 0, 0, err
		}
		name := make([]byte, int(lenBuf[0])+2)
		if _, err := io.ReadFull(r, name); err != nil {
			return "", 0, 0, err
		}
		return string(name[:len(name)-2]), binary.BigEndian.Uint16(name[len(name)-2:]), 1 + 1 + len(name), nil
	case 4:
		var buf [18]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return "", 0, 0, err
		}
		return net.IP(buf[:16]).String(), binary.BigEndian.Uint16(buf[16:]), 1 + 16 + 2, nil
	default:
		return "", 0, 0, ErrUnsupportedAddress
	}
}

func parseAddress(data []byte, off int) (string, uint16, int, error) {
	if off >= len(data) {
		return "", 0, 0, errors.New("missing address type")
	}

	atyp := data[off]
	off++
	var host string
	switch atyp {
	case 1:
		if len(data)-off < 4+2 {
			return "", 0, 0, errors.New("short ipv4 address")
		}
		host = net.IP(data[off : off+4]).String()
		off += 4
	case 3:
		if off >= len(data) {
			return "", 0, 0, errors.New("missing domain length")
		}
		ln := int(data[off])
		off++
		if ln == 0 || len(data)-off < ln+2 {
			return "", 0, 0, errors.New("short domain address")
		}
		host = string(data[off : off+ln])
		off += ln
	case 4:
		if len(data)-off < 16+2 {
			return "", 0, 0, errors.New("short ipv6 address")
		}
		host = net.IP(data[off : off+16]).String()
		off += 16
	default:
		return "", 0, 0, ErrUnsupportedAddress
	}

	port := binary.BigEndian.Uint16(data[off : off+2])
	off += 2
	return host, port, off, nil
}

func encodeAddress(host string, port uint16) ([]byte, error) {
	if host == "" {
		return nil, errors.New("host is empty")
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			out := make([]byte, 1+4+2)
			out[0] = 1
			copy(out[1:], ip4)
			binary.BigEndian.PutUint16(out[5:], port)
			return out, nil
		}
		ip16 := ip.To16()
		if ip16 == nil {
			return nil, fmt.Errorf("bad ip address: %s", host)
		}
		out := make([]byte, 1+16+2)
		out[0] = 4
		copy(out[1:], ip16)
		binary.BigEndian.PutUint16(out[17:], port)
		return out, nil
	}
	if len(host) > 255 {
		return nil, fmt.Errorf("host too long: %d", len(host))
	}
	out := make([]byte, 1+1+len(host)+2)
	out[0] = 3
	out[1] = byte(len(host))
	copy(out[2:], host)
	binary.BigEndian.PutUint16(out[2+len(host):], port)
	return out, nil
}
