/*
Copyright (C) 2026 by saba <contact me via issue>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

In addition, no derivative work may use the name or imply association
with this application without prior consent.
*/
package app

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/SUDOKU-ASCII/sudoku/internal/config"
	"github.com/SUDOKU-ASCII/sudoku/internal/protocol"
	"github.com/SUDOKU-ASCII/sudoku/pkg/connutil"
)

const (
	socks5Version        = 0x05
	socks5NoAuth         = 0x00
	socks5UserPassAuth   = 0x02
	socks5NoAcceptable   = 0xff
	socks5UDPAssociate   = 0x03
	socks5ReplySucceeded = 0x00
)

// newSOCKS5UDPDialer creates one SOCKS5 UDP association per UoT destination.
// Keeping the TCP control connection open is required for the association lifetime.
func newSOCKS5UDPDialer(proxyCfg *config.ProxyConfig) func(string) (net.Conn, error) {
	return func(target string) (net.Conn, error) {
		return dialSOCKS5UDP(proxyCfg, target)
	}
}

func dialSOCKS5UDP(proxyCfg *config.ProxyConfig, target string) (net.Conn, error) {
	if proxyCfg == nil || strings.TrimSpace(proxyCfg.Address) == "" {
		return nil, fmt.Errorf("missing SOCKS5 proxy address")
	}
	if _, _, err := net.SplitHostPort(target); err != nil {
		return nil, fmt.Errorf("invalid UDP target %q: %w", target, err)
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	control, err := dialer.Dial("tcp", strings.TrimSpace(proxyCfg.Address))
	if err != nil {
		return nil, fmt.Errorf("dial SOCKS5 proxy: %w", err)
	}
	fail := func(err error) (net.Conn, error) {
		_ = control.Close()
		return nil, err
	}

	auth := strings.TrimSpace(proxyCfg.Username) != "" || strings.TrimSpace(proxyCfg.Password) != ""
	methods := []byte{socks5NoAuth}
	if auth {
		methods = append(methods, socks5UserPassAuth)
	}
	if err := connutil.WriteFull(control, append([]byte{socks5Version, byte(len(methods))}, methods...)); err != nil {
		return fail(fmt.Errorf("write SOCKS5 greeting: %w", err))
	}
	var selected [2]byte
	if _, err := io.ReadFull(control, selected[:]); err != nil {
		return fail(fmt.Errorf("read SOCKS5 greeting: %w", err))
	}
	if selected[0] != socks5Version || selected[1] == socks5NoAcceptable {
		return fail(fmt.Errorf("SOCKS5 authentication rejected"))
	}
	if selected[1] == socks5UserPassAuth {
		user, password := []byte(proxyCfg.Username), []byte(proxyCfg.Password)
		if len(user) > 255 || len(password) > 255 {
			return fail(fmt.Errorf("SOCKS5 credentials exceed 255 bytes"))
		}
		request := append([]byte{0x01, byte(len(user))}, user...)
		request = append(request, byte(len(password)))
		request = append(request, password...)
		if err := connutil.WriteFull(control, request); err != nil {
			return fail(fmt.Errorf("write SOCKS5 credentials: %w", err))
		}
		var response [2]byte
		if _, err := io.ReadFull(control, response[:]); err != nil {
			return fail(fmt.Errorf("read SOCKS5 credentials: %w", err))
		}
		if response[0] != 0x01 || response[1] != 0x00 {
			return fail(fmt.Errorf("SOCKS5 credentials rejected"))
		}
	} else if selected[1] != socks5NoAuth {
		return fail(fmt.Errorf("unsupported SOCKS5 authentication method: %d", selected[1]))
	}

	associate := []byte{socks5Version, socks5UDPAssociate, 0x00, protocol.AddrTypeIPv4, 0, 0, 0, 0, 0, 0}
	if err := connutil.WriteFull(control, associate); err != nil {
		return fail(fmt.Errorf("write SOCKS5 UDP associate: %w", err))
	}
	var reply [3]byte
	if _, err := io.ReadFull(control, reply[:]); err != nil {
		return fail(fmt.Errorf("read SOCKS5 UDP associate: %w", err))
	}
	if reply[0] != socks5Version || reply[2] != 0x00 || reply[1] != socks5ReplySucceeded {
		return fail(fmt.Errorf("SOCKS5 UDP associate rejected: %d", reply[1]))
	}
	relayAddr, _, _, err := protocol.ReadAddress(control)
	if err != nil {
		return fail(fmt.Errorf("read SOCKS5 UDP relay address: %w", err))
	}
	relayAddr, err = resolveSOCKS5Relay(relayAddr, proxyCfg.Address)
	if err != nil {
		return fail(err)
	}
	relay, err := net.ResolveUDPAddr("udp", relayAddr)
	if err != nil {
		return fail(fmt.Errorf("resolve SOCKS5 UDP relay %q: %w", relayAddr, err))
	}
	udp, err := net.DialUDP("udp", nil, relay)
	if err != nil {
		return fail(fmt.Errorf("dial SOCKS5 UDP relay: %w", err))
	}
	return &socks5UDPConn{UDPConn: udp, control: control, target: target}, nil
}

func resolveSOCKS5Relay(relayAddr, proxyAddr string) (string, error) {
	host, port, err := net.SplitHostPort(relayAddr)
	if err != nil {
		return "", err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsUnspecified() {
		return relayAddr, nil
	}
	proxyHost, _, err := net.SplitHostPort(strings.TrimSpace(proxyAddr))
	if err != nil {
		return "", fmt.Errorf("invalid SOCKS5 proxy address %q: %w", proxyAddr, err)
	}
	return net.JoinHostPort(proxyHost, port), nil
}

type socks5UDPConn struct {
	*net.UDPConn
	control net.Conn
	target  string
	once    sync.Once
}

func (c *socks5UDPConn) Read(p []byte) (int, error) {
	buf := make([]byte, 3+300+65535)
	n, err := c.UDPConn.Read(buf)
	if err != nil {
		return 0, err
	}
	if n < 4 || buf[0] != 0 || buf[1] != 0 || buf[2] != 0 {
		return 0, fmt.Errorf("invalid SOCKS5 UDP response")
	}
	reader := bytes.NewReader(buf[3:n])
	if _, _, _, err := protocol.ReadAddress(reader); err != nil {
		return 0, fmt.Errorf("decode SOCKS5 UDP response address: %w", err)
	}
	return reader.Read(p)
}

func (c *socks5UDPConn) Write(p []byte) (int, error) {
	packet := bytes.NewBuffer(make([]byte, 0, len(p)+300))
	packet.Write([]byte{0, 0, 0})
	if err := protocol.WriteAddress(packet, c.target); err != nil {
		return 0, err
	}
	packet.Write(p)
	if _, err := c.UDPConn.Write(packet.Bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *socks5UDPConn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.UDPConn.Close()
		if closeErr := c.control.Close(); err == nil {
			err = closeErr
		}
	})
	return err
}
