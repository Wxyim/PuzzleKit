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
	"io"
	"net"
	"testing"
	"time"

	"github.com/SUDOKU-ASCII/sudoku/internal/config"
	"github.com/SUDOKU-ASCII/sudoku/internal/protocol"
)

func TestDialSOCKS5UDP(t *testing.T) {
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen UDP relay: %v", err)
	}
	defer relay.Close()

	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen SOCKS5 proxy: %v", err)
	}
	defer proxyListener.Close()

	controlClosed := make(chan struct{})
	go func() {
		conn, err := proxyListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		greeting := make([]byte, 3)
		if _, err := io.ReadFull(conn, greeting); err != nil {
			return
		}
		if !bytes.Equal(greeting, []byte{socks5Version, 1, socks5NoAuth}) {
			return
		}
		if _, err := conn.Write([]byte{socks5Version, socks5NoAuth}); err != nil {
			return
		}

		request := make([]byte, 10)
		if _, err := io.ReadFull(conn, request); err != nil {
			return
		}
		if request[0] != socks5Version || request[1] != socks5UDPAssociate {
			return
		}
		response := bytes.NewBuffer([]byte{socks5Version, socks5ReplySucceeded, 0})
		if err := protocol.WriteAddress(response, relay.LocalAddr().String()); err != nil {
			return
		}
		if _, err := conn.Write(response.Bytes()); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
		close(controlClosed)
	}()

	packetSeen := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 2048)
		n, client, err := relay.ReadFromUDP(buf)
		if err != nil {
			return
		}
		packet := append([]byte(nil), buf[:n]...)
		packetSeen <- packet
		_, _ = relay.WriteToUDP(packet, client)
	}()

	conn, err := dialSOCKS5UDP(&config.ProxyConfig{Address: proxyListener.Addr().String()}, "example.com:53")
	if err != nil {
		t.Fatalf("dialSOCKS5UDP: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write UDP payload: %v", err)
	}

	buf := make([]byte, 16)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read UDP payload: %v", err)
	}
	if got := string(buf[:n]); got != "ping" {
		t.Fatalf("UDP payload = %q, want ping", got)
	}

	select {
	case packet := <-packetSeen:
		if len(packet) < 3 || !bytes.Equal(packet[:3], []byte{0, 0, 0}) {
			t.Fatalf("invalid SOCKS5 UDP header: %v", packet)
		}
		addr, _, _, err := protocol.ReadAddress(bytes.NewReader(packet[3:]))
		if err != nil || addr != "example.com:53" {
			t.Fatalf("SOCKS5 UDP target = %q, err = %v", addr, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SOCKS5 UDP relay did not receive a packet")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close UDP association: %v", err)
	}
	select {
	case <-controlClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("SOCKS5 UDP control connection remained open after Close")
	}
}
