package apis

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg, table, err := selectedTable(&Config{})
	if err != nil {
		t.Fatalf("selectedTable failed: %v", err)
	}
	if table == nil {
		t.Fatalf("expected table")
	}
	if cfg.Key != DefaultKey {
		t.Fatalf("default key mismatch: %q", cfg.Key)
	}
	if cfg.ASCII != DefaultASCII {
		t.Fatalf("default ascii mismatch: %q", cfg.ASCII)
	}
	if cfg.EnablePureDownlink {
		t.Fatalf("default downlink should be packed")
	}
}

func TestClientServerConnRoundTrip(t *testing.T) {
	for _, pure := range []bool{false, true} {
		t.Run(fmt.Sprintf("pure_downlink=%t", pure), func(t *testing.T) {
			cfg := &Config{
				Key:                "roundtrip-key",
				ASCII:              "prefer_entropy",
				EnablePureDownlink: pure,
			}
			assertRoundTrip(t, cfg)
		})
	}
}

func TestClientServerConnCustomTableIndex(t *testing.T) {
	cfg := &Config{
		Key:          "custom-key",
		ASCII:        "up_ascii_down_entropy",
		CustomTables: []string{"xpxvvpvv", "vxpvxvvp"},
		TableIndex:   1,
	}
	assertRoundTrip(t, cfg)
}

func TestMuxOverWrappedConn(t *testing.T) {
	cfg := &Config{
		Key:   "mux-key",
		ASCII: "prefer_entropy",
	}
	rawClient, rawServer := net.Pipe()
	client, err := ClientConn(rawClient, cfg)
	if err != nil {
		t.Fatalf("ClientConn failed: %v", err)
	}
	server, err := ServerConn(rawServer, cfg)
	if err != nil {
		t.Fatalf("ServerConn failed: %v", err)
	}
	defer client.Close()
	defer server.Close()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- HandleMuxWithDialer(server, nil, func(targetAddr string) (net.Conn, error) {
			if targetAddr != "example.com:443" {
				return nil, fmt.Errorf("unexpected target: %s", targetAddr)
			}
			target, peer := net.Pipe()
			go func() {
				defer peer.Close()
				_, _ = io.Copy(peer, peer)
			}()
			return target, nil
		})
	}()

	mux, err := NewMuxClient(client)
	if err != nil {
		t.Fatalf("NewMuxClient failed: %v", err)
	}
	stream, err := mux.Dial("example.com:443")
	if err != nil {
		t.Fatalf("mux dial failed: %v", err)
	}

	payload := []byte("mux payload")
	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("mux write failed: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("mux read failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("mux echo mismatch: got %q want %q", got, payload)
	}

	_ = stream.Close()
	_ = mux.Close()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("mux server failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("mux server did not stop")
	}
}

func assertRoundTrip(t *testing.T, cfg *Config) {
	t.Helper()

	rawClient, rawServer := net.Pipe()
	client, err := ClientConn(rawClient, cfg)
	if err != nil {
		t.Fatalf("ClientConn failed: %v", err)
	}
	server, err := ServerConn(rawServer, cfg)
	if err != nil {
		t.Fatalf("ServerConn failed: %v", err)
	}
	defer client.Close()
	defer server.Close()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))

	payload := []byte("client to server")
	response := []byte("server to client")
	errs := make(chan error, 2)
	go func() {
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(server, got); err != nil {
			errs <- err
			return
		}
		if !bytes.Equal(got, payload) {
			errs <- fmt.Errorf("server got %q want %q", got, payload)
			return
		}
		_, err := server.Write(response)
		errs <- err
	}()

	if _, err := client.Write(payload); err != nil {
		t.Fatalf("client write failed: %v", err)
	}
	got := make([]byte, len(response))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("client read failed: %v", err)
	}
	if !bytes.Equal(got, response) {
		t.Fatalf("client got %q want %q", got, response)
	}
	if err := <-errs; err != nil {
		t.Fatalf("server failed: %v", err)
	}
}
