package app

import (
	"net"
	"testing"

	"github.com/SUDOKU-ASCII/sudoku/internal/config"
	"golang.org/x/net/proxy"
)

type fakeDialer struct {
	called bool
	conn   net.Conn
}

func (f *fakeDialer) Dial(network, addr string) (net.Conn, error) {
	f.called = true
	if f.conn == nil {
		client, _ := net.Pipe()
		f.conn = client
	}
	return f.conn, nil
}

func TestNewServerDialTarget_UsesProvidedDialer(t *testing.T) {
	fake := &fakeDialer{}

	dialTarget, err := newServerDialTarget(&config.Config{}, func(*config.Config) (proxy.Dialer, error) {
		return fake, nil
	})
	if err != nil {
		t.Fatalf("newServerDialTarget() error = %v", err)
	}

	conn, err := dialTarget("example.com:443")
	if err != nil {
		t.Fatalf("dialTarget() error = %v", err)
	}
	if conn == nil {
		t.Fatal("dialTarget() returned nil conn")
	}
	if !fake.called {
		t.Fatal("expected provided dialer to be used")
	}
	_ = conn.Close()
}
