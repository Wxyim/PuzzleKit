package tests

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/SUDOKU-ASCII/sudoku/internal/config"
)

func TestOfficialInterop_HTTPMaskAutoReuseHalfClose(t *testing.T) {
	target, stopTarget := startDelayedHalfCloseServer(t, 40*time.Millisecond)
	defer stopTarget()

	serverKey, clientKey := newTestKeys(t)
	ports, err := getFreePorts(2)
	if err != nil {
		t.Fatalf("ports: %v", err)
	}

	serverCfg := newTestServerConfig(ports[0], serverKey)
	serverCfg.HTTPMask = config.HTTPMaskConfig{
		Mode:      "auto",
		Multiplex: "auto",
	}
	startSudokuServer(t, serverCfg)

	clientCfg := newTestClientConfig(ports[1], localServerAddr(ports[0]), clientKey)
	clientCfg.ProxyMode = "global"
	clientCfg.HTTPMask = config.HTTPMaskConfig{
		Mode:      "auto",
		Multiplex: "auto",
	}
	startSudokuClient(t, clientCfg)

	const connections = 16
	errCh := make(chan error, connections)
	var wg sync.WaitGroup
	wg.Add(connections)
	for i := 0; i < connections; i++ {
		i := i
		go func() {
			defer wg.Done()
			errCh <- runOfficialHalfCloseExchange(ports[1], target, i)
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func runOfficialHalfCloseExchange(clientPort int, target string, id int) error {
	conn, err := net.DialTimeout("tcp", localServerAddr(clientPort), 3*time.Second)
	if err != nil {
		return fmt.Errorf("dial official client %d: %w", id, err)
	}
	defer conn.Close()

	request := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	if err := writeFullConn(conn, []byte(request)); err != nil {
		return fmt.Errorf("write CONNECT %d: %w", id, err)
	}
	var connectResponse [1024]byte
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	n, err := conn.Read(connectResponse[:])
	if err != nil {
		return fmt.Errorf("read CONNECT %d: %w", id, err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	if !bytes.Contains(connectResponse[:n], []byte("200 Connection Established")) {
		return fmt.Errorf("CONNECT %d rejected: %q", id, connectResponse[:n])
	}

	payload := []byte(fmt.Sprintf("auto-reuse-%02d", id))
	if err := writeFullConn(conn, payload); err != nil {
		return fmt.Errorf("write payload %d: %w", id, err)
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("client conn %d is %T, want *net.TCPConn", id, conn)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		return fmt.Errorf("half-close %d: %w", id, err)
	}

	want := append([]byte("response:"), payload...)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("read delayed response %d: %w", id, err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("response %d = %q, want %q", id, got, want)
	}
	return nil
}

func startDelayedHalfCloseServer(t testing.TB, delay time.Duration) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen delayed server: %v", err)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				payload, err := io.ReadAll(conn)
				if err != nil {
					return
				}
				time.Sleep(delay)
				_, _ = conn.Write(append([]byte("response:"), payload...))
			}()
		}
	}()

	return listener.Addr().String(), func() {
		_ = listener.Close()
		<-done
		wg.Wait()
	}
}
