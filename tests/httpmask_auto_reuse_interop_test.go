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

func TestOfficialInterop_HTTPMaskDownlinkHalfCloseKeepsUplink(t *testing.T) {
	for _, mode := range []string{"stream", "poll", "auto"} {
		t.Run(mode, func(t *testing.T) {
			target, targetResult, stopTarget := startDirectionalHalfCloseServer(t)
			defer stopTarget()

			serverKey, clientKey := newTestKeys(t)
			ports, err := getFreePorts(2)
			if err != nil {
				t.Fatalf("ports: %v", err)
			}

			serverCfg := newTestServerConfig(ports[0], serverKey)
			serverCfg.HTTPMask = config.HTTPMaskConfig{
				Mode:      mode,
				Multiplex: "auto",
			}
			startSudokuServer(t, serverCfg)

			clientCfg := newTestClientConfig(ports[1], localServerAddr(ports[0]), clientKey)
			clientCfg.ProxyMode = "global"
			clientCfg.HTTPMask = config.HTTPMaskConfig{
				Mode:      mode,
				Multiplex: "auto",
			}
			startSudokuClient(t, clientCfg)

			if err := runOfficialDirectionalHalfCloseExchange(ports[1], target); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-targetResult:
				if result.err != nil {
					t.Fatalf("target exchange: %v", result.err)
				}
				if string(result.tail) != "tail" {
					t.Fatalf("target tail = %q", result.tail)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("target did not receive tail")
			}
		})
	}
}

func runOfficialDirectionalHalfCloseExchange(clientPort int, target string) error {
	conn, err := dialOfficialProxyTunnel(clientPort, target)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := writeFullConn(conn, []byte("start")); err != nil {
		return fmt.Errorf("write start: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	response, err := io.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("read half-closed response: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	if string(response) != "response" {
		return fmt.Errorf("response = %q", response)
	}
	if err := writeFullConn(conn, []byte("tail")); err != nil {
		return fmt.Errorf("write tail after EOF: %w", err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		return fmt.Errorf("close tail: %w", err)
	}
	return nil
}

type directionalHalfCloseResult struct {
	tail []byte
	err  error
}

func startDirectionalHalfCloseServer(t testing.TB) (string, <-chan directionalHalfCloseResult, func()) {
	t.Helper()
	result := make(chan directionalHalfCloseResult, 1)
	addr, stop := startTCPTestServer(t, func(conn net.Conn) {
		start := make([]byte, len("start"))
		if _, err := io.ReadFull(conn, start); err != nil {
			result <- directionalHalfCloseResult{err: err}
			return
		}
		if string(start) != "start" {
			result <- directionalHalfCloseResult{err: fmt.Errorf("start = %q", start)}
			return
		}
		if _, err := conn.Write([]byte("response")); err != nil {
			result <- directionalHalfCloseResult{err: err}
			return
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			result <- directionalHalfCloseResult{err: fmt.Errorf("target conn is %T", conn)}
			return
		}
		if err := tcpConn.CloseWrite(); err != nil {
			result <- directionalHalfCloseResult{err: err}
			return
		}
		tail, err := io.ReadAll(conn)
		result <- directionalHalfCloseResult{tail: tail, err: err}
	})
	return addr, result, stop
}

func runOfficialHalfCloseExchange(clientPort int, target string, id int) error {
	conn, err := dialOfficialProxyTunnel(clientPort, target)
	if err != nil {
		return fmt.Errorf("connect %d: %w", id, err)
	}
	defer conn.Close()

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

func dialOfficialProxyTunnel(clientPort int, target string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", localServerAddr(clientPort), 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial official client: %w", err)
	}

	request := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	if err := writeFullConn(conn, []byte(request)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}
	var response [1024]byte
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	n, err := conn.Read(response[:])
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read CONNECT: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	if !bytes.Contains(response[:n], []byte("200 Connection Established")) {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNECT rejected: %q", response[:n])
	}
	return conn, nil
}

func startDelayedHalfCloseServer(t testing.TB, delay time.Duration) (string, func()) {
	t.Helper()
	return startTCPTestServer(t, func(conn net.Conn) {
		payload, err := io.ReadAll(conn)
		if err != nil {
			return
		}
		time.Sleep(delay)
		_, _ = conn.Write(append([]byte("response:"), payload...))
	})
}

func startTCPTestServer(t testing.TB, handle func(net.Conn)) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen test server: %v", err)
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
				handle(conn)
			}()
		}
	}()

	return listener.Addr().String(), func() {
		_ = listener.Close()
		<-done
		wg.Wait()
	}
}
