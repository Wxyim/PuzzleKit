package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SUDOKU-ASCII/sudoku/internal/config"
	"github.com/SUDOKU-ASCII/sudoku/internal/tunnel"
	"github.com/SUDOKU-ASCII/sudoku/pkg/connutil"
	"github.com/SUDOKU-ASCII/sudoku/pkg/obfs/httpmask"
	"github.com/SUDOKU-ASCII/sudoku/pkg/obfs/sudoku"
)

const wanSimulationEnv = "SUDOKU_WAN_SIM"

type wanProfile struct {
	oneWayDelay    time.Duration
	jitter         time.Duration
	retransmitEach int64
	retransmitWait time.Duration
	idleTimeout    time.Duration
}

type wanProxy struct {
	listener net.Listener
	backend  string
	profile  wanProfile

	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
}

func startWANProxy(t testing.TB, backend string, profile wanProfile) *wanProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen WAN proxy: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &wanProxy{
		listener: listener,
		backend:  backend,
		profile:  profile,
		cancel:   cancel,
		conns:    make(map[net.Conn]struct{}),
	}
	proxy.wg.Add(1)
	go proxy.serve(ctx)
	t.Cleanup(proxy.close)
	return proxy
}

func (p *wanProxy) addr() string {
	return p.listener.Addr().String()
}

func (p *wanProxy) serve(ctx context.Context) {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.handleConn(ctx, client)
		}()
	}
}

func (p *wanProxy) handleConn(ctx context.Context, client net.Conn) {
	server, err := net.DialTimeout("tcp", p.backend, 3*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client, server)
	defer func() {
		p.untrack(client, server)
		_ = client.Close()
		_ = server.Close()
	}()

	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().UnixNano())
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if p.profile.idleTimeout > 0 {
		go func() {
			interval := p.profile.idleTimeout / 4
			if interval > time.Second {
				interval = time.Second
			}
			timer := time.NewTicker(interval)
			defer timer.Stop()
			for {
				select {
				case <-timer.C:
					last := time.Unix(0, lastActivity.Load())
					if time.Since(last) >= p.profile.idleTimeout {
						_ = client.Close()
						_ = server.Close()
						return
					}
				case <-connCtx.Done():
					return
				}
			}
		}()
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		copyWAN(server, client, p.profile, 1, &lastActivity)
	}()
	go func() {
		defer wg.Done()
		copyWAN(client, server, p.profile, 2, &lastActivity)
	}()
	wg.Wait()
}

func copyWAN(dst, src net.Conn, profile wanProfile, direction int64, lastActivity *atomic.Int64) {
	buf := make([]byte, 32*1024)
	var delivered int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			delivered++
			delay := profile.oneWayDelay + deterministicJitter(profile.jitter, delivered, direction)
			if profile.retransmitEach > 0 && delivered%profile.retransmitEach == 0 {
				delay += profile.retransmitWait
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			if writeFull(dst, buf[:n]) != nil {
				return
			}
			lastActivity.Store(time.Now().UnixNano())
		}
		if err != nil {
			if closeWriter, ok := dst.(interface{ CloseWrite() error }); ok {
				_ = closeWriter.CloseWrite()
			}
			return
		}
	}
}

func deterministicJitter(max time.Duration, delivered, direction int64) time.Duration {
	if max <= 0 {
		return 0
	}
	phase := (delivered*7 + direction*3) % 5
	return time.Duration(phase-2) * max / 2
}

func (p *wanProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	for _, conn := range conns {
		p.conns[conn] = struct{}{}
	}
	p.mu.Unlock()
}

func (p *wanProxy) untrack(conns ...net.Conn) {
	p.mu.Lock()
	for _, conn := range conns {
		delete(p.conns, conn)
	}
	p.mu.Unlock()
}

func (p *wanProxy) close() {
	if p == nil {
		return
	}
	p.cancel()
	_ = p.listener.Close()
	p.mu.Lock()
	for conn := range p.conns {
		_ = conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func TestWAN200msMuxHTTP204Latency(t *testing.T) {
	requireWANSimulation(t)

	harness := newWANMuxHarness(t, wanProfile{oneWayDelay: 100 * time.Millisecond})
	const samplesPerMode = 7
	results := make(map[string][]time.Duration)

	for _, mode := range []string{"disable", "ws", "auto", "stream"} {
		t.Run(mode, func(t *testing.T) {
			clientCfg := newWANClientConfig(t, mode, harness.proxy.addr())
			dialer := &tunnel.MuxDialer{BaseDialer: tunnel.BaseDialer{
				Config: clientCfg,
				Tables: harness.tables,
			}}
			defer dialer.Close()

			warmCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := dialer.Warm(warmCtx); err != nil {
				t.Fatalf("warm %s mux: %v", mode, err)
			}

			samples := make([]time.Duration, 0, samplesPerMode)
			for range samplesPerMode {
				elapsed, err := measureMuxHTTP204(dialer, harness.originAddr)
				if err != nil {
					t.Fatalf("%s HTTP 204: %v", mode, err)
				}
				samples = append(samples, elapsed)
				time.Sleep(250 * time.Millisecond)
			}
			results[mode] = samples
			t.Logf("%s 204 latency: p50=%v p95=%v samples=%v",
				mode, percentileDuration(samples, 50), percentileDuration(samples, 95), samples)
		})
	}

	wsP50 := percentileDuration(results["ws"], 50)
	wsP95 := percentileDuration(results["ws"], 95)
	const parityTolerance = 60 * time.Millisecond
	for _, mode := range []string{"auto", "stream"} {
		if got := percentileDuration(results[mode], 50); got > wsP50+parityTolerance {
			t.Fatalf("%s p50=%v exceeds ws p50=%v by more than %v", mode, got, wsP50, parityTolerance)
		}
		if got := percentileDuration(results[mode], 95); got > wsP95+parityTolerance {
			t.Fatalf("%s p95=%v exceeds ws p95=%v by more than %v", mode, got, wsP95, parityTolerance)
		}
	}
}

func TestWAN200msLossyStreamMuxStability(t *testing.T) {
	requireWANSimulation(t)

	duration := wanStabilityDuration(t)
	harness := newWANMuxHarness(t, wanProfile{
		oneWayDelay:    100 * time.Millisecond,
		jitter:         20 * time.Millisecond,
		retransmitEach: 23,
		retransmitWait: 200 * time.Millisecond,
	})
	clientCfg := newWANClientConfig(t, "auto", harness.proxy.addr())
	base := tunnel.BaseDialer{Config: clientCfg, Tables: harness.tables}
	baseConn, err := base.DialBase()
	if err != nil {
		t.Fatalf("dial warmed stream carrier: %v", err)
	}
	if err := tunnel.WriteKIPMessage(baseConn, tunnel.KIPTypeStartMux, nil); err != nil {
		_ = baseConn.Close()
		t.Fatalf("start mux: %v", err)
	}
	readyCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpmask.WaitTunnelReady(readyCtx, baseConn); err != nil {
		_ = baseConn.Close()
		t.Fatalf("wait stream carrier ready: %v", err)
	}
	muxClient, err := tunnel.NewMuxClient(baseConn)
	if err != nil {
		_ = baseConn.Close()
		t.Fatalf("new mux client: %v", err)
	}
	defer muxClient.Close()

	deadline := time.Now().Add(duration)
	var requests int
	var worst time.Duration
	for time.Now().Before(deadline) {
		elapsed, err := measureMuxHTTP204(muxClient, harness.originAddr)
		if err != nil {
			t.Fatalf("same-session 204 failed after %v and %d requests: %v", duration-time.Until(deadline), requests, err)
		}
		requests++
		if elapsed > worst {
			worst = elapsed
		}
		time.Sleep(500 * time.Millisecond)
	}
	select {
	case <-muxClient.Done():
		t.Fatalf("mux carrier ended during stability run: %v", muxClient.Err())
	default:
	}
	t.Logf("loss-like WAN stability: duration=%v requests=%d worst_204=%v", duration, requests, worst)
}

func TestWAN200msLossyAutoReuseHalfClose(t *testing.T) {
	requireWANSimulation(t)

	harness := newWANMuxHarness(t, wanProfile{
		oneWayDelay:    100 * time.Millisecond,
		jitter:         20 * time.Millisecond,
		retransmitEach: 11,
		retransmitWait: 200 * time.Millisecond,
	})
	targetAddr := startWANHalfCloseTarget(t, 50*time.Millisecond)
	clientCfg := newWANClientConfig(t, "auto", harness.proxy.addr())
	clientCfg.Multiplex = "auto"
	clientCfg.HTTPMask.Multiplex = "auto"
	if err := clientCfg.Finalize(); err != nil {
		t.Fatalf("finalize auto-reuse WAN client: %v", err)
	}
	dialer := &tunnel.StandardDialer{BaseDialer: tunnel.BaseDialer{
		Config: clientCfg,
		Tables: harness.tables,
	}}

	for i := range 12 {
		conn, err := dialer.Dial(targetAddr)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		payload := []byte(fmt.Sprintf("wan-auto-reuse-%02d", i))
		if err := writeFull(conn, payload); err != nil {
			_ = conn.Close()
			t.Fatalf("write %d: %v", i, err)
		}
		if closeWriter, ok := conn.(interface{ CloseWrite() error }); !ok {
			_ = conn.Close()
			t.Fatalf("connection %d does not support CloseWrite", i)
		} else if err := closeWriter.CloseWrite(); err != nil {
			_ = conn.Close()
			t.Fatalf("close write %d: %v", i, err)
		}

		want := append([]byte("response:"), payload...)
		readDone := make(chan struct {
			payload []byte
			err     error
		}, 1)
		go func() {
			got, err := io.ReadAll(conn)
			readDone <- struct {
				payload []byte
				err     error
			}{payload: got, err: err}
		}()
		select {
		case result := <-readDone:
			_ = conn.Close()
			if result.err != nil {
				t.Fatalf("read %d: %v", i, result.err)
			}
			if !bytes.Equal(result.payload, want) {
				t.Fatalf("response %d = %q, want %q", i, result.payload, want)
			}
		case <-time.After(10 * time.Second):
			_ = conn.Close()
			t.Fatalf("read %d timed out", i)
		}
	}
}

func TestWAN200msLossyDownlinkHalfCloseKeepsUplink(t *testing.T) {
	requireWANSimulation(t)

	harness := newWANMuxHarness(t, wanProfile{
		oneWayDelay:    100 * time.Millisecond,
		jitter:         20 * time.Millisecond,
		retransmitEach: 11,
		retransmitWait: 200 * time.Millisecond,
	})
	targetAddr, targetResults := startWANDirectionalHalfCloseTarget(t)

	for _, mode := range []string{"stream", "poll", "auto"} {
		t.Run(mode, func(t *testing.T) {
			clientCfg := newWANClientConfig(t, mode, harness.proxy.addr())
			clientCfg.Multiplex = "auto"
			clientCfg.HTTPMask.Multiplex = "auto"
			if err := clientCfg.Finalize(); err != nil {
				t.Fatalf("finalize %s WAN client: %v", mode, err)
			}
			dialer := &tunnel.StandardDialer{BaseDialer: tunnel.BaseDialer{
				Config: clientCfg,
				Tables: harness.tables,
			}}

			conn, err := dialer.Dial(targetAddr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			relayed := relayWANConn(t, conn)
			defer relayed.Close()
			if err := writeFull(relayed, []byte("start")); err != nil {
				t.Fatalf("write start: %v", err)
			}
			response, err := io.ReadAll(relayed)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			if string(response) != "response" {
				t.Fatalf("response = %q", response)
			}
			if err := writeFull(relayed, []byte("tail")); err != nil {
				t.Fatalf("write tail after EOF: %v", err)
			}
			if closeWriter, ok := relayed.(interface{ CloseWrite() error }); !ok {
				t.Fatal("connection does not support CloseWrite")
			} else if err := closeWriter.CloseWrite(); err != nil {
				t.Fatalf("close tail: %v", err)
			}
			select {
			case result := <-targetResults:
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

func relayWANConn(t testing.TB, target net.Conn) net.Conn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = target.Close()
		t.Fatalf("listen relay: %v", err)
	}
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, err := listener.Accept()
		accepted <- acceptResult{conn: conn, err: err}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		_ = target.Close()
		t.Fatalf("dial relay: %v", err)
	}
	result := <-accepted
	_ = listener.Close()
	if result.err != nil {
		_ = client.Close()
		_ = target.Close()
		t.Fatalf("accept relay: %v", result.err)
	}
	proxy := result.conn
	go connutil.PipeConn(proxy, target)
	t.Cleanup(func() {
		_ = client.Close()
		_ = proxy.Close()
		_ = target.Close()
	})
	return client
}

func TestWANMuxKeepaliveSurvivesIdleCarrierTimeout(t *testing.T) {
	requireWANSimulation(t)

	harness := newWANMuxHarness(t, wanProfile{
		oneWayDelay: 100 * time.Millisecond,
		idleTimeout: 25 * time.Second,
	})
	clientCfg := newWANClientConfig(t, "disable", harness.proxy.addr())
	dialer := &tunnel.MuxDialer{BaseDialer: tunnel.BaseDialer{
		Config: clientCfg,
		Tables: harness.tables,
	}}
	defer dialer.Close()

	warmCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := dialer.Warm(warmCtx); err != nil {
		t.Fatalf("warm mux: %v", err)
	}
	time.Sleep(32 * time.Second)
	if _, err := measureMuxHTTP204(dialer, harness.originAddr); err != nil {
		t.Fatalf("mux carrier did not survive idle cutoff: %v", err)
	}
}

func startWANHalfCloseTarget(t testing.TB, delay time.Duration) string {
	t.Helper()
	return startWANTarget(t, func(conn net.Conn) {
		payload, err := io.ReadAll(conn)
		if err != nil {
			return
		}
		time.Sleep(delay)
		_, _ = conn.Write(append([]byte("response:"), payload...))
	})
}

type wanDirectionalHalfCloseResult struct {
	tail []byte
	err  error
}

func startWANDirectionalHalfCloseTarget(t testing.TB) (string, <-chan wanDirectionalHalfCloseResult) {
	t.Helper()
	results := make(chan wanDirectionalHalfCloseResult, 3)
	addr := startWANTarget(t, func(conn net.Conn) {
		start := make([]byte, len("start"))
		if _, err := io.ReadFull(conn, start); err != nil {
			results <- wanDirectionalHalfCloseResult{err: err}
			return
		}
		if string(start) != "start" {
			results <- wanDirectionalHalfCloseResult{err: fmt.Errorf("start = %q", start)}
			return
		}
		if _, err := conn.Write([]byte("response")); err != nil {
			results <- wanDirectionalHalfCloseResult{err: err}
			return
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			results <- wanDirectionalHalfCloseResult{err: fmt.Errorf("target conn is %T", conn)}
			return
		}
		if err := tcpConn.CloseWrite(); err != nil {
			results <- wanDirectionalHalfCloseResult{err: err}
			return
		}
		tail, err := io.ReadAll(conn)
		results <- wanDirectionalHalfCloseResult{tail: tail, err: err}
	})
	return addr, results
}

func startWANTarget(t testing.TB, handle func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen WAN target: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				handle(conn)
			}()
		}
	}()
	return listener.Addr().String()
}

type wanMuxHarness struct {
	originAddr string
	tables     []*sudoku.Table
	proxy      *wanProxy
}

func newWANMuxHarness(t testing.TB, profile wanProfile) *wanMuxHarness {
	t.Helper()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate_204" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(origin.Close)

	const key = "wan-mux-simulation-key"
	tables, err := getTables(key, "prefer_entropy", "default")
	if err != nil {
		t.Fatalf("build WAN tables: %v", err)
	}
	serverCfg := &config.Config{
		Mode:               "server",
		Transport:          "tcp",
		Key:                key,
		AEAD:               "chacha20-poly1305",
		SuspiciousAction:   "silent",
		PaddingMin:         0,
		PaddingMax:         0,
		ASCII:              "prefer_entropy",
		EnablePureDownlink: true,
		Multiplex:          "on",
		HTTPMask: config.HTTPMaskConfig{
			Mode:     "auto",
			PathRoot: "wan204",
		},
	}
	if err := serverCfg.Finalize(); err != nil {
		t.Fatalf("finalize WAN server config: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server, err := startSudokuServer(ctx, serverCfg, tables, false)
	if err != nil {
		t.Fatalf("start WAN Sudoku server: %v", err)
	}
	t.Cleanup(func() { _ = server.close() })

	proxy := startWANProxy(t, server.serverAddr, profile)
	return &wanMuxHarness{
		originAddr: strings.TrimPrefix(origin.URL, "http://"),
		tables:     tables,
		proxy:      proxy,
	}
}

func newWANClientConfig(t testing.TB, mode, serverAddr string) *config.Config {
	t.Helper()
	httpMode := mode
	disabled := mode == "disable"
	if disabled {
		httpMode = "legacy"
	}
	cfg := &config.Config{
		Mode:               "client",
		Transport:          "tcp",
		ServerAddress:      serverAddr,
		Key:                "wan-mux-simulation-key",
		AEAD:               "chacha20-poly1305",
		SuspiciousAction:   "silent",
		PaddingMin:         0,
		PaddingMax:         0,
		ASCII:              "prefer_entropy",
		EnablePureDownlink: true,
		Multiplex:          "on",
		HTTPMask: config.HTTPMaskConfig{
			Disable:  disabled,
			Mode:     httpMode,
			PathRoot: "wan204",
		},
	}
	if err := cfg.Finalize(); err != nil {
		t.Fatalf("finalize %s WAN client config: %v", mode, err)
	}
	return cfg
}

type muxHTTPDialer interface {
	Dial(string) (net.Conn, error)
}

func measureMuxHTTP204(dialer muxHTTPDialer, targetAddr string) (time.Duration, error) {
	start := time.Now()
	conn, err := dialer.Dial(targetAddr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	req, err := http.NewRequest(http.MethodGet, "http://"+targetAddr+"/generate_204", nil)
	if err != nil {
		return 0, err
	}
	req.Close = true
	if err := req.Write(conn); err != nil {
		return 0, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return 0, err
	}
	_, copyErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return 0, fmt.Errorf("status=%s", resp.Status)
	}
	if copyErr != nil {
		return 0, copyErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	return time.Since(start), nil
}

func percentileDuration(samples []time.Duration, percentile int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	index := (len(ordered)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(ordered) {
		index = len(ordered)
	}
	return ordered[index-1]
}

func requireWANSimulation(t testing.TB) {
	t.Helper()
	if os.Getenv(wanSimulationEnv) == "" {
		t.Skip("set " + wanSimulationEnv + "=1 to run the 200ms WAN simulation")
	}
}

func wanStabilityDuration(t testing.TB) time.Duration {
	t.Helper()
	const defaultDuration = 125 * time.Second
	raw := strings.TrimSpace(os.Getenv("SUDOKU_WAN_STABILITY_DURATION"))
	if raw == "" {
		return defaultDuration
	}
	if seconds, err := strconv.Atoi(raw); err == nil {
		return time.Duration(seconds) * time.Second
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid SUDOKU_WAN_STABILITY_DURATION=%q", raw)
	}
	return duration
}
