package main

import (
	"bufio"
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
