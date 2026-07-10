package tests

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SUDOKU-ASCII/sudoku/internal/config"
	"github.com/SUDOKU-ASCII/sudoku/internal/protocol"
	"github.com/SUDOKU-ASCII/sudoku/internal/tunnel"
	"github.com/SUDOKU-ASCII/sudoku/pkg/connutil"
	"github.com/SUDOKU-ASCII/sudoku/pkg/obfs/httpmask"
	"github.com/SUDOKU-ASCII/sudoku/pkg/obfs/sudoku"
)

const downlinkBenchmarkKey = "downlink-throughput-ci-key"

func BenchmarkDownlinkThroughputConcurrentMatrix(b *testing.B) {
	connCount := envInt(b, "SUDOKU_DOWNLINK_CONCURRENT_CONNS", 200)
	bytesPerConn := int64(envInt(b, "SUDOKU_DOWNLINK_CONCURRENT_BYTES", 1<<20))

	downlinks := []struct {
		name string
		pure bool
	}{
		{"pure", true},
		{"packed", false},
	}
	transports := []struct {
		name string
		mode string
	}{
		{"httpmask_off", "legacy"},
		{"httpmask_stream", "stream"},
		{"httpmask_poll", "poll"},
		{"httpmask_ws", "ws"},
	}
	muxModes := []string{"off", "auto", "on"}

	for _, dl := range downlinks {
		for _, tr := range transports {
			for _, muxMode := range muxModes {
				name := fmt.Sprintf("%s/%s/mux_%s", dl.name, tr.name, muxMode)
				b.Run(name, func(b *testing.B) {
					benchmarkDownlinkThroughputConcurrent(b, connCount, bytesPerConn, dl.pure, tr.mode, muxMode)
				})
			}
		}
	}
}

func benchmarkDownlinkThroughputConcurrent(b *testing.B, connCount int, bytesPerConn int64, pureDownlink bool, httpmaskMode, muxMode string) {
	b.Helper()

	source := newControlledDownlinkSource(bytesPerConn)
	bench := newDownlinkBenchHarness(b, pureDownlink, httpmaskMode, muxMode, source.dialTarget)
	totalBytes := int64(connCount) * bytesPerConn

	b.SetBytes(totalBytes)
	b.ReportMetric(float64(connCount), "conns")
	b.ReportMetric(float64(bytesPerConn), "bytes/conn")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		batch := source.newBatch(connCount)
		conns, err := openConcurrentBenchConns(bench.dialer, source.addr, connCount)
		if err != nil {
			closeBenchmarkConns(conns)
			b.Fatalf("open conns: %v", err)
		}
		if err := batch.waitReady(60 * time.Second); err != nil {
			closeBenchmarkConns(conns)
			b.Fatalf("wait source conns: %v", err)
		}

		b.StartTimer()
		err = runConcurrentDownlinkReads(conns, bytesPerConn, batch.release)
		b.StopTimer()
		closeBenchmarkConns(conns)
		if err != nil {
			b.Fatalf("download: %v; %s", err, batch.stats())
		}
	}
}

type downlinkBenchHarness struct {
	dialer tunnel.Dialer
}

func newDownlinkBenchHarness(tb testing.TB, pureDownlink bool, httpmaskMode, muxMode string, dialTarget func(string) (net.Conn, error)) *downlinkBenchHarness {
	tb.Helper()
	if dialTarget == nil {
		dialTarget = dialBenchmarkTarget
	}

	cfg := newBenchmarkConfig("server", pureDownlink, httpmaskMode, muxMode, "")
	table := sudoku.NewTable(downlinkBenchmarkKey, cfg.ASCII)
	if table == nil {
		tb.Fatal("nil sudoku table")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen tunnel: %v", err)
	}
	tb.Cleanup(func() { _ = ln.Close() })

	cfg.ServerAddress = ln.Addr().String()
	var tunnelSrv *httpmask.TunnelServer
	if cfg.HTTPMaskTunnelEnabled() {
		tunnelSrv = httpmask.NewTunnelServer(httpmask.TunnelServerOptions{
			Mode:     cfg.HTTPMask.Mode,
			AuthKey:  cfg.Key,
			PathRoot: cfg.HTTPMask.PathRoot,
			EarlyHandshake: tunnel.NewHTTPMaskServerEarlyHandshake(tunnel.EarlyCodecConfig{
				PSK:                cfg.Key,
				AEAD:               cfg.AEAD,
				EnablePureDownlink: cfg.EnablePureDownlink,
				PaddingMin:         cfg.PaddingMin,
				PaddingMax:         cfg.PaddingMax,
			}, []*sudoku.Table{table}, tunnel.AllowHandshakeReplay),
		})
	}
	go serveBenchmarkTunnel(ln, cfg, []*sudoku.Table{table}, tunnelSrv, dialTarget)

	clientCfg := newBenchmarkConfig("client", pureDownlink, httpmaskMode, muxMode, ln.Addr().String())
	base := tunnel.BaseDialer{Config: clientCfg, Tables: []*sudoku.Table{table}}
	if clientCfg.SessionMuxEnabled() {
		return &downlinkBenchHarness{dialer: &tunnel.MuxDialer{BaseDialer: base}}
	}
	return &downlinkBenchHarness{dialer: &tunnel.StandardDialer{BaseDialer: base}}
}

func newBenchmarkConfig(mode string, pureDownlink bool, httpmaskMode, muxMode, serverAddr string) *config.Config {
	disableHTTPMask := httpmaskMode == "legacy" || httpmaskMode == "off"
	cfg := &config.Config{
		Mode:               mode,
		Transport:          "tcp",
		ServerAddress:      serverAddr,
		Key:                downlinkBenchmarkKey,
		AEAD:               "chacha20-poly1305",
		SuspiciousAction:   "silent",
		PaddingMin:         0,
		PaddingMax:         0,
		ASCII:              "prefer_ascii",
		EnablePureDownlink: pureDownlink,
		Multiplex:          muxMode,
		HTTPMask: config.HTTPMaskConfig{
			Disable: disableHTTPMask,
			Mode:    httpmaskMode,
		},
	}
	if err := cfg.Finalize(); err != nil {
		panic(err)
	}
	return cfg
}

func serveBenchmarkTunnel(ln net.Listener, cfg *config.Config, tables []*sudoku.Table, tunnelSrv *httpmask.TunnelServer, dialTarget func(string) (net.Conn, error)) {
	for {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		go handleBenchmarkTunnelConn(raw, cfg, tables, tunnelSrv, dialTarget)
	}
}

func handleBenchmarkTunnelConn(raw net.Conn, cfg *config.Config, tables []*sudoku.Table, tunnelSrv *httpmask.TunnelServer, dialTarget func(string) (net.Conn, error)) {
	if tunnelSrv != nil {
		res, c, err := tunnelSrv.HandleConn(raw)
		if err != nil {
			_ = raw.Close()
			return
		}
		switch res {
		case httpmask.HandleDone:
			return
		case httpmask.HandleStartTunnel:
			inner := *cfg
			inner.HTTPMask.Disable = true
			handleBenchmarkSudokuConn(c, &inner, tables, dialTarget)
			return
		case httpmask.HandlePassThrough:
			handleBenchmarkSudokuConn(c, cfg, tables, dialTarget)
			return
		default:
			_ = raw.Close()
			return
		}
	}
	handleBenchmarkSudokuConn(raw, cfg, tables, dialTarget)
}

func handleBenchmarkSudokuConn(raw net.Conn, cfg *config.Config, tables []*sudoku.Table, dialTarget func(string) (net.Conn, error)) {
	conn, _, err := tunnel.HandshakeAndUpgradeWithTablesMeta(raw, cfg, tables)
	if err != nil {
		_ = raw.Close()
		return
	}

	msg, err := readBenchmarkSessionMessage(conn)
	if err != nil {
		_ = conn.Close()
		return
	}
	switch msg.Type {
	case tunnel.KIPTypeOpenTCP:
		targetAddr, _, _, err := protocol.ReadAddress(bytes.NewReader(msg.Payload))
		if err != nil {
			_ = conn.Close()
			return
		}
		target, err := dialTarget(targetAddr)
		if err != nil {
			_ = conn.Close()
			return
		}
		connutil.PipeConn(conn, target)
	case tunnel.KIPTypeStartMux:
		_ = tunnel.HandleMuxWithDialer(conn, nil, dialTarget)
	default:
		_ = conn.Close()
	}
}

func readBenchmarkSessionMessage(conn net.Conn) (*tunnel.KIPMessage, error) {
	for {
		msg, err := tunnel.ReadKIPMessage(conn)
		if err != nil {
			return nil, err
		}
		if msg.Type != tunnel.KIPTypeKeepAlive {
			return msg, nil
		}
	}
}

type controlledDownlinkSource struct {
	addr         string
	bytesPerConn int64
	mu           sync.Mutex
	batch        *downlinkSourceBatch
}

type downlinkSourceBatch struct {
	want      int
	ready     int
	done      int
	written   int64
	firstErr  error
	readyCh   chan struct{}
	releaseCh chan struct{}
	mu        sync.Mutex
}

func newControlledDownlinkSource(bytesPerConn int64) *controlledDownlinkSource {
	return &controlledDownlinkSource{addr: "bench.downlink.invalid:1", bytesPerConn: bytesPerConn}
}

func (s *controlledDownlinkSource) newBatch(want int) *downlinkSourceBatch {
	b := &downlinkSourceBatch{
		want:      want,
		readyCh:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
	s.mu.Lock()
	s.batch = b
	s.mu.Unlock()
	return b
}

func (b *downlinkSourceBatch) markReady() {
	b.mu.Lock()
	b.ready++
	if b.ready == b.want {
		close(b.readyCh)
	}
	b.mu.Unlock()
}

func (b *downlinkSourceBatch) waitReady(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-b.readyCh:
		return nil
	case <-timer.C:
		b.mu.Lock()
		ready := b.ready
		b.mu.Unlock()
		return fmt.Errorf("only %d/%d source conns ready", ready, b.want)
	}
}

func (b *downlinkSourceBatch) release() {
	close(b.releaseCh)
}

func (b *downlinkSourceBatch) markDone(written int64, err error) {
	b.mu.Lock()
	b.done++
	b.written += written
	if err != nil && b.firstErr == nil {
		b.firstErr = err
	}
	b.mu.Unlock()
}

func (b *downlinkSourceBatch) stats() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.firstErr != nil {
		return fmt.Sprintf("source ready=%d/%d done=%d written=%d firstErr=%v", b.ready, b.want, b.done, b.written, b.firstErr)
	}
	return fmt.Sprintf("source ready=%d/%d done=%d written=%d", b.ready, b.want, b.done, b.written)
}

func dialBenchmarkTarget(targetAddr string) (net.Conn, error) {
	if targetAddr == "bench.downlink.invalid:1" {
		return nil, fmt.Errorf("downlink source not installed")
	}
	return net.DialTimeout("tcp", targetAddr, 5*time.Second)
}

func (s *controlledDownlinkSource) dialTarget(targetAddr string) (net.Conn, error) {
	if targetAddr != s.addr {
		return dialBenchmarkTarget(targetAddr)
	}
	target, source := net.Pipe()
	s.mu.Lock()
	batch := s.batch
	s.mu.Unlock()
	go serveControlledDownlinkPipe(source, batch, s.bytesPerConn)
	return target, nil
}

func serveControlledDownlinkPipe(conn net.Conn, batch *downlinkSourceBatch, bytesPerConn int64) {
	defer conn.Close()
	if err := readBenchmarkPrelude(conn); err != nil {
		if batch != nil {
			batch.markDone(0, err)
		}
		return
	}
	if batch != nil {
		batch.markReady()
		<-batch.releaseCh
	}
	written, err := writeBenchmarkPayload(conn, bytesPerConn)
	if batch != nil {
		batch.markDone(written, err)
	}
	finishBenchmarkSourceConn(conn)
}

func openConcurrentBenchConns(dialer tunnel.Dialer, targetAddr string, connCount int) ([]net.Conn, error) {
	conns := make([]net.Conn, 0, connCount)
	for i := 0; i < connCount; i++ {
		conn, err := dialer.Dial(targetAddr)
		if err != nil {
			return conns, err
		}
		if _, err := conn.Write([]byte{0}); err != nil {
			_ = conn.Close()
			return conns, err
		}
		conns = append(conns, conn)
	}
	return conns, nil
}

func closeBenchmarkConns(conns []net.Conn) {
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func runConcurrentDownlinkReads(conns []net.Conn, bytesPerConn int64, releaseSource func()) error {
	errCh := make(chan error, len(conns))
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(len(conns))
	for idx, conn := range conns {
		go func(idx int, conn net.Conn) {
			ready.Done()
			<-start
			errCh <- readOneDownlinkConn(idx, conn, bytesPerConn)
		}(idx, conn)
	}
	ready.Wait()
	close(start)
	if releaseSource != nil {
		releaseSource()
	}

	var firstErr error
	for i := 0; i < len(conns); i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func readOneDownlinkConn(idx int, conn net.Conn, bytesPerConn int64) error {
	buf := make([]byte, 32*1024)
	n, err := copyExactly(io.Discard, conn, buf, bytesPerConn)
	if err != nil {
		return fmt.Errorf("conn %d read after %d/%d bytes: %w", idx, n, bytesPerConn, err)
	}
	if n != bytesPerConn {
		return fmt.Errorf("conn %d downloaded %d, want %d", idx, n, bytesPerConn)
	}
	return nil
}

func copyExactly(dst io.Writer, src io.Reader, buf []byte, want int64) (int64, error) {
	var copied int64
	for copied < want {
		readBuf := buf
		if remaining := want - copied; remaining < int64(len(readBuf)) {
			readBuf = readBuf[:remaining]
		}
		nr, er := src.Read(readBuf)
		if nr > 0 {
			nw, ew := dst.Write(readBuf[:nr])
			copied += int64(nw)
			if ew != nil {
				return copied, ew
			}
			if nw != nr {
				return copied, io.ErrShortWrite
			}
		}
		if er != nil {
			return copied, er
		}
	}
	return copied, nil
}

func writeBenchmarkPayload(conn net.Conn, bytesPerConn int64) (int64, error) {
	chunk := make([]byte, 64*1024)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	var written int64
	for written < bytesPerConn {
		n := len(chunk)
		if remaining := bytesPerConn - written; remaining < int64(n) {
			n = int(remaining)
		}
		if err := writeFullConn(conn, chunk[:n]); err != nil {
			return written, err
		}
		written += int64(n)
	}
	return written, nil
}

func readBenchmarkPrelude(conn net.Conn) error {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var b [1]byte
	_, err := io.ReadFull(conn, b[:])
	_ = conn.SetReadDeadline(time.Time{})
	return err
}

func finishBenchmarkSourceConn(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.Copy(io.Discard, conn)
	_ = conn.SetReadDeadline(time.Time{})
}

func envInt(tb testing.TB, name string, fallback int) int {
	tb.Helper()
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		tb.Fatalf("invalid %s=%q", name, v)
	}
	return n
}
