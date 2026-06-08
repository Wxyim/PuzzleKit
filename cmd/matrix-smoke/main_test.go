package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/SUDOKU-ASCII/sudoku/internal/config"
	"github.com/SUDOKU-ASCII/sudoku/internal/tunnel"
	"github.com/SUDOKU-ASCII/sudoku/pkg/obfs/sudoku"
)

func TestMatrixSmoke(t *testing.T) {
	t.Helper()

	*flagFailFast = true
	*flagVerbose = false
	*flagTimeout = 10 * time.Second
	*flagPayload = 64 // KiB
	*flagQuick = testing.Short()

	all := dedupeCombos(combos(*flagQuick))

	for _, tc := range all {
		tc := tc
		name := fmt.Sprintf(
			"dl=%t_hm=%t_mode=%s_mux=%s_root=%s_ascii=%s_tables=%s",
			tc.enablePureDownlink,
			tc.httpmaskEnabled,
			tc.httpmaskMode,
			tc.mux,
			func() string {
				if tc.pathRoot == "" {
					return "none"
				}
				return tc.pathRoot
			}(),
			tc.asciiMode,
			tc.tableSet,
		)
		t.Run(name, func(t *testing.T) {
			if err := runOne(tc); err != nil {
				t.Fatalf("matrix smoke failed: %v", err)
			}
		})
	}
}

func TestHTTPMaskRTTParity(t *testing.T) {
	t.Helper()
	t.Skip("httpmask RTT parity is timing-sensitive and flaky under CI/load")

	cases := []combo{
		{enablePureDownlink: true, httpmaskEnabled: true, mux: "off", httpmaskMode: "auto", pathRoot: "aabbcc", asciiMode: "prefer_entropy", tableSet: "default"},
		{enablePureDownlink: false, httpmaskEnabled: true, mux: "off", httpmaskMode: "ws", pathRoot: "", asciiMode: "prefer_ascii", tableSet: "custom7"},
		{enablePureDownlink: true, httpmaskEnabled: true, mux: "off", httpmaskMode: "auto", pathRoot: "", asciiMode: "up_ascii_down_entropy", tableSet: "custom7"},
		{enablePureDownlink: false, httpmaskEnabled: true, mux: "off", httpmaskMode: "ws", pathRoot: "aabbcc", asciiMode: "up_entropy_down_ascii", tableSet: "custom7"},
	}

	for _, tc := range cases {
		tc := tc.canonical()
		t.Run(tc.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fallbackAddr, closeFallback, err := startFallbackHTTPServer(ctx)
			if err != nil {
				t.Fatalf("fallback server: %v", err)
			}
			defer closeFallback()

			echoAddr, closeEcho, err := startTCPEchoServer(ctx)
			if err != nil {
				t.Fatalf("echo server: %v", err)
			}
			defer closeEcho()

			const seedKey = "matrix-smoke-key"
			tables, err := getTables(seedKey, tc.asciiMode, tc.tableSet)
			if err != nil {
				t.Fatalf("tables: %v", err)
			}

			onCfg, err := newMatrixConfig("server", seedKey, tc, "", fallbackAddr)
			if err != nil {
				t.Fatalf("httpmask config: %v", err)
			}
			offCombo := tc
			offCombo.httpmaskEnabled = false
			offCfg, err := newMatrixConfig("server", seedKey, offCombo, "", fallbackAddr)
			if err != nil {
				t.Fatalf("baseline config: %v", err)
			}

			onSrv, err := startSudokuServer(ctx, onCfg, tables, false)
			if err != nil {
				t.Fatalf("start httpmask server: %v", err)
			}
			defer onSrv.close()

			offSrv, err := startSudokuServer(ctx, offCfg, tables, false)
			if err != nil {
				t.Fatalf("start baseline server: %v", err)
			}
			defer offSrv.close()

			const oneWayDelay = 40 * time.Millisecond
			onProxyAddr, closeOnProxy, err := startDelayProxy(ctx, onSrv.serverAddr, oneWayDelay)
			if err != nil {
				t.Fatalf("start httpmask proxy: %v", err)
			}
			defer closeOnProxy()

			offProxyAddr, closeOffProxy, err := startDelayProxy(ctx, offSrv.serverAddr, oneWayDelay)
			if err != nil {
				t.Fatalf("start baseline proxy: %v", err)
			}
			defer closeOffProxy()

			onClient, err := newMatrixConfig("client", seedKey, tc, onProxyAddr, "")
			if err != nil {
				t.Fatalf("httpmask client config: %v", err)
			}
			offClient, err := newMatrixConfig("client", seedKey, offCombo, offProxyAddr, "")
			if err != nil {
				t.Fatalf("baseline client config: %v", err)
			}

			warmupRuns := 2
			sampleRuns := 5
			for i := 0; i < warmupRuns; i++ {
				if _, err := measureFirstEchoRTT(context.Background(), onClient, tables, echoAddr); err != nil {
					t.Fatalf("warm up httpmask rtt: %v", err)
				}
				if _, err := measureFirstEchoRTT(context.Background(), offClient, tables, echoAddr); err != nil {
					t.Fatalf("warm up baseline rtt: %v", err)
				}
			}

			enabledSamples := make([]time.Duration, 0, sampleRuns)
			baselineSamples := make([]time.Duration, 0, sampleRuns)
			for i := 0; i < sampleRuns; i++ {
				if i%2 == 0 {
					enabledDur, err := measureFirstEchoRTT(context.Background(), onClient, tables, echoAddr)
					if err != nil {
						t.Fatalf("measure httpmask rtt: %v", err)
					}
					enabledSamples = append(enabledSamples, enabledDur)

					baselineDur, err := measureFirstEchoRTT(context.Background(), offClient, tables, echoAddr)
					if err != nil {
						t.Fatalf("measure baseline rtt: %v", err)
					}
					baselineSamples = append(baselineSamples, baselineDur)
					continue
				}

				baselineDur, err := measureFirstEchoRTT(context.Background(), offClient, tables, echoAddr)
				if err != nil {
					t.Fatalf("measure baseline rtt: %v", err)
				}
				baselineSamples = append(baselineSamples, baselineDur)

				enabledDur, err := measureFirstEchoRTT(context.Background(), onClient, tables, echoAddr)
				if err != nil {
					t.Fatalf("measure httpmask rtt: %v", err)
				}
				enabledSamples = append(enabledSamples, enabledDur)
			}

			enabledDur := trimmedMeanDuration(enabledSamples)
			baselineDur := trimmedMeanDuration(baselineSamples)
			const tolerance = 45 * time.Millisecond
			if enabledDur > baselineDur+tolerance {
				t.Fatalf(
					"httpmask RTT mismatch: enabled=%v baseline=%v tolerance=%v enabled_samples=%v baseline_samples=%v",
					enabledDur,
					baselineDur,
					tolerance,
					enabledSamples,
					baselineSamples,
				)
			}
		})
	}
}

func TestDirectionalCustomTableSetRetainsRotation(t *testing.T) {
	tables, err := getTables("matrix-smoke-key", "up_ascii_down_entropy", "custom7")
	if err != nil {
		t.Fatalf("get tables failed: %v", err)
	}
	if len(tables) != 7 {
		t.Fatalf("expected 7 directional tables, got %d", len(tables))
	}
}

func trimmedMeanDuration(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	if len(ordered) > 2 {
		ordered = ordered[1 : len(ordered)-1]
	}
	var total time.Duration
	for _, sample := range ordered {
		total += sample
	}
	return total / time.Duration(len(ordered))
}

func measureFirstEchoRTT(ctx context.Context, cfg *config.Config, tables []*sudoku.Table, targetAddr string) (time.Duration, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dialer := &tunnel.StandardDialer{
		BaseDialer: tunnel.BaseDialer{
			Config: cfg,
			Tables: tables,
		},
	}
	start := time.Now()
	conn, err := dialer.Dial(targetAddr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte{0x42}); err != nil {
		return 0, err
	}
	var b [1]byte
	if _, err := io.ReadFull(conn, b[:]); err != nil {
		return 0, err
	}
	_ = runCtx
	return time.Since(start), nil
}

func startDelayProxy(ctx context.Context, backend string, oneWayDelay time.Duration) (addr string, closeFn func() error, err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handleDelayProxyConn(c, backend, oneWayDelay)
		}
	}()

	closeFn = func() error {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		return nil
	}
	go func() {
		<-ctx.Done()
		_ = closeFn()
	}()
	return ln.Addr().String(), closeFn, nil
}

func handleDelayProxyConn(client net.Conn, backend string, oneWayDelay time.Duration) {
	defer client.Close()

	server, err := net.DialTimeout("tcp", backend, 3*time.Second)
	if err != nil {
		return
	}
	defer server.Close()

	copyWithDelay := func(dst net.Conn, src net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				time.Sleep(oneWayDelay)
				if werr := writeFull(dst, buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				if cw, ok := dst.(interface{ CloseWrite() error }); ok {
					_ = cw.CloseWrite()
				}
				return
			}
		}
	}

	done := make(chan struct{}, 2)
	go func() {
		copyWithDelay(server, client)
		done <- struct{}{}
	}()
	go func() {
		copyWithDelay(client, server)
		done <- struct{}{}
	}()
	<-done
	<-done
}
