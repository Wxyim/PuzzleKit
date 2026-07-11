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
package httpmask

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	compatPathRoot = "compat"
	compatAuthKey  = "compat-key"
)

type connectionThreshold struct {
	target int32
	count  atomic.Int32
	once   sync.Once
	ready  chan struct{}
}

func newConnectionThreshold(target int) *connectionThreshold {
	return &connectionThreshold{
		target: int32(target),
		ready:  make(chan struct{}),
	}
}

func (c *connectionThreshold) record() {
	if c.count.Add(1) >= c.target {
		c.once.Do(func() { close(c.ready) })
	}
}

func TestTunnelHTTPClient_PreconnectsTLSHandshakes(t *testing.T) {
	var httpClient *tunnelHTTPClient
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			http.Error(w, "HTTP/2 was not negotiated", http.StatusHTTPVersionNotSupported)
			return
		}
		if !waitForPreparedConns(httpClient, 2, 2*time.Second) {
			http.Error(w, "preconnections not ready", http.StatusGatewayTimeout)
			return
		}
		_, _ = io.WriteString(w, "token=compat")
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)

	target, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	httpClient = newHTTPClient(target.Host, target.Host, target.Hostname(), "https", 4, false)
	t.Cleanup(httpClient.transport.close)

	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())
	httpClient.transport.transport.TLSClientConfig.RootCAs = roots
	httpClient.transport.dialer.tlsConfig.RootCAs = roots

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/session", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	cancelPreconnect := httpClient.preconnect(ctx, req, tunnelPreconnectCount)
	defer cancelPreconnect()

	resp, err := httpClient.client.Do(req)
	if err != nil {
		t.Fatalf("authorize request: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authorize status: %s", resp.Status)
	}
	if !waitForPreparedConns(httpClient, 2, time.Second) {
		t.Fatal("two completed TLS preconnections were not retained")
	}
}

func TestPreconnectDialer_MaintainsSpareConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	dialer := newPreconnectDialer(listener.Addr().String(), listener.Addr().String(), "", nil)
	defer dialer.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dialer.maintainPreconnect(ctx, false, 1)

	waitForSpare := func() {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			dialer.pool.mu.Lock()
			ready := len(dialer.pool.ready)
			dialer.pool.mu.Unlock()
			if ready >= 1 {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("spare connection was not prepared")
	}

	waitForSpare()
	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("initial spare connection was not accepted")
	}
	first, ok, err := dialer.pool.take(context.Background())
	if err != nil || !ok || first == nil {
		t.Fatalf("take first spare: conn=%v ok=%v err=%v", first, ok, err)
	}
	_ = first.Close()
	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(250 * time.Millisecond):
		t.Fatal("maintainer did not immediately replenish the consumed connection")
	}
	waitForSpare()
}

func TestSessionPreconnectCount(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want int
	}{
		{mode: "off", want: tunnelPreconnectCount},
		{mode: "auto", want: tunnelPreconnectCount},
		{mode: "on", want: tunnelMuxPreconnectCount},
		{mode: " ON ", want: tunnelMuxPreconnectCount},
	} {
		if got := sessionPreconnectCount(tc.mode); got != tc.want {
			t.Fatalf("sessionPreconnectCount(%q) = %d, want %d", tc.mode, got, tc.want)
		}
	}
}

func TestPreparedConnPool_WaitReadyStopsOnTunnelClose(t *testing.T) {
	pool := newPreparedConnPool()
	closed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- pool.waitReady(context.Background(), closed, 1)
	}()

	close(closed)
	select {
	case err := <-done:
		if err != net.ErrClosed {
			t.Fatalf("wait ready error = %v, want %v", err, net.ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("wait ready did not stop after tunnel close")
	}
}

func TestPreparedConnPool_DoesNotReturnExpiredConnection(t *testing.T) {
	pool := newPreparedConnPool()
	client, peer := net.Pipe()
	defer peer.Close()

	pool.ready = append(pool.ready, &preparedConn{
		conn:      client,
		expiresAt: time.Now().Add(-time.Second),
	})

	conn, ok, err := pool.take(context.Background())
	if err != nil {
		t.Fatalf("take expired connection: %v", err)
	}
	if ok || conn != nil {
		t.Fatalf("expired connection was returned: conn=%v ok=%v", conn, ok)
	}

	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := peer.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("expired connection was not closed: %v", err)
	}
}

func TestDialTunnel_BidirectionalSmoke(t *testing.T) {
	for _, mode := range []string{"stream", "poll", "ws", "auto"} {
		t.Run(mode, func(t *testing.T) {
			srv := NewTunnelServer(TunnelServerOptions{
				Mode:            "auto",
				PathRoot:        compatPathRoot,
				AuthKey:         compatAuthKey,
				PullReadTimeout: 50 * time.Millisecond,
				SessionTTL:      3 * time.Second,
			})
			addr, stop, tunnelCh := startRawTunnelServer(t, srv)
			defer stop()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialTunnel(ctx, addr, TunnelDialOptions{
				Mode:       mode,
				PathRoot:   compatPathRoot,
				AuthKey:    compatAuthKey,
				Multiplex:  "auto",
				TLSEnabled: false,
			})
			if err != nil {
				t.Fatalf("dial %s: %v", mode, err)
			}
			defer client.Close()

			server := waitForTunnelConn(t, tunnelCh)
			defer server.Close()
			assertBidirectionalExchange(t, client, server)
		})
	}
}

func TestDialTunnel_WaitReadyConfirmsSplitSession(t *testing.T) {
	for _, mode := range []string{"stream", "poll", "auto"} {
		t.Run(mode, func(t *testing.T) {
			srv := NewTunnelServer(TunnelServerOptions{
				Mode:            "auto",
				PathRoot:        compatPathRoot,
				AuthKey:         compatAuthKey,
				PullReadTimeout: 50 * time.Millisecond,
				SessionTTL:      3 * time.Second,
			})
			addr, stop, tunnelCh := startRawTunnelServer(t, srv)
			defer stop()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialTunnel(ctx, addr, TunnelDialOptions{
				Mode:       mode,
				PathRoot:   compatPathRoot,
				AuthKey:    compatAuthKey,
				Multiplex:  "on",
				TLSEnabled: false,
			})
			if err != nil {
				t.Fatalf("dial %s: %v", mode, err)
			}
			defer client.Close()

			server := waitForTunnelConn(t, tunnelCh)
			defer server.Close()
			payload := []byte("mux-start-placeholder")
			readDone := make(chan error, 1)
			go func() {
				got := make([]byte, len(payload))
				_, err := io.ReadFull(server, got)
				if err == nil && !bytes.Equal(got, payload) {
					err = fmt.Errorf("uplink = %q, want %q", got, payload)
				}
				readDone <- err
			}()

			if _, err := client.Write(payload); err != nil {
				t.Fatalf("queue initial upload: %v", err)
			}
			readyCtx, readyCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer readyCancel()
			if err := WaitTunnelReady(readyCtx, client); err != nil {
				t.Fatalf("wait ready: %v", err)
			}
			select {
			case err := <-readDone:
				if err != nil {
					t.Fatalf("server read: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("upload was not delivered before readiness")
			}

			downlink := []byte("ready-downlink")
			if _, err := server.Write(downlink); err != nil {
				t.Fatalf("server write: %v", err)
			}
			got := make([]byte, len(downlink))
			if _, err := io.ReadFull(client, got); err != nil {
				t.Fatalf("client read: %v", err)
			}
			if !bytes.Equal(got, downlink) {
				t.Fatalf("downlink = %q, want %q", got, downlink)
			}
		})
	}
}

func TestDialTunnel_NewClientWithV047Server(t *testing.T) {
	tests := []struct {
		name     string
		dialMode string
		wireMode TunnelMode
	}{
		{name: "stream", dialMode: "stream", wireMode: TunnelModeStream},
		{name: "poll", dialMode: "poll", wireMode: TunnelModePoll},
		{name: "auto", dialMode: "auto", wireMode: TunnelModeStream},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appConn, sessionConn := net.Pipe()
			defer appConn.Close()
			defer sessionConn.Close()

			connections := newConnectionThreshold(tunnelPreconnectCount)
			var pullServed atomic.Bool
			handlerErr := make(chan error, 1)
			const token = "v047-session-token"

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path, ok := stripPathRoot("/"+compatPathRoot, r.URL.Path)
				if !ok {
					reportHandlerError(handlerErr, w, "unexpected path %q", r.URL.Path)
					return
				}
				authValue := r.Header.Get(tunnelAuthHeaderKey)
				if authValue == "" {
					authValue = r.URL.Query().Get(tunnelAuthQueryKey)
				}
				auth := newTunnelAuth(compatAuthKey, 0)
				if !auth.verifyValue(authValue, tt.wireMode, r.Method, path, time.Now()) {
					reportHandlerError(handlerErr, w, "invalid auth for %s %s", r.Method, path)
					return
				}

				switch {
				case r.Method == http.MethodGet && path == "/session":
					select {
					case <-connections.ready:
					case <-time.After(2 * time.Second):
						reportHandlerError(handlerErr, w, "preconnections not ready")
						return
					}
					w.Header().Set("Connection", "close")
					w.Header().Set("Content-Type", "application/octet-stream")
					_, _ = io.WriteString(w, "token="+token)

				case r.Method == http.MethodPost && path == "/api/v1/upload":
					if r.URL.Query().Get("token") != token {
						reportHandlerError(handlerErr, w, "unexpected upload token")
						return
					}
					if r.URL.Query().Get("close") == "1" || r.URL.Query().Get("fin") == "1" {
						w.WriteHeader(http.StatusOK)
						return
					}
					body, err := io.ReadAll(r.Body)
					if err != nil {
						reportHandlerError(handlerErr, w, "read upload: %v", err)
						return
					}
					payload, err := decodeV047Payload(tt.wireMode, body)
					if err != nil {
						reportHandlerError(handlerErr, w, "decode upload: %v", err)
						return
					}
					_ = sessionConn.SetWriteDeadline(time.Now().Add(2 * time.Second))
					_, err = sessionConn.Write(payload)
					_ = sessionConn.SetWriteDeadline(time.Time{})
					if err != nil {
						reportHandlerError(handlerErr, w, "forward upload: %v", err)
						return
					}
					w.WriteHeader(http.StatusOK)

				case r.Method == http.MethodGet && path == "/stream":
					if r.URL.Query().Get("token") != token {
						reportHandlerError(handlerErr, w, "unexpected pull token")
						return
					}
					if !pullServed.CompareAndSwap(false, true) {
						<-r.Context().Done()
						return
					}
					buf := make([]byte, len("server-to-client"))
					_ = sessionConn.SetReadDeadline(time.Now().Add(2 * time.Second))
					_, err := io.ReadFull(sessionConn, buf)
					_ = sessionConn.SetReadDeadline(time.Time{})
					if err != nil {
						reportHandlerError(handlerErr, w, "read downlink: %v", err)
						return
					}
					_, _ = w.Write(encodeV047Payload(tt.wireMode, buf))

				default:
					reportHandlerError(handlerErr, w, "unexpected request %s %s", r.Method, path)
				}
			})

			ts := httptest.NewUnstartedServer(handler)
			ts.Listener = &countingListener{
				Listener: ts.Listener,
				onAccept: connections.record,
			}
			ts.Start()
			defer ts.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialTunnel(ctx, strings.TrimPrefix(ts.URL, "http://"), TunnelDialOptions{
				Mode:       tt.dialMode,
				PathRoot:   compatPathRoot,
				AuthKey:    compatAuthKey,
				Multiplex:  "off",
				TLSEnabled: false,
			})
			if err != nil {
				t.Fatalf("dial v0.4.7 server: %v", err)
			}
			defer client.Close()

			assertBidirectionalExchange(t, client, appConn)
			select {
			case err := <-handlerErr:
				t.Fatal(err)
			default:
			}
		})
	}
}

func TestTunnelServer_V047ClientBidirectional(t *testing.T) {
	for _, mode := range []TunnelMode{TunnelModeStream, TunnelModePoll} {
		t.Run(string(mode), func(t *testing.T) {
			srv := NewTunnelServer(TunnelServerOptions{
				Mode:            "auto",
				PathRoot:        compatPathRoot,
				AuthKey:         compatAuthKey,
				PullReadTimeout: 50 * time.Millisecond,
				SessionTTL:      3 * time.Second,
			})
			addr, stop, tunnelCh := startRawTunnelServer(t, srv)
			defer stop()

			client := &http.Client{
				Transport: &http.Transport{
					DisableCompression: true,
				},
			}
			defer client.Transport.(*http.Transport).CloseIdleConnections()
			baseURL := "http://" + addr
			auth := newTunnelAuth(compatAuthKey, 0)

			resp := doV047Request(t, client, auth, mode, http.MethodGet, baseURL, "/session", nil, nil)
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				t.Fatalf("read authorize response: %v", err)
			}
			authResp, err := parseAuthorizeResponse(body)
			if err != nil {
				t.Fatalf("parse authorize response: %v", err)
			}

			server := waitForTunnelConn(t, tunnelCh)
			defer server.Close()

			uplink := []byte("client-to-server")
			readDone := make(chan error, 1)
			go func() {
				buf := make([]byte, len(uplink))
				_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, err := io.ReadFull(server, buf)
				if err == nil && !bytes.Equal(buf, uplink) {
					err = fmt.Errorf("uplink = %q, want %q", buf, uplink)
				}
				readDone <- err
			}()
			query := url.Values{"token": []string{authResp.token}}
			upload := encodeV047Payload(mode, uplink)
			resp = doV047Request(t, client, auth, mode, http.MethodPost, baseURL, "/api/v1/upload", query, upload)
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if err := <-readDone; err != nil {
				t.Fatalf("server read: %v", err)
			}

			downlink := []byte("server-to-client")
			writeDone := make(chan error, 1)
			go func() {
				_ = server.SetWriteDeadline(time.Now().Add(2 * time.Second))
				_, err := server.Write(downlink)
				writeDone <- err
			}()
			resp = doV047Request(t, client, auth, mode, http.MethodGet, baseURL, "/stream", query, nil)
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				t.Fatalf("read pull response: %v", err)
			}
			if err := <-writeDone; err != nil {
				t.Fatalf("server write: %v", err)
			}
			got, err := decodeV047Payload(mode, body)
			if err != nil {
				t.Fatalf("decode downlink: %v", err)
			}
			if !bytes.Equal(got, downlink) {
				t.Fatalf("downlink = %q, want %q", got, downlink)
			}
		})
	}
}

type countingListener struct {
	net.Listener
	onAccept func()
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil && l.onAccept != nil {
		l.onAccept()
	}
	return conn, err
}

func waitForTunnelConn(t *testing.T, tunnelCh <-chan net.Conn) net.Conn {
	t.Helper()
	select {
	case conn := <-tunnelCh:
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server tunnel")
		return nil
	}
}

func waitForPreparedConns(client *tunnelHTTPClient, count int, timeout time.Duration) bool {
	if client == nil || client.transport == nil || client.transport.dialer == nil ||
		client.transport.dialer.pool == nil {
		return false
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pool := client.transport.dialer.pool
		pool.mu.Lock()
		ready := len(pool.ready)
		pool.mu.Unlock()
		if ready >= count {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func assertBidirectionalExchange(t *testing.T, client, server net.Conn) {
	t.Helper()
	assertOneWayExchange(t, client, server, []byte("client-to-server"))
	assertOneWayExchange(t, server, client, []byte("server-to-client"))
}

func assertOneWayExchange(t *testing.T, writer, reader net.Conn, payload []byte) {
	t.Helper()
	writeDone := make(chan error, 1)
	go func() {
		_ = writer.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, err := writer.Write(payload)
		writeDone <- err
	}()

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, len(payload))
		_ = reader.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := io.ReadFull(reader, buf)
		if err == nil && !bytes.Equal(buf, payload) {
			err = fmt.Errorf("payload = %q, want %q", buf, payload)
		}
		readDone <- err
	}()

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write payload: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("write payload timed out")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read payload: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read payload timed out")
	}
}

func reportHandlerError(errCh chan<- error, w http.ResponseWriter, format string, args ...any) {
	err := fmt.Errorf(format, args...)
	select {
	case errCh <- err:
	default:
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func encodeV047Payload(mode TunnelMode, payload []byte) []byte {
	if mode == TunnelModePoll {
		encoded := make([]byte, base64.StdEncoding.EncodedLen(len(payload))+1)
		base64.StdEncoding.Encode(encoded, payload)
		encoded[len(encoded)-1] = '\n'
		return encoded
	}
	return payload
}

func decodeV047Payload(mode TunnelMode, payload []byte) ([]byte, error) {
	if mode != TunnelModePoll {
		return payload, nil
	}
	var decoded []byte
	for _, line := range bytes.Split(payload, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		chunk := make([]byte, base64.StdEncoding.DecodedLen(len(line)))
		n, err := base64.StdEncoding.Decode(chunk, line)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, chunk[:n]...)
	}
	return decoded, nil
}

func doV047Request(
	t *testing.T,
	client *http.Client,
	auth *tunnelAuth,
	mode TunnelMode,
	method, baseURL, path string,
	query url.Values,
	body []byte,
) *http.Response {
	t.Helper()
	requestURL := baseURL + joinPathRoot(compatPathRoot, path)
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, requestURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new v0.4.7 request: %v", err)
	}
	req.Host = strings.TrimPrefix(baseURL, "http://")
	applyTunnelHeaders(req.Header, req.Host, mode)
	applyTunnelAuth(req, auth, mode, method, path)
	if method == http.MethodPost {
		if mode == TunnelModePoll {
			req.Header.Set("Content-Type", "text/plain")
		} else {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("v0.4.7 request %s %s: %v", method, path, err)
	}
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("v0.4.7 request %s %s: %s (%s)", method, path, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return resp
}
