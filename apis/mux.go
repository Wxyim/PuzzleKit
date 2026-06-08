package apis

import (
	"io"
	"net"

	"github.com/SUDOKU-ASCII/sudoku/internal/tunnel"
)

// MuxClient opens multiple logical target streams over one already-wrapped raw
// Sudoku appearance connection. It has no HTTPMask dependency.
type MuxClient struct {
	inner *tunnel.MuxClient
}

// NewMuxClient starts a mux session over conn.
func NewMuxClient(conn net.Conn) (*MuxClient, error) {
	inner, err := tunnel.NewMuxClient(conn)
	if err != nil {
		return nil, err
	}
	return &MuxClient{inner: inner}, nil
}

// Dial opens a logical stream to targetAddr.
func (c *MuxClient) Dial(targetAddr string) (net.Conn, error) {
	if c == nil || c.inner == nil {
		return nil, io.ErrClosedPipe
	}
	return c.inner.Dial(targetAddr)
}

// Close closes the mux session and its underlying connection.
func (c *MuxClient) Close() error {
	if c == nil || c.inner == nil {
		return nil
	}
	return c.inner.Close()
}

// Done is closed when the mux session ends.
func (c *MuxClient) Done() <-chan struct{} {
	if c == nil || c.inner == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return c.inner.Done()
}

// Err returns the terminal mux session error after Done closes.
func (c *MuxClient) Err() error {
	if c == nil || c.inner == nil {
		return io.ErrClosedPipe
	}
	return c.inner.Err()
}

// HandleMuxServer runs a target-address mux server over an already-wrapped raw
// Sudoku appearance connection.
func HandleMuxServer(conn net.Conn, onConnect func(targetAddr string)) error {
	return tunnel.HandleMuxServer(conn, onConnect)
}

// HandleMuxWithDialer is like HandleMuxServer but lets the caller control how
// target addresses are opened.
func HandleMuxWithDialer(conn net.Conn, onConnect func(targetAddr string), dialTarget func(targetAddr string) (net.Conn, error)) error {
	return tunnel.HandleMuxWithDialer(conn, onConnect, dialTarget)
}
