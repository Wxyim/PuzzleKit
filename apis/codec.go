package apis

import (
	"fmt"
	"io"
	"net"

	"github.com/SUDOKU-ASCII/sudoku/pkg/obfs/sudoku"
)

// Side selects which traffic direction a wrapped connection should use.
type Side string

const (
	// ClientSide writes uplink with the selected table and reads downlink with the opposite table.
	ClientSide Side = "client"
	// ServerSide reads uplink with the selected table and writes downlink with the opposite table.
	ServerSide Side = "server"
)

// WrapConn returns a net.Conn that exposes original bytes to the caller and
// encodes/decodes the underlying wire stream with the Sudoku appearance layer.
func WrapConn(raw net.Conn, cfg *Config, side Side) (net.Conn, error) {
	switch side {
	case ClientSide:
		return ClientConn(raw, cfg)
	case ServerSide:
		return ServerConn(raw, cfg)
	default:
		return nil, fmt.Errorf("invalid side: %s", side)
	}
}

// ClientConn wraps the client side of a raw connection.
func ClientConn(raw net.Conn, cfg *Config) (net.Conn, error) {
	if raw == nil {
		return nil, fmt.Errorf("nil conn")
	}
	c, table, err := selectedTable(cfg)
	if err != nil {
		return nil, err
	}

	reader := downlinkReader(raw, table.OppositeDirection(), c)
	writer := sudoku.NewConn(raw, table, c.PaddingMin, c.PaddingMax, false)
	return sudoku.NewDirectionalConn(raw, reader, writer), nil
}

// ServerConn wraps the server side of a raw connection.
func ServerConn(raw net.Conn, cfg *Config) (net.Conn, error) {
	if raw == nil {
		return nil, fmt.Errorf("nil conn")
	}
	c, table, err := selectedTable(cfg)
	if err != nil {
		return nil, err
	}

	reader := sudoku.NewConn(raw, table, c.PaddingMin, c.PaddingMax, false)
	writer, closers := sudoku.NewServerDownlinkWriter(raw, table.OppositeDirection(), c.PaddingMin, c.PaddingMax, c.EnablePureDownlink)
	return sudoku.NewDirectionalConn(raw, reader, writer, closers...), nil
}

func downlinkReader(raw net.Conn, table *sudoku.Table, cfg Config) io.Reader {
	if cfg.EnablePureDownlink {
		return sudoku.NewConn(raw, table, cfg.PaddingMin, cfg.PaddingMax, false)
	}
	return sudoku.NewPackedConn(raw, table, cfg.PaddingMin, cfg.PaddingMax)
}
