// Package apis exposes the Sudoku appearance codec as a small raw-stream API.
//
// The returned connections read and write the caller's original bytes while the
// underlying wire stream is encoded with the Sudoku byte layout. This package
// intentionally does not provide HTTP masking, encryption, handshakes, UoT, or
// reverse proxy helpers.
package apis
