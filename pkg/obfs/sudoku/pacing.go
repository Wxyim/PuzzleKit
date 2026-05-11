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
package sudoku

import (
	"io"
	"net"
	"sync"

	"github.com/SUDOKU-ASCII/sudoku/pkg/connutil"
)

const (
	// Each MTU-sized raw downlink chunk gets an independent padding chance. 1440
	// matches common full-sized TCP payloads when timestamp/options are present.
	defaultInjectMTUBytes = 1440

	// Random low-frequency injection keeps the pattern irregular while limiting average
	// overhead. 1/13 means only a small subset of MTU-sized segments get a spacer packet.
	defaultInjectChanceNumerator   = 1
	defaultInjectChanceDenominator = 13

	// Inter-packet padding must stay small so it behaves like a pacing spacer.
	defaultPaddingPacketMin = 1
	defaultPaddingPacketMax = 128
)

type downlinkPacingConfig struct {
	mtuWireBytes            int
	injectChanceNumerator   int
	injectChanceDenominator int
	paddingPacketMin        int
	paddingPacketMax        int
}

func defaultDownlinkPacingConfig() downlinkPacingConfig {
	return downlinkPacingConfig{
		mtuWireBytes:            defaultInjectMTUBytes,
		injectChanceNumerator:   defaultInjectChanceNumerator,
		injectChanceDenominator: defaultInjectChanceDenominator,
		paddingPacketMin:        defaultPaddingPacketMin,
		paddingPacketMax:        defaultPaddingPacketMax,
	}
}

type randomPaddingPolicy struct {
	mtuWireBytes int
	numerator    int
	denominator  int
}

func newRandomPaddingPolicy(cfg downlinkPacingConfig) *randomPaddingPolicy {
	return &randomPaddingPolicy{
		mtuWireBytes: maxInt(cfg.mtuWireBytes, 1),
		numerator:    clampInt(cfg.injectChanceNumerator, 0, maxInt(cfg.injectChanceDenominator, 1)),
		denominator:  maxInt(cfg.injectChanceDenominator, 1),
	}
}

func (p *randomPaddingPolicy) ShouldInject(rng randomSource) bool {
	if p == nil || rng == nil || p.numerator <= 0 {
		return false
	}
	if p.numerator >= p.denominator {
		return true
	}
	return rng.Intn(p.denominator) < p.numerator
}

func (p *randomPaddingPolicy) MTUBytes() int {
	if p == nil || p.mtuWireBytes <= 0 {
		return defaultInjectMTUBytes
	}
	return p.mtuWireBytes
}

type interPacketPaddingInjector struct {
	writer       io.Writer
	paddingPool  []byte
	rng          randomSource
	minPacketLen int
	maxPacketLen int
	packetBuf    []byte
}

func newInterPacketPaddingInjector(writer io.Writer, paddingPool []byte, rng randomSource, cfg downlinkPacingConfig) *interPacketPaddingInjector {
	return &interPacketPaddingInjector{
		writer:       writer,
		paddingPool:  append([]byte(nil), paddingPool...),
		rng:          rng,
		minPacketLen: maxInt(cfg.paddingPacketMin, 1),
		maxPacketLen: maxInt(cfg.paddingPacketMax, cfg.paddingPacketMin),
	}
}

// Inject writes a small pure-padding packet directly to the raw transport. Receivers
// safely discard it because these bytes never match Sudoku hints.
func (i *interPacketPaddingInjector) Inject() error {
	if i == nil || len(i.paddingPool) == 0 || i.writer == nil || i.rng == nil {
		return nil
	}

	packetLen := i.pickPacketLen()
	if cap(i.packetBuf) < packetLen {
		i.packetBuf = make([]byte, packetLen)
	}
	packet := i.packetBuf[:0]
	for len(packet) < packetLen {
		packet = append(packet, i.paddingPool[i.rng.Intn(len(i.paddingPool))])
	}
	return connutil.WriteFull(i.writer, packet)
}

func (i *interPacketPaddingInjector) pickPacketLen() int {
	if i.maxPacketLen <= i.minPacketLen {
		return i.minPacketLen
	}
	return i.minPacketLen + i.rng.Intn(i.maxPacketLen-i.minPacketLen+1)
}

type pacingRawWriter struct {
	writer   io.Writer
	policy   *randomPaddingPolicy
	injector *interPacketPaddingInjector
	pending  int
}

func newPacingRawWriter(raw io.Writer, paddingPool []byte, rng randomSource, cfg downlinkPacingConfig) *pacingRawWriter {
	return &pacingRawWriter{
		writer:   raw,
		policy:   newRandomPaddingPolicy(cfg),
		injector: newInterPacketPaddingInjector(raw, paddingPool, rng, cfg),
	}
}

func (w *pacingRawWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w == nil || w.writer == nil || w.policy == nil {
		return 0, io.ErrClosedPipe
	}

	mtu := w.policy.MTUBytes()
	written := 0
	for len(p) > 0 {
		need := mtu - w.pending
		if need <= 0 {
			need = mtu
			w.pending = 0
		}
		if need > len(p) {
			need = len(p)
		}

		chunk := p[:need]
		if err := connutil.WriteFull(w.writer, chunk); err != nil {
			return written, err
		}
		written += len(chunk)
		w.pending += len(chunk)
		p = p[need:]

		if w.pending >= mtu {
			w.pending = 0
			if w.policy.ShouldInject(w.injector.rng) {
				if err := w.injector.Inject(); err != nil {
					return written, err
				}
			}
		}
	}
	return written, nil
}

type pacingNetConn struct {
	net.Conn
	writer *pacingRawWriter
}

func (c *pacingNetConn) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

// DownlinkPacingWriter encodes payload bytes with the underlying downlink codec and
// occasionally injects a tiny pure-padding spacer packet.
type DownlinkPacingWriter struct {
	dataWriter io.Writer
	writeMu    sync.Mutex
}

func NewDownlinkPacingWriter(raw io.Writer, table *Table, pMin, pMax int) *DownlinkPacingWriter {
	localRng := newSeededRand()
	cfg := defaultDownlinkPacingConfig()
	pacedRaw := newPacingRawWriter(raw, table.PaddingPool, localRng, cfg)
	return newDownlinkPacingWriterWithConfig(newSudokuDataWriter(pacedRaw, table, localRng, pMin, pMax))
}

func NewPackedDownlinkPacingWriter(raw net.Conn, table *Table, pMin, pMax int) (*PackedConn, *DownlinkPacingWriter) {
	cfg := defaultDownlinkPacingConfig()
	pacedRaw := newPacingRawWriter(raw, packedInterPacketPaddingPool(table), newSeededRand(), cfg)
	packed := NewPackedConn(&pacingNetConn{Conn: raw, writer: pacedRaw}, table, pMin, pMax)
	return packed, newDownlinkPacingWriterWithConfig(packed)
}

func NewServerDownlinkWriter(raw net.Conn, table *Table, pMin, pMax int, pure bool) (io.Writer, []func() error) {
	if pure {
		return NewDownlinkPacingWriter(raw, table, pMin, pMax), nil
	}

	packed, writer := NewPackedDownlinkPacingWriter(raw, table, pMin, pMax)
	return writer, []func() error{packed.Flush}
}

func newDownlinkPacingWriterWithConfig(
	dataWriter io.Writer,
) *DownlinkPacingWriter {
	return &DownlinkPacingWriter{
		dataWriter: dataWriter,
	}
}

func (w *DownlinkPacingWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	n, err := w.dataWriter.Write(p)
	if err != nil {
		return n, err
	}
	return n, nil
}

type sudokuDataWriter struct {
	writer           io.Writer
	table            *Table
	rng              randomSource
	paddingThreshold uint64
	writeBuf         []byte
}

func newSudokuDataWriter(writer io.Writer, table *Table, rng randomSource, pMin, pMax int) *sudokuDataWriter {
	return &sudokuDataWriter{
		writer:           writer,
		table:            table,
		rng:              rng,
		paddingThreshold: pickPaddingThreshold(rng, pMin, pMax),
		writeBuf:         make([]byte, 0, 4096),
	}
}

func (w *sudokuDataWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.writeBuf = encodeSudokuPayload(w.writeBuf[:0], w.table, w.rng, w.paddingThreshold, p)
	return len(p), connutil.WriteFull(w.writer, w.writeBuf)
}

func encodeSudokuPayload(dst []byte, table *Table, rng randomSource, paddingThreshold uint64, p []byte) []byte {
	if len(p) == 0 {
		return dst[:0]
	}

	outCapacity := len(p)*6 + 1
	if cap(dst) < outCapacity {
		dst = make([]byte, 0, outCapacity)
	}
	out := dst[:0]
	pads := table.PaddingPool
	padLen := len(pads)

	for _, b := range p {
		if shouldPad(rng, paddingThreshold) {
			out = append(out, pads[rng.Intn(padLen)])
		}

		puzzles := table.EncodeTable[b]
		puzzle := puzzles[rng.Intn(len(puzzles))]

		perm := perm4[rng.Intn(len(perm4))]
		for _, idx := range perm {
			if shouldPad(rng, paddingThreshold) {
				out = append(out, pads[rng.Intn(padLen)])
			}
			out = append(out, puzzle[idx])
		}
	}

	if shouldPad(rng, paddingThreshold) {
		out = append(out, pads[rng.Intn(padLen)])
	}
	return out
}

func minEncodedSudokuBytes(plainBytes int) int {
	if plainBytes <= 0 {
		return 0
	}
	return plainBytes * 4
}

func minEncodedPackedBytes(plainBytes int) int {
	if plainBytes <= 0 {
		return 0
	}
	// Packed downlink has a 4/3 core expansion plus a short protected prefix/tail.
	return ((plainBytes + 2) / 3 * 4) + 16
}

func packedInterPacketPaddingPool(table *Table) []byte {
	if table == nil || table.layout == nil {
		return nil
	}
	pool := make([]byte, 0, len(table.PaddingPool))
	for _, b := range table.PaddingPool {
		if b != table.layout.padMarker {
			pool = append(pool, b)
		}
	}
	if len(pool) == 0 && table.layout.padMarker != 0 {
		pool = append(pool, table.layout.padMarker)
	}
	return pool
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
