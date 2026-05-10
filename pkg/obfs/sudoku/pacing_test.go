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
	"bytes"
	"io"
	"net"
	"testing"
)

func TestRandomPaddingPolicy_SizeGateAndProbability(t *testing.T) {
	cfg := defaultDownlinkPacingConfig()
	cfg.mtuWireBytes = 16
	cfg.injectChanceNumerator = 1
	cfg.injectChanceDenominator = 1

	policy := newRandomPaddingPolicy(cfg)
	rng := newSudokuRand(1)
	if policy.MTUBytes() != 16 {
		t.Fatalf("mtu bytes = %d, want 16", policy.MTUBytes())
	}
	if !policy.ShouldInject(rng) {
		t.Fatalf("forced probability should inject")
	}
	cfg.injectChanceNumerator = 0
	policy = newRandomPaddingPolicy(cfg)
	if policy.ShouldInject(rng) {
		t.Fatalf("zero probability should not inject")
	}
}

func TestDownlinkPacingWriter_SkipsSmallTraffic(t *testing.T) {
	table := NewTable("pacing-small", "prefer_ascii")
	cfg := defaultDownlinkPacingConfig()
	cfg.mtuWireBytes = 128
	cfg.injectChanceNumerator = 1
	cfg.injectChanceDenominator = 1

	var raw bytes.Buffer
	writer := newTestSudokuPacingWriter(&raw, table, cfg)
	payload := []byte("small-response")

	n, err := writer.Write(payload)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("write len mismatch: got %d want %d", n, len(payload))
	}

	if got, want := raw.Len(), len(payload)*4; got != want {
		t.Fatalf("small traffic should not add pacing bytes: got %d want %d", got, want)
	}
	if extra := countNonHintBytes(raw.Bytes(), table); extra != 0 {
		t.Fatalf("small traffic unexpectedly injected %d padding bytes", extra)
	}
}

func TestDownlinkPacingWriter_InjectsPurePaddingOnEligibleWrite(t *testing.T) {
	table := NewTable("pacing-burst", "prefer_ascii")
	cfg := defaultDownlinkPacingConfig()
	cfg.mtuWireBytes = 32
	cfg.injectChanceNumerator = 1
	cfg.injectChanceDenominator = 1
	cfg.paddingPacketMin = 5
	cfg.paddingPacketMax = 5

	var raw bytes.Buffer
	writer := newTestSudokuPacingWriter(&raw, table, cfg)
	chunk := bytes.Repeat([]byte("a"), 8)

	if _, err := writer.Write(chunk); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	expectedDataBytes := len(chunk) * 4
	if got, want := raw.Len(), expectedDataBytes+5; got != want {
		t.Fatalf("unexpected wire len: got %d want %d", got, want)
	}
	if extra := countNonHintBytes(raw.Bytes(), table); extra != 5 {
		t.Fatalf("expected exactly one 5-byte padding packet, got %d extra bytes", extra)
	}
}

func TestPacingRawWriter_InjectsAfterEachMTUChunk(t *testing.T) {
	table := NewTable("pacing-raw", "prefer_ascii")
	cfg := defaultDownlinkPacingConfig()
	cfg.mtuWireBytes = 4
	cfg.injectChanceNumerator = 1
	cfg.injectChanceDenominator = 1
	cfg.paddingPacketMin = 2
	cfg.paddingPacketMax = 2

	var raw bytes.Buffer
	writer := newPacingRawWriter(&raw, table.PaddingPool, newSudokuRand(4), cfg)
	payload := []byte("abcdefgh")

	n, err := writer.Write(payload)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("write len mismatch: got %d want %d", n, len(payload))
	}
	if got, want := raw.Len(), len(payload)+4; got != want {
		t.Fatalf("wire len = %d, want %d", got, want)
	}
	if !bytes.Equal(raw.Bytes()[:4], payload[:4]) {
		t.Fatalf("first mtu chunk changed")
	}
	if !bytes.Equal(raw.Bytes()[6:10], payload[4:]) {
		t.Fatalf("second mtu chunk not placed after first padding")
	}
	if extra := countNonHintBytes(raw.Bytes()[4:6], table); extra != 2 {
		t.Fatalf("first spacer has %d padding bytes, want 2", extra)
	}
	if extra := countNonHintBytes(raw.Bytes()[10:12], table); extra != 2 {
		t.Fatalf("second spacer has %d padding bytes, want 2", extra)
	}
}

func TestDownlinkPacingWriter_RoundTripCompatibility(t *testing.T) {
	table := NewTable("pacing-roundtrip", "prefer_entropy")
	cfg := defaultDownlinkPacingConfig()
	cfg.mtuWireBytes = 4
	cfg.injectChanceNumerator = 1
	cfg.injectChanceDenominator = 1
	cfg.paddingPacketMin = 3
	cfg.paddingPacketMax = 3

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	writer := newTestSudokuPacingWriter(c1, table, cfg)
	reader := NewConn(c2, table, 0, 0, false)
	payload := bytes.Repeat([]byte("compat-padding-"), 8)

	writeErr := make(chan error, 1)
	go func() {
		_, err := writer.Write(payload)
		_ = c1.Close()
		writeErr <- err
	}()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("roundtrip mismatch with pacing padding injected")
	}
}

func TestPackedDownlinkPacingWriter_RoundTripCompatibility(t *testing.T) {
	table := NewTable("packed-roundtrip", "prefer_entropy")
	cfg := defaultDownlinkPacingConfig()
	cfg.mtuWireBytes = 4
	cfg.injectChanceNumerator = 1
	cfg.injectChanceDenominator = 1
	cfg.paddingPacketMin = 4
	cfg.paddingPacketMax = 4

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	packed, writer := newTestPackedPacingWriter(c1, table, cfg)
	reader := NewPackedConn(c2, table, 0, 0)
	payload := bytes.Repeat([]byte("packed-padding-compat"), 96)

	writeErr := make(chan error, 1)
	go func() {
		_, err := writer.Write(payload)
		if err == nil {
			err = packed.Flush()
		}
		_ = c1.Close()
		writeErr <- err
	}()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("packed roundtrip mismatch with pacing padding injected")
	}
}

func countNonHintBytes(data []byte, table *Table) int {
	count := 0
	for _, b := range data {
		if !table.layout.isHint(b) {
			count++
		}
	}
	return count
}

func newTestSudokuPacingWriter(raw io.Writer, table *Table, cfg downlinkPacingConfig) *DownlinkPacingWriter {
	rng := newSudokuRand(1)
	pacedRaw := newPacingRawWriter(raw, table.PaddingPool, rng, cfg)
	return newDownlinkPacingWriterWithConfig(newSudokuDataWriter(pacedRaw, table, rng, 0, 0))
}

func newTestPackedPacingWriter(raw net.Conn, table *Table, cfg downlinkPacingConfig) (*PackedConn, *DownlinkPacingWriter) {
	pacedRaw := newPacingRawWriter(raw, packedInterPacketPaddingPool(table), newSudokuRand(3), cfg)
	packed := NewPackedConn(&pacingNetConn{Conn: raw, writer: pacedRaw}, table, 0, 0)
	return packed, newDownlinkPacingWriterWithConfig(packed)
}
