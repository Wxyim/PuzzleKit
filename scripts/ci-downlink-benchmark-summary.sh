#!/usr/bin/env bash
set -euo pipefail

CONNS="${SUDOKU_DOWNLINK_CONCURRENT_CONNS:-200}"
BYTES_PER_CONN="${SUDOKU_DOWNLINK_CONCURRENT_BYTES:-1048576}"
RTT_SAMPLES="${SUDOKU_RTT_BENCH_SAMPLES:-7}"
RTT_ONE_WAY_MS="${SUDOKU_RTT_ONE_WAY_MS:-100}"
RTT_APP_DELAY_MS="${SUDOKU_RTT_APP_DELAY_MS:-20}"
OUT_DIR="${RUNNER_TEMP:-/tmp}"
DOWNLINK_OUT="$OUT_DIR/sudoku-downlink-benchmark.txt"
RTT_OUT="$OUT_DIR/sudoku-rtt-benchmark.txt"

SUDOKU_LOG_LEVEL="${SUDOKU_LOG_LEVEL:-error}" \
SUDOKU_DOWNLINK_CONCURRENT_CONNS="$CONNS" \
SUDOKU_DOWNLINK_CONCURRENT_BYTES="$BYTES_PER_CONN" \
go test -run '^$' \
  -bench 'BenchmarkDownlinkThroughputConcurrentMatrix/(pure|packed)/httpmask_(off|stream|ws)/mux_(off|auto|on)$' \
  -benchtime=1x \
  -benchmem \
  ./tests | tee "$DOWNLINK_OUT"

SUDOKU_LOG_LEVEL="${SUDOKU_LOG_LEVEL:-error}" \
SUDOKU_RTT_BENCH_SAMPLES="$RTT_SAMPLES" \
SUDOKU_RTT_ONE_WAY_MS="$RTT_ONE_WAY_MS" \
SUDOKU_RTT_APP_DELAY_MS="$RTT_APP_DELAY_MS" \
go test -run '^$' \
  -bench 'BenchmarkHTTPMaskRTTMatrix/httpmask_(disable|stream|ws)/mux_(off|on)$' \
  -benchtime=1x \
  -benchmem \
  ./tests | tee "$RTT_OUT"

python3 - "$DOWNLINK_OUT" "$RTT_OUT" "$CONNS" "$BYTES_PER_CONN" "$RTT_SAMPLES" "$RTT_ONE_WAY_MS" "$RTT_APP_DELAY_MS" <<'PY'
import os
import re
import sys

downlink_path, rtt_path, conns, bytes_per_conn, rtt_samples, rtt_one_way_ms, rtt_app_delay_ms = sys.argv[1:8]

def metric(fields, label):
    idx = fields.index(label)
    return fields[idx - 1]

downlink_prefix = "BenchmarkDownlinkThroughputConcurrentMatrix/"
downlink_rows = []
with open(downlink_path, "r", encoding="utf-8") as f:
    for line in f:
        if not line.startswith(downlink_prefix):
            continue
        fields = line.split()
        name = fields[0][len(downlink_prefix):]
        name = re.sub(r"-\d+$", "", name)
        mb_s = float(metric(fields, "MB/s"))
        b_op = int(metric(fields, "B/op"))
        allocs_op = int(metric(fields, "allocs/op"))
        downlink_rows.append((name, mb_s * 8, b_op, allocs_op))
if not downlink_rows:
    raise SystemExit("no downlink benchmark rows parsed")

rtt_prefix = "BenchmarkHTTPMaskRTTMatrix/"
rtt_rows = []
with open(rtt_path, "r", encoding="utf-8") as f:
    for line in f:
        if not line.startswith(rtt_prefix):
            continue
        fields = line.split()
        name = fields[0][len(rtt_prefix):]
        name = re.sub(r"-\d+$", "", name)
        setup_p50 = float(metric(fields, "setup_p50_ms"))
        setup_p95 = float(metric(fields, "setup_p95_ms"))
        total_p50 = float(metric(fields, "total_echo_p50_ms"))
        total_p95 = float(metric(fields, "total_echo_p95_ms"))
        est_p50 = float(metric(fields, "est_echo_p50_ms"))
        b_op = int(metric(fields, "B/op"))
        allocs_op = int(metric(fields, "allocs/op"))
        rtt_rows.append((name, setup_p50, setup_p95, total_p50, total_p95, est_p50, b_op, allocs_op))
if not rtt_rows:
    raise SystemExit("no rtt benchmark rows parsed")

def pct_delta(new, old):
    if old == 0:
        return "n/a"
    return f"{(new - old) * 100 / old:+.1f}%"

def ms_delta(new, old):
    return f"{new - old:+.3f}"

rtt_by_name = {
    name: {
        "setup_p50": setup_p50,
        "setup_p95": setup_p95,
        "total_p50": total_p50,
        "total_p95": total_p95,
        "est_p50": est_p50,
        "b_op": b_op,
        "allocs_op": allocs_op,
    }
    for name, setup_p50, setup_p95, total_p50, total_p95, est_p50, b_op, allocs_op in rtt_rows
}

def row(name):
    return rtt_by_name.get(name)

disable_off = row("httpmask_disable/mux_off")
stream_off = row("httpmask_stream/mux_off")
stream_on = row("httpmask_stream/mux_on")
ws_off = row("httpmask_ws/mux_off")
ws_on = row("httpmask_ws/mux_on")

stream_off_total_ok = False
if stream_off and disable_off and ws_off:
    baseline = max(disable_off["total_p50"], ws_off["total_p50"])
    stream_off_total_ok = stream_off["total_p50"] <= baseline + 15.0

mux_on_setup_ok = False
if stream_on and ws_on:
    mux_on_setup_ok = stream_on["setup_p50"] <= 5.0 and ws_on["setup_p50"] <= 5.0

mux_on_total_ok = False
if stream_on and ws_on:
    mux_on_total_ok = stream_on["total_p50"] <= ws_on["total_p50"] + 15.0

summary = [
    "## Downlink Throughput Benchmark",
    "",
    f"Concurrent connections: `{conns}`; bytes per connection: `{bytes_per_conn}`.",
    "",
    "| Config | Total Mbps | B/op | allocs/op |",
    "|---|---:|---:|---:|",
]
for name, mbps, b_op, allocs_op in downlink_rows:
    summary.append(f"| `{name}` | {mbps:.2f} | {b_op} | {allocs_op} |")

summary.append("")
summary.append("_Single CI run; use local 3-run median sampling for resource comparisons._")
summary.extend([
    "",
    "## HTTPMask CONNECT Echo RTT",
    "",
    f"Samples per config: `{rtt_samples}`; one-way delay: `{rtt_one_way_ms}ms`; app delay after CONNECT 200: `{rtt_app_delay_ms}ms`.",
    "",
    "| Config | setup p50 ms | setup p95 ms | total echo p50 ms | total echo p95 ms | established echo p50 ms | B/op | allocs/op |",
    "|---|---:|---:|---:|---:|---:|---:|---:|",
])
for name, setup_p50, setup_p95, total_p50, total_p95, est_p50, b_op, allocs_op in rtt_rows:
    summary.append(f"| `{name}` | {setup_p50:.3f} | {setup_p95:.3f} | {total_p50:.3f} | {total_p95:.3f} | {est_p50:.3f} | {b_op} | {allocs_op} |")

if stream_off and stream_on and ws_on:
    summary.extend([
        "",
        "| Check | Baseline | Current | Delta | Result |",
        "|---|---:|---:|---:|---|",
    ])
    if disable_off and ws_off:
        baseline = max(disable_off["total_p50"], ws_off["total_p50"])
        summary.append(f"| stream mux_off total echo p50 vs disable/ws class | {baseline:.3f} | {stream_off['total_p50']:.3f} | {ms_delta(stream_off['total_p50'], baseline)} | {'same RTT class' if stream_off_total_ok else 'check stream total echo'} |")
    summary.append(f"| stream mux_on setup p50 | 5.000 | {stream_on['setup_p50']:.3f} | {ms_delta(stream_on['setup_p50'], 5.0)} | {'deferred-open effective' if stream_on['setup_p50'] <= 5.0 else 'check mux setup'} |")
    summary.append(f"| stream mux_on total echo p50 vs ws mux_on | {ws_on['total_p50']:.3f} | {stream_on['total_p50']:.3f} | {ms_delta(stream_on['total_p50'], ws_on['total_p50'])} | {'same RTT class' if mux_on_total_ok else 'check mux total echo'} |")
    summary.append(f"| stream mux_on allocations vs mux_off | {stream_off['allocs_op']} | {stream_on['allocs_op']} | {pct_delta(stream_on['allocs_op'], stream_off['allocs_op'])} | allocation reuse signal |")

    summary.append("")
    if stream_off_total_ok and mux_on_setup_ok and mux_on_total_ok:
        summary.append("RTT conclusion: under the same CONNECT echo shape as `/tmp/sudoku-rtt-bench`, stream `mux_off` stayed in the ws/disable RTT class, and `mux_on` kept deferred-open effective: setup p50 stayed near zero while total echo p50 stayed in the ws `mux_on` class.")
    else:
        summary.append("RTT conclusion: at least one sampled RTT check drifted outside the expected `/tmp/sudoku-rtt-bench` class; compare setup and total echo columns before treating this as only CI noise.")
summary_text = "\n".join(summary) + "\n"

step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
if step_summary:
    with open(step_summary, "a", encoding="utf-8") as f:
        f.write(summary_text)
else:
    print(summary_text)
PY
