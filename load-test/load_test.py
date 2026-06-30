#!/usr/bin/env python3
"""
Load test harness for the Ad Billing Engine.

Runs a producer at a target rate, then measures actual throughput and
latency by querying the PostgreSQL transaction log.

Usage:
    python load_test.py --rate 1000 --duration 60
    python load_test.py --rate 500 --duration 120 --duplicate-pct 10

This script saves results to load-test-results/run_<timestamp>.json
so you have evidence to show in interviews.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg2

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "load-test-results")


def run_load_test(args):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_id  = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = os.path.join(RESULTS_DIR, f"run_{run_id}.json")

    print(f"{'='*60}")
    print(f"AD BILLING ENGINE LOAD TEST")
    print(f"{'='*60}")
    print(f"Target rate:     {args.rate} events/s")
    print(f"Duration:        {args.duration}s")
    print(f"Duplicate pct:   {args.duplicate_pct}%")
    print(f"Bootstrap:       {args.bootstrap}")
    print(f"Results file:    {outfile}")
    print()

    # Connect to Postgres for pre-test baseline
    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname="billing", user="billing", password="billing_secret"
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM transaction_log")
    baseline_count = cur.fetchone()[0]
    test_start = datetime.now(timezone.utc).isoformat()
    wall_start = time.time()

    print(f"Baseline transaction count: {baseline_count}")
    print(f"Starting producer at {args.rate} events/s for {args.duration}s...")
    print()

    # Run producer as subprocess
    producer_cmd = [
        sys.executable,
        os.path.join(os.path.dirname(__file__), "producer.py"),
        "--rate",          str(args.rate),
        "--duration",      str(args.duration),
        "--duplicate-pct", str(args.duplicate_pct),
        "--bootstrap",     args.bootstrap,
    ]
    proc = subprocess.run(producer_cmd, capture_output=False)

    # Give consumers time to drain
    drain_wait = min(30, args.duration // 4)
    print(f"\nWaiting {drain_wait}s for consumers to drain...")
    time.sleep(drain_wait)

    wall_elapsed = time.time() - wall_start

    # Collect results from Postgres
    cur.execute("SELECT COUNT(*) FROM transaction_log WHERE processed_at >= %s", (test_start,))
    total_processed = cur.fetchone()[0]

    cur.execute("""
        SELECT status, COUNT(*) as cnt, SUM(cost_micros) as total_micros
        FROM transaction_log
        WHERE processed_at >= %s
        GROUP BY status
    """, (test_start,))
    by_status = {row[0]: {"count": row[1], "total_micros": row[2]} for row in cur.fetchall()}

    cur.execute("""
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cost_micros) as p50,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cost_micros) as p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY cost_micros) as p99,
            MIN(cost_micros) as min_cost,
            MAX(cost_micros) as max_cost
        FROM transaction_log
        WHERE processed_at >= %s
    """, (test_start,))
    row = cur.fetchone()
    cost_stats = {
        "p50_micros": int(row[0] or 0),
        "p95_micros": int(row[1] or 0),
        "p99_micros": int(row[2] or 0),
        "min_micros": int(row[3] or 0),
        "max_micros": int(row[4] or 0),
    }

    # Check for duplicates that leaked through (should be 0)
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT event_id, COUNT(*) as cnt
            FROM transaction_log
            WHERE processed_at >= %s
            GROUP BY event_id
            HAVING COUNT(*) > 1
        ) AS dups
    """, (test_start,))
    duplicate_leakage = cur.fetchone()[0]

    success_count  = by_status.get("SUCCESS",  {}).get("count", 0)
    rejected_count = by_status.get("REJECTED", {}).get("count", 0)
    dlq_count      = by_status.get("DLQ",      {}).get("count", 0)

    throughput = total_processed / args.duration if args.duration > 0 else 0

    results = {
        "run_id":             run_id,
        "test_start_utc":     test_start,
        "config": {
            "target_rate_per_sec": args.rate,
            "duration_sec":        args.duration,
            "duplicate_pct":       args.duplicate_pct,
        },
        "results": {
            "total_processed":    total_processed,
            "success_count":      success_count,
            "rejected_count":     rejected_count,
            "dlq_count":          dlq_count,
            "duplicate_leakage":  duplicate_leakage,
            "throughput_per_sec": round(throughput, 1),
            "wall_elapsed_sec":   round(wall_elapsed, 1),
        },
        "cost_distribution": cost_stats,
        "idempotency_check": {
            "duplicates_sent_pct":     args.duplicate_pct,
            "duplicate_leakage_count": duplicate_leakage,
            "idempotency_hold":        duplicate_leakage == 0,
        },
    }

    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"LOAD TEST RESULTS")
    print(f"{'='*60}")
    print(f"Total processed:   {total_processed}")
    print(f"  SUCCESS:         {success_count}")
    print(f"  REJECTED:        {rejected_count}")
    print(f"  DLQ:             {dlq_count}")
    print(f"Throughput:        {throughput:.1f} events/s")
    print(f"Wall time:         {wall_elapsed:.1f}s")
    print()
    print(f"Idempotency check:")
    print(f"  Duplicates sent: {args.duplicate_pct}%")
    print(f"  Leakage:         {duplicate_leakage} (should be 0)")
    print(f"  PASS:            {duplicate_leakage == 0}")
    print()
    print(f"Cost distribution (micros):")
    print(f"  p50: {cost_stats['p50_micros']}")
    print(f"  p95: {cost_stats['p95_micros']}")
    print(f"  p99: {cost_stats['p99_micros']}")
    print()
    print(f"Results saved to: {outfile}")
    print(f"{'='*60}")

    conn.close()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ad Billing Engine load test")
    parser.add_argument("--rate",          type=int,   default=500)
    parser.add_argument("--duration",      type=int,   default=60)
    parser.add_argument("--duplicate-pct", type=float, default=5.0, dest="duplicate_pct")
    parser.add_argument("--bootstrap",     type=str,   default="localhost:9092")
    parser.add_argument("--pg-host",       type=str,   default="localhost", dest="pg_host")
    parser.add_argument("--pg-port",       type=int,   default=5432,        dest="pg_port")
    args = parser.parse_args()
    run_load_test(args)
