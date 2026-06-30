#!/usr/bin/env python3
"""
Daily reconciliation job.

Compares:
  "expected spend" = SUM(cost_micros) of all SUCCESS transactions in PostgreSQL
against:
  "actual balance deduction" = initial_balance - current_balance_in_redis

If they match within a tolerance, status = OK.
If they diverge, status = MISMATCH — write to reconciliation_report table and print alert.

This proves you understand that real billing systems need end-to-end verification,
not blind trust in the happy path.  If Redis gets corrupted or the billing engine
has a bug, reconciliation catches it within 24 hours.

Usage:
    python reconcile.py
    python reconcile.py --date 2024-01-15   # reconcile a specific date
    python reconcile.py --tolerance 1       # allow 1 micro-dollar rounding
"""

import argparse
import json
import sys
from datetime import date, datetime, timezone

import psycopg2
import redis

ADVERTISERS = ["adv-001", "adv-002", "adv-003", "adv-004", "adv-005"]

# Tolerance: micro-dollar rounding errors under this threshold are ignored.
# In a real system you'd set this to 0 and investigate all discrepancies.
DEFAULT_TOLERANCE_MICROS = 0


def reconcile(pg_conn, redis_client, report_date: date, tolerance: int):
    cur = pg_conn.cursor()
    results = []
    mismatches = []

    print(f"\n{'='*65}")
    print(f"RECONCILIATION REPORT — {report_date}")
    print(f"{'='*65}")
    print(f"{'ADVERTISER':<15} {'EXPECTED SPEND':>16} {'BALANCE DEDUCTED':>18} {'DISCREPANCY':>14} STATUS")
    print(f"{'-'*65}")

    for adv_id in ADVERTISERS:
        # Expected: sum of all successful transactions in Postgres
        cur.execute("""
            SELECT COALESCE(SUM(cost_micros), 0)
            FROM transaction_log
            WHERE advertiser_id = %s AND status = 'SUCCESS'
              AND DATE(processed_at) <= %s
        """, (adv_id, report_date))
        expected_spend = cur.fetchone()[0]

        # Initial balance from advertiser_account table
        cur.execute("""
            SELECT initial_balance_micros FROM advertiser_account WHERE advertiser_id = %s
        """, (adv_id,))
        row = cur.fetchone()
        initial_balance = row[0] if row else 0

        # Actual current balance from Redis
        redis_val = redis_client.get(f"balance:{adv_id}")
        current_balance = int(redis_val) if redis_val else 0

        actual_deducted = initial_balance - current_balance
        discrepancy     = expected_spend - actual_deducted
        status          = "OK" if abs(discrepancy) <= tolerance else "MISMATCH"

        if status == "MISMATCH":
            mismatches.append({
                "advertiser_id":         adv_id,
                "expected_spend_micros": expected_spend,
                "actual_deducted_micros": actual_deducted,
                "discrepancy_micros":    discrepancy,
            })

        print(f"{adv_id:<15} {expected_spend:>16,} {actual_deducted:>18,} {discrepancy:>14,} {status}")

        results.append({
            "advertiser_id":           adv_id,
            "expected_spend_micros":   expected_spend,
            "actual_balance_micros":   current_balance,
            "initial_balance_micros":  initial_balance,
            "discrepancy_micros":      discrepancy,
            "status":                  status,
        })

        # Persist to DB
        cur.execute("""
            INSERT INTO reconciliation_report
                (report_date, advertiser_id, expected_spend_micros,
                 actual_balance_micros, initial_balance_micros, discrepancy_micros, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date, advertiser_id) DO UPDATE SET
                expected_spend_micros  = EXCLUDED.expected_spend_micros,
                actual_balance_micros  = EXCLUDED.actual_balance_micros,
                discrepancy_micros     = EXCLUDED.discrepancy_micros,
                status                 = EXCLUDED.status,
                created_at             = NOW()
        """, (report_date, adv_id, expected_spend, current_balance,
              initial_balance, discrepancy, status))

    pg_conn.commit()
    cur.close()

    print(f"{'-'*65}")
    ok_count      = sum(1 for r in results if r["status"] == "OK")
    mismatch_count = len(mismatches)

    print(f"\nSummary: {ok_count} OK, {mismatch_count} MISMATCH")

    if mismatches:
        print(f"\n{'!'*65}")
        print(f"  ALERT: {mismatch_count} DISCREPANCY(IES) DETECTED")
        print(f"{'!'*65}")
        for m in mismatches:
            print(f"  Advertiser: {m['advertiser_id']}")
            print(f"    Expected spend:   {m['expected_spend_micros']:,} micros")
            print(f"    Actual deducted:  {m['actual_deducted_micros']:,} micros")
            print(f"    Discrepancy:      {m['discrepancy_micros']:,} micros "
                  f"(${m['discrepancy_micros'] / 1_000_000:.6f})")
        print()
        print("  ACTION: Investigate transaction_log for gaps or double-counts.")
        print("          Check Redis for unexpected balance modifications.")
        sys.exit(1)   # non-zero exit so cron jobs / CI pipelines catch this
    else:
        print("\n  All balances reconciled. No discrepancies found.")

    return results


def main():
    parser = argparse.ArgumentParser(description="Daily billing reconciliation")
    parser.add_argument("--date",      type=str, default=str(date.today()),
                        help="Date to reconcile (YYYY-MM-DD)")
    parser.add_argument("--tolerance", type=int, default=DEFAULT_TOLERANCE_MICROS,
                        help="Acceptable discrepancy in micros (default: 0)")
    parser.add_argument("--pg-host",   type=str, default="localhost")
    parser.add_argument("--pg-port",   type=int, default=5432)
    parser.add_argument("--redis-host", type=str, default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()

    report_date = date.fromisoformat(args.date)

    pg_conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname="billing", user="billing", password="billing_secret"
    )
    redis_client = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)

    try:
        reconcile(pg_conn, redis_client, report_date, args.tolerance)
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
