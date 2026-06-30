#!/usr/bin/env python3
"""
Seeds advertiser balances in Redis before a load test.

Usage:
    python3 seed_balances.py
    python3 seed_balances.py --redis-host localhost --redis-port 6379
"""

import argparse
import redis

BALANCES = {
    "adv-001": 10_000_000_000,   # $10,000
    "adv-002": 5_000_000_000,    # $5,000
    "adv-003": 20_000_000_000,   # $20,000
    "adv-004": 1_000_000_000,    # $1,000
    "adv-005": 50_000_000_000,   # $50,000
}

def seed(host="localhost", port=6379):
    r = redis.Redis(host=host, port=port, decode_responses=True)
    for adv_id, balance in BALANCES.items():
        r.set(f"balance:{adv_id}", balance)
        print(f"Seeded {adv_id} -> {balance} micros (${balance/1_000_000:.2f})")
    print("Done seeding balances")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()
    seed(args.redis_host, args.redis_port)
