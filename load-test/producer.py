#!/usr/bin/env python3
"""
Ad event Kafka producer for load testing.

Usage:
    python producer.py --rate 1000 --duration 60
    python producer.py --rate 500 --duration 120 --duplicate-pct 5

Arguments:
    --rate          Events per second to produce (default: 500)
    --duration      Seconds to run (default: 60)
    --duplicate-pct Percentage of events to replay as duplicates (default: 0)
                    Used to test idempotency guarantees.
    --bootstrap     Kafka bootstrap servers (default: localhost:9092)
"""

import argparse
import json
import random
import time
import uuid
from collections import deque
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

ADVERTISERS = ["adv-001", "adv-002", "adv-003", "adv-004", "adv-005"]
CAMPAIGNS   = [f"camp-{i:03d}" for i in range(1, 21)]
EVENT_TYPES = ["CLICK", "IMPRESSION"]

# Cost ranges in micro-dollars
CLICK_COST_RANGE      = (50_000, 500_000)    # $0.05 – $0.50 per click
IMPRESSION_COST_RANGE = (1_000,  10_000)     # $0.001 – $0.01 per impression


def make_event() -> dict:
    event_type = random.choice(EVENT_TYPES)
    cost_range = CLICK_COST_RANGE if event_type == "CLICK" else IMPRESSION_COST_RANGE
    return {
        "eventId":      str(uuid.uuid4()),
        "advertiserId": random.choice(ADVERTISERS),
        "campaignId":   random.choice(CAMPAIGNS),
        "eventType":    event_type,
        "costMicros":   random.randint(*cost_range),
        "timestamp":    int(time.time() * 1000),
        "retryCount":   0,
    }


def produce(args):
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",                # Wait for leader + all ISR replicas to ack
        retries=3,
        linger_ms=5,               # Small batching window for throughput
        batch_size=32_768,         # 32 KB batch
        compression_type="gzip",
    )

    interval       = 1.0 / args.rate
    end_time       = time.time() + args.duration
    sent_count     = 0
    error_count    = 0
    duplicate_count = 0
    recent_ids     = deque(maxlen=200)  # pool for duplicates

    print(f"[{datetime.now().isoformat()}] Starting producer: "
          f"rate={args.rate}/s duration={args.duration}s "
          f"duplicate_pct={args.duplicate_pct}%")

    stats_interval = 5.0
    stats_deadline = time.time() + stats_interval
    stats_sent     = 0

    while time.time() < end_time:
        loop_start = time.time()

        # Decide whether to send a duplicate
        if recent_ids and random.random() < args.duplicate_pct / 100.0:
            event = make_event()
            event["eventId"] = random.choice(list(recent_ids))
            duplicate_count += 1
        else:
            event = make_event()
            recent_ids.append(event["eventId"])

        try:
            producer.send("ad-events", key=event["eventId"], value=event)
            sent_count += 1
            stats_sent += 1
        except KafkaError as e:
            error_count += 1
            print(f"[ERROR] Failed to send event: {e}")

        # Print throughput stats every 5 seconds
        now = time.time()
        if now >= stats_deadline:
            elapsed = now - (stats_deadline - stats_interval)
            print(f"[{datetime.now().isoformat()}] "
                  f"sent={sent_count} duplicates={duplicate_count} "
                  f"errors={error_count} "
                  f"throughput={stats_sent / elapsed:.0f}/s")
            stats_sent    = 0
            stats_deadline = now + stats_interval

        # Rate limiting: sleep for the remaining time in this interval
        elapsed = time.time() - loop_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

    producer.flush()
    producer.close()

    print(f"\n{'='*60}")
    print(f"PRODUCER SUMMARY")
    print(f"{'='*60}")
    print(f"Total sent:      {sent_count}")
    print(f"Duplicates sent: {duplicate_count} ({duplicate_count/max(sent_count,1)*100:.1f}%)")
    print(f"Errors:          {error_count}")
    print(f"Duration:        {args.duration}s")
    print(f"Avg throughput:  {sent_count / args.duration:.0f} events/s")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ad event Kafka producer")
    parser.add_argument("--rate",          type=int,   default=500)
    parser.add_argument("--duration",      type=int,   default=60)
    parser.add_argument("--duplicate-pct", type=float, default=0.0,
                        dest="duplicate_pct")
    parser.add_argument("--bootstrap",     type=str,   default="localhost:9092")
    args = parser.parse_args()
    produce(args)
