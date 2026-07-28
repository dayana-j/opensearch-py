#!/usr/bin/env python3
"""
profile_grpc_bulk.py — Profile gRPC vs REST Bulk Performance

Uses cProfile to measure overhead in the gRPC bulk path compared to REST.
Run on an EC2 instance with access to the OpenSearch domain.

Usage:
    python profile_grpc_bulk.py
    python profile_grpc_bulk.py --docs 1000 --batches 10
"""

import argparse
import cProfile
import pstats
import time
import io
import os
import json

import boto3
from opensearchpy import OpenSearch, Urllib3AWSV4SignerAuth

# Try to import gRPC client
try:
    from opensearchpy import OpenSearchGrpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False
    print("WARNING: OpenSearchGrpc not available. Only REST will be profiled.")


# ─── Configuration ────────────────────────────────────────────────────────────

HOST = os.environ.get(
    "OPENSEARCH_HOST",
    "search-grpc-pub-eit-pen-zmxs6i2qwtltvt3gbqvb6abvda.eu-west-1.es-staging.amazonaws.com"
)
REST_PORT = int(os.environ.get("OPENSEARCH_REST_PORT", "443"))
GRPC_PORT = int(os.environ.get("OPENSEARCH_GRPC_PORT", "9400"))
REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")
SERVICE = "es"

INDEX_NAME = "grpc-profile-test"


# ─── Helper Functions ─────────────────────────────────────────────────────────

def get_auth():
    """Get SigV4 auth from environment or boto3 session."""
    credentials = boto3.Session(region_name=REGION).get_credentials()
    return Urllib3AWSV4SignerAuth(credentials, REGION, SERVICE)


def generate_bulk_body(num_docs, index=INDEX_NAME):
    """Generate a bulk body with num_docs documents."""
    body = []
    for i in range(num_docs):
        body.append({"index": {"_index": index, "_id": str(i)}})
        body.append({
            "title": f"Document {i}",
            "content": f"This is test document number {i} for profiling gRPC performance.",
            "value": i,
            "tags": ["profile", "test", f"batch-{i % 10}"],
        })
    return body


def create_rest_client():
    """Create a standard REST OpenSearch client."""
    auth = get_auth()
    return OpenSearch(
        hosts=[{"host": HOST, "port": REST_PORT}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
    )


def create_grpc_client():
    """Create an OpenSearchGrpc client."""
    if not GRPC_AVAILABLE:
        return None
    auth = get_auth()
    return OpenSearchGrpc(
        hosts=[{"host": HOST, "port": REST_PORT}],
        grpc_hosts=[{"host": HOST, "port": GRPC_PORT}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
    )


# ─── Profiling Functions ──────────────────────────────────────────────────────

def profile_bulk(client, body, label=""):
    """Profile a single bulk call."""
    profiler = cProfile.Profile()
    profiler.enable()

    start = time.perf_counter()
    response = client.bulk(body=body)
    elapsed = time.perf_counter() - start

    profiler.disable()

    errors = response.get("errors", True)
    items = len(response.get("items", []))

    return profiler, elapsed, errors, items


def run_profiling(client, num_docs, num_batches, label):
    """Run multiple bulk batches and collect profiling stats."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Documents per batch: {num_docs}")
    print(f"  Number of batches: {num_batches}")
    print(f"{'='*60}")

    body = generate_bulk_body(num_docs)
    elapsed_times = []
    combined_profiler = cProfile.Profile()

    for i in range(num_batches):
        combined_profiler.enable()
        start = time.perf_counter()
        response = client.bulk(body=body)
        elapsed = time.perf_counter() - start
        combined_profiler.disable()

        elapsed_times.append(elapsed)
        errors = response.get("errors", True)
        items = len(response.get("items", []))

        status = "✓" if not errors else "✗"
        print(f"  Batch {i+1}/{num_batches}: {status} {items} items in {elapsed*1000:.1f}ms")

    # Summary
    avg_ms = (sum(elapsed_times) / len(elapsed_times)) * 1000
    min_ms = min(elapsed_times) * 1000
    max_ms = max(elapsed_times) * 1000
    total_docs = num_docs * num_batches
    docs_per_sec = total_docs / sum(elapsed_times)

    print(f"\n  Summary:")
    print(f"    Average: {avg_ms:.1f}ms per batch")
    print(f"    Min:     {min_ms:.1f}ms")
    print(f"    Max:     {max_ms:.1f}ms")
    print(f"    Total:   {sum(elapsed_times)*1000:.1f}ms for {total_docs} docs")
    print(f"    Rate:    {docs_per_sec:.0f} docs/sec")

    # Top 20 functions by cumulative time
    print(f"\n  Top 20 functions by cumulative time:")
    stream = io.StringIO()
    stats = pstats.Stats(combined_profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(20)
    print(stream.getvalue())

    # Save detailed profile
    profile_file = f"profile_{label.lower().replace(' ', '_')}.prof"
    combined_profiler.dump_stats(profile_file)
    print(f"  Full profile saved to: {profile_file}")
    print(f"  View with: python -m pstats {profile_file}")

    return {
        "label": label,
        "avg_ms": avg_ms,
        "min_ms": min_ms,
        "max_ms": max_ms,
        "docs_per_sec": docs_per_sec,
        "total_docs": total_docs,
        "elapsed_times": elapsed_times,
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Profile gRPC vs REST bulk performance")
    parser.add_argument("--docs", type=int, default=100, help="Documents per batch (default: 100)")
    parser.add_argument("--batches", type=int, default=5, help="Number of batches (default: 5)")
    parser.add_argument("--rest-only", action="store_true", help="Only profile REST")
    parser.add_argument("--grpc-only", action="store_true", help="Only profile gRPC")
    args = parser.parse_args()

    print(f"OpenSearch gRPC vs REST Profiling")
    print(f"Host: {HOST}")
    print(f"Region: {REGION}")
    print(f"REST Port: {REST_PORT}, gRPC Port: {GRPC_PORT}")

    results = []

    # ─── REST Profiling ───────────────────────────────────────────────────
    if not args.grpc_only:
        print("\nConnecting REST client...")
        rest_client = create_rest_client()

        # Warm up
        print("Warming up REST...")
        warmup_body = generate_bulk_body(10)
        rest_client.bulk(body=warmup_body)

        rest_result = run_profiling(rest_client, args.docs, args.batches, "REST Bulk")
        results.append(rest_result)
        rest_client.close()

    # ─── gRPC Profiling ───────────────────────────────────────────────────
    if not args.rest_only and GRPC_AVAILABLE:
        print("\nConnecting gRPC client...")
        grpc_client = create_grpc_client()

        if grpc_client:
            # Warm up
            print("Warming up gRPC...")
            warmup_body = generate_bulk_body(10)
            try:
                grpc_client.bulk(body=warmup_body)
            except Exception as e:
                print(f"  gRPC warmup failed: {e}")
                print("  Continuing with profiling anyway...")

            grpc_result = run_profiling(grpc_client, args.docs, args.batches, "gRPC Bulk")
            results.append(grpc_result)
            grpc_client.close()

    # ─── Comparison ───────────────────────────────────────────────────────
    if len(results) == 2:
        rest_r, grpc_r = results[0], results[1]
        speedup = rest_r["avg_ms"] / grpc_r["avg_ms"] if grpc_r["avg_ms"] > 0 else 0
        improvement = ((rest_r["avg_ms"] - grpc_r["avg_ms"]) / rest_r["avg_ms"]) * 100

        print(f"\n{'='*60}")
        print(f"  COMPARISON")
        print(f"{'='*60}")
        print(f"  REST average:  {rest_r['avg_ms']:.1f}ms ({rest_r['docs_per_sec']:.0f} docs/sec)")
        print(f"  gRPC average:  {grpc_r['avg_ms']:.1f}ms ({grpc_r['docs_per_sec']:.0f} docs/sec)")
        print(f"  Speedup:       {speedup:.2f}x")
        print(f"  Improvement:   {improvement:.1f}%")
        print(f"{'='*60}")

    # Cleanup test index
    print("\nCleaning up...")
    try:
        cleanup_client = create_rest_client()
        cleanup_client.indices.delete(index=INDEX_NAME, ignore=[404])
        cleanup_client.close()
    except Exception as e:
        print(f"  Cleanup failed: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
