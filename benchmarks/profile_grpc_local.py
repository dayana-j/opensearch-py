#!/usr/bin/env python3
"""
profile_grpc_local.py — Profile gRPC vs REST on local OpenSearch instance

For use with a local Docker OpenSearch 3.x instance with basic auth
and self-signed certs. No SigV4 needed.

Usage:
    python3.11 benchmarks/profile_grpc_local.py
    python3.11 benchmarks/profile_grpc_local.py --docs 1000 --batches 10
"""

import argparse
import cProfile
import io
import pstats
import time
import warnings

warnings.filterwarnings("ignore")

from opensearchpy import OpenSearch

try:
    from opensearchpy import OpenSearchGrpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

HOST = "localhost"
REST_PORT = 9200
GRPC_PORT = 9400
AUTH = None
INDEX = "profile-test"


def generate_body(num_docs):
    body = []
    for i in range(num_docs):
        body.append({"index": {"_index": INDEX, "_id": str(i)}})
        body.append({"title": f"Document {i}", "content": f"Test content {i}", "value": i})
    return body


def profile_transport(client, body, label, num_batches):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  {len(body)//2} docs x {num_batches} batches")
    print(f"{'='*60}")

    # Warmup
    try:
        client.bulk(body=body)
    except Exception as e:
        print(f"  Warmup failed: {e}")
        return None

    # Profile
    profiler = cProfile.Profile()
    times = []

    for i in range(num_batches):
        profiler.enable()
        start = time.perf_counter()
        try:
            resp = client.bulk(body=body)
            elapsed = time.perf_counter() - start
            profiler.disable()
            errors = resp.get("errors", True)
            items = len(resp.get("items", []))
            status = "✓" if not errors else "✗"
            times.append(elapsed)
            print(f"  Batch {i+1}/{num_batches}: {status} {items} items in {elapsed*1000:.1f}ms")
        except Exception as e:
            profiler.disable()
            elapsed = time.perf_counter() - start
            print(f"  Batch {i+1}/{num_batches}: ✗ FAILED in {elapsed*1000:.1f}ms - {e}")

    if not times:
        print("  No successful batches.")
        return None

    avg_ms = sum(times) / len(times) * 1000
    min_ms = min(times) * 1000
    max_ms = max(times) * 1000
    total_docs = (len(body) // 2) * len(times)
    docs_per_sec = total_docs / sum(times)

    print(f"\n  Results:")
    print(f"    Average: {avg_ms:.1f}ms")
    print(f"    Min:     {min_ms:.1f}ms")
    print(f"    Max:     {max_ms:.1f}ms")
    print(f"    Rate:    {docs_per_sec:.0f} docs/sec")

    # Top functions
    print(f"\n  Top 15 functions by cumulative time:")
    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(15)
    print(stream.getvalue())

    return {"avg_ms": avg_ms, "docs_per_sec": docs_per_sec}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, default=100)
    parser.add_argument("--batches", type=int, default=5)
    args = parser.parse_args()

    print(f"Local gRPC vs REST Profiling")
    print(f"Host: {HOST}, REST: {REST_PORT}, gRPC: {GRPC_PORT}")
    print(f"Docs per batch: {args.docs}, Batches: {args.batches}")

    body = generate_body(args.docs)

    # REST (no security)
    rest_client = OpenSearch(
        hosts=[{"host": HOST, "port": REST_PORT}],
        use_ssl=False,
        verify_certs=False,
    )
    rest_result = profile_transport(rest_client, body, "REST Bulk", args.batches)

    # gRPC
    grpc_result = None
    if GRPC_AVAILABLE:
        grpc_client = OpenSearchGrpc(
            hosts=[{"host": HOST, "port": REST_PORT}],
            grpc_hosts=[{"host": HOST, "port": GRPC_PORT}],
            use_ssl=False,
        )
        grpc_result = profile_transport(grpc_client, body, "gRPC Bulk", args.batches)
        grpc_client.close()
    else:
        print("\n  gRPC client not available (missing opensearch-protobufs)")

    # Comparison
    if rest_result and grpc_result:
        speedup = rest_result["avg_ms"] / grpc_result["avg_ms"]
        improvement = ((rest_result["avg_ms"] - grpc_result["avg_ms"]) / rest_result["avg_ms"]) * 100
        print(f"\n{'='*60}")
        print(f"  COMPARISON")
        print(f"{'='*60}")
        print(f"  REST:  {rest_result['avg_ms']:.1f}ms  ({rest_result['docs_per_sec']:.0f} docs/sec)")
        print(f"  gRPC:  {grpc_result['avg_ms']:.1f}ms  ({grpc_result['docs_per_sec']:.0f} docs/sec)")
        print(f"  Speedup: {speedup:.2f}x ({improvement:.1f}% improvement)")
        print(f"{'='*60}")

    # Cleanup
    rest_client.indices.delete(index=INDEX, ignore=[404])
    rest_client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
